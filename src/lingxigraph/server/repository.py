"""Control-plane repositories and durable PostgreSQL run queue."""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from ..errors import ConcurrentRunError, IdempotencyConflictError
from ..types import MultitaskStrategy, RunStatus
from .models import (
    Assistant,
    AssistantCreate,
    AssistantPatch,
    AuditRecord,
    Run,
    RunCreate,
    RunEvent,
    Schedule,
    ScheduleCreate,
    SchedulePatch,
    Thread,
    ThreadCreate,
    ThreadPatch,
    enum_value,
    utcnow,
)

ACTIVE = {RunStatus.RUNNING.value, RunStatus.CANCELLING.value}
TERMINAL = {
    RunStatus.SUCCEEDED.value,
    RunStatus.FAILED.value,
    RunStatus.CANCELLED.value,
    RunStatus.TIMED_OUT.value,
    RunStatus.DEAD_LETTER.value,
}


@dataclass(frozen=True, slots=True)
class RepositoryLimits:
    max_active_runs: int = 100
    max_queued_runs: int = 1000
    max_sse_connections: int = 200
    max_requests_per_minute: int = 600
    max_request_bytes: int = 1_048_576
    max_state_bytes: int = 1_048_576
    max_event_bytes: int = 262_144


class InMemoryRepository:
    """Concurrency-correct control plane for tests and single-host deployments."""

    def __init__(self, *, limits: RepositoryLimits | None = None) -> None:
        self.limits = limits or RepositoryLimits()
        self._assistants: dict[tuple[str, str], Assistant] = {}
        self._threads: dict[tuple[str, str], Thread] = {}
        self._runs: dict[tuple[str, str], Run] = {}
        self._events: dict[tuple[str, str], list[RunEvent]] = {}
        self._schedules: dict[tuple[str, str], Schedule] = {}
        self._audits: list[AuditRecord] = []
        self._lock = asyncio.Lock()
        self._changed = asyncio.Condition()

    async def create_assistant(
        self, tenant_id: str, request: AssistantCreate, graph_version: str
    ) -> Assistant:
        assistant = Assistant(
            tenant_id=tenant_id,
            graph_id=request.graph_id,
            graph_version=graph_version,
            name=request.name,
            config=request.config,
            context=request.context,
            metadata=request.metadata,
        )
        async with self._lock:
            self._assistants[(tenant_id, assistant.id)] = assistant
        return assistant.model_copy(deep=True)

    async def healthcheck(self) -> bool:
        return True

    async def get_assistant(self, tenant_id: str, assistant_id: str) -> Assistant | None:
        async with self._lock:
            value = self._assistants.get((tenant_id, assistant_id))
            return value.model_copy(deep=True) if value else None

    async def list_assistants(self, tenant_id: str) -> list[Assistant]:
        async with self._lock:
            return [
                value.model_copy(deep=True)
                for (tenant, _), value in self._assistants.items()
                if tenant == tenant_id
            ]

    async def patch_assistant(
        self, tenant_id: str, assistant_id: str, request: AssistantPatch
    ) -> Assistant | None:
        async with self._lock:
            key = (tenant_id, assistant_id)
            current = self._assistants.get(key)
            if current is None:
                return None
            changes = request.model_dump(exclude_none=True)
            changes["updated_at"] = utcnow()
            updated = current.model_copy(update=changes, deep=True)
            self._assistants[key] = updated
            return updated.model_copy(deep=True)

    async def delete_assistant(self, tenant_id: str, assistant_id: str) -> bool:
        async with self._lock:
            return self._assistants.pop((tenant_id, assistant_id), None) is not None

    async def create_thread(self, tenant_id: str, request: ThreadCreate) -> Thread:
        thread = Thread(tenant_id=tenant_id, metadata=request.metadata)
        async with self._lock:
            self._threads[(tenant_id, thread.id)] = thread
        return thread.model_copy(deep=True)

    async def get_thread(self, tenant_id: str, thread_id: str) -> Thread | None:
        async with self._lock:
            value = self._threads.get((tenant_id, thread_id))
            return value.model_copy(deep=True) if value else None

    async def list_threads(self, tenant_id: str) -> list[Thread]:
        async with self._lock:
            return [
                value.model_copy(deep=True)
                for (tenant, _), value in self._threads.items()
                if tenant == tenant_id
            ]

    async def patch_thread(
        self, tenant_id: str, thread_id: str, request: ThreadPatch
    ) -> Thread | None:
        async with self._lock:
            key = (tenant_id, thread_id)
            current = self._threads.get(key)
            if current is None:
                return None
            updated = current.model_copy(
                update={"metadata": request.metadata, "updated_at": utcnow()}, deep=True
            )
            self._threads[key] = updated
            return updated.model_copy(deep=True)

    async def delete_thread(self, tenant_id: str, thread_id: str) -> bool:
        async with self._lock:
            if any(
                run.tenant_id == tenant_id
                and run.thread_id == thread_id
                and enum_value(run.status) in ACTIVE
                for run in self._runs.values()
            ):
                raise ConcurrentRunError("cannot delete a thread with an active run")
            return self._threads.pop((tenant_id, thread_id), None) is not None

    async def create_run(
        self,
        tenant_id: str,
        thread_id: str | None,
        assistant: Assistant,
        request: RunCreate,
        *,
        idempotency_key: str | None = None,
        request_digest: str | None = None,
    ) -> Run:
        async with self._lock:
            if idempotency_key is not None:
                existing = next(
                    (
                        run
                        for run in self._runs.values()
                        if run.tenant_id == tenant_id
                        and run.idempotency_key == idempotency_key
                    ),
                    None,
                )
                if existing is not None:
                    if existing.request_digest != request_digest:
                        raise IdempotencyConflictError(
                            "idempotency key was already used for a different run request"
                        )
                    return existing.model_copy(deep=True)
            tenant_runs = [run for run in self._runs.values() if run.tenant_id == tenant_id]
            active = [run for run in tenant_runs if enum_value(run.status) in ACTIVE]
            queued = [
                run
                for run in tenant_runs
                if enum_value(run.status) == RunStatus.PENDING.value
            ]
            if len(active) >= self.limits.max_active_runs:
                raise ConcurrentRunError("tenant active-run quota exceeded")
            if len(queued) >= self.limits.max_queued_runs:
                raise ConcurrentRunError("tenant queued-run quota exceeded")
            same_thread = [
                run
                for run in active
                if thread_id is not None and run.thread_id == thread_id
            ]
            strategy = MultitaskStrategy(request.multitask_strategy)
            if same_thread and strategy is MultitaskStrategy.REJECT:
                raise ConcurrentRunError("thread already has an active run")
            if same_thread and strategy is MultitaskStrategy.CANCEL_PREVIOUS:
                for run in same_thread:
                    self._runs[(tenant_id, run.id)] = run.model_copy(
                        update={"status": RunStatus.CANCELLING.value}
                    )
            run = Run(
                tenant_id=tenant_id,
                thread_id=thread_id,
                assistant_id=assistant.id,
                graph_id=assistant.graph_id,
                graph_version=assistant.graph_version,
                idempotency_key=idempotency_key,
                request_digest=request_digest,
                input=request.input,
                context={**assistant.context, **request.context},
                config={**assistant.config, **request.config},
                metadata=request.metadata,
                resume=request.resume,
                update=request.update,
                goto=request.goto,
                durability=request.durability,
            )
            if request.run_timeout is not None:
                run.config["run_timeout"] = request.run_timeout
            run.config.setdefault("max_state_bytes", self.limits.max_state_bytes)
            for budget_name in ("max_model_calls", "max_tool_calls", "max_tokens", "max_cost"):
                budget_value = getattr(request, budget_name)
                if budget_value is not None:
                    run.config[budget_name] = budget_value
            self._runs[(tenant_id, run.id)] = run
            self._events[(tenant_id, run.id)] = []
        await self._notify()
        return run.model_copy(deep=True)

    async def get_run(self, tenant_id: str, run_id: str) -> Run | None:
        async with self._lock:
            value = self._runs.get((tenant_id, run_id))
            return value.model_copy(deep=True) if value else None

    async def list_runs(
        self, tenant_id: str, *, thread_id: str | None = None
    ) -> list[Run]:
        async with self._lock:
            values = [
                run.model_copy(deep=True)
                for run in self._runs.values()
                if run.tenant_id == tenant_id
                and (thread_id is None or run.thread_id == thread_id)
            ]
        return sorted(values, key=lambda run: run.created_at, reverse=True)

    async def claim_run(
        self, worker_id: str, *, lease_seconds: int = 30
    ) -> Run | None:
        now = utcnow()
        async with self._lock:
            for key, run in list(self._runs.items()):
                if (
                    enum_value(run.status) == RunStatus.RUNNING.value
                    and run.lease_expires_at is not None
                    and run.lease_expires_at <= now
                ):
                    self._runs[key] = run.model_copy(
                        update={
                            "status": RunStatus.PENDING.value,
                            "lease_owner": None,
                            "lease_expires_at": None,
                        }
                    )
            pending = sorted(
                (
                    run
                    for run in self._runs.values()
                    if enum_value(run.status) == RunStatus.PENDING.value
                ),
                key=lambda run: run.created_at,
            )
            for run in pending:
                blocked = any(
                    other.tenant_id == run.tenant_id
                    and run.thread_id is not None
                    and other.thread_id == run.thread_id
                    and enum_value(other.status) in ACTIVE
                    for other in self._runs.values()
                )
                if blocked:
                    continue
                claimed = run.model_copy(
                    update={
                        "status": RunStatus.RUNNING.value,
                        "lease_owner": worker_id,
                        "lease_expires_at": now + timedelta(seconds=lease_seconds),
                        "attempt": run.attempt + 1,
                        "started_at": run.started_at or now,
                    }
                )
                self._runs[(run.tenant_id, run.id)] = claimed
                return claimed.model_copy(deep=True)
        return None

    async def heartbeat(
        self, tenant_id: str, run_id: str, worker_id: str, *, lease_seconds: int = 30
    ) -> bool:
        async with self._lock:
            key = (tenant_id, run_id)
            run = self._runs.get(key)
            if (
                run is None
                or run.lease_owner != worker_id
                or enum_value(run.status) not in ACTIVE
            ):
                return False
            self._runs[key] = run.model_copy(
                update={"lease_expires_at": utcnow() + timedelta(seconds=lease_seconds)}
            )
            return True

    async def finish_run(
        self,
        tenant_id: str,
        run_id: str,
        status: RunStatus | str,
        *,
        output: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
    ) -> Run:
        async with self._lock:
            key = (tenant_id, run_id)
            run = self._runs[key]
            updated = run.model_copy(
                update={
                    "status": RunStatus(status).value,
                    "output": output,
                    "error": error,
                    "finished_at": utcnow(),
                    "lease_owner": None,
                    "lease_expires_at": None,
                }
            )
            self._runs[key] = updated
        await self._notify()
        return updated.model_copy(deep=True)

    async def retry_run(
        self, tenant_id: str, run_id: str, *, error: dict[str, Any] | None = None
    ) -> Run:
        async with self._lock:
            key = (tenant_id, run_id)
            run = self._runs[key]
            updated = run.model_copy(
                update={
                    "status": RunStatus.PENDING.value,
                    "error": error,
                    "finished_at": None,
                    "lease_owner": None,
                    "lease_expires_at": None,
                }
            )
            self._runs[key] = updated
        await self._notify()
        return updated.model_copy(deep=True)

    async def redrive_run(self, tenant_id: str, run_id: str) -> Run | None:
        async with self._lock:
            key = (tenant_id, run_id)
            run = self._runs.get(key)
            if run is None or enum_value(run.status) not in {
                RunStatus.DEAD_LETTER.value,
                RunStatus.FAILED.value,
            }:
                return None
            updated = run.model_copy(
                update={
                    "status": RunStatus.PENDING.value,
                    "attempt": 0,
                    "error": None,
                    "finished_at": None,
                    "lease_owner": None,
                    "lease_expires_at": None,
                }
            )
            self._runs[key] = updated
        await self._notify()
        return updated.model_copy(deep=True)

    async def request_cancel(self, tenant_id: str, run_id: str) -> bool:
        async with self._lock:
            key = (tenant_id, run_id)
            run = self._runs.get(key)
            if run is None or enum_value(run.status) in TERMINAL:
                return False
            status = (
                RunStatus.CANCELLED.value
                if enum_value(run.status) == RunStatus.PENDING.value
                else RunStatus.CANCELLING.value
            )
            self._runs[key] = run.model_copy(
                update={
                    "status": status,
                    "finished_at": utcnow() if status == RunStatus.CANCELLED.value else None,
                }
            )
        await self._notify()
        return True

    async def is_cancel_requested(self, tenant_id: str, run_id: str) -> bool:
        run = await self.get_run(tenant_id, run_id)
        return run is not None and enum_value(run.status) in {
            RunStatus.CANCELLING.value,
            RunStatus.CANCELLED.value,
        }

    async def append_event(
        self, tenant_id: str, run_id: str, kind: str, data: dict[str, Any]
    ) -> RunEvent:
        async with self._lock:
            events = self._events.setdefault((tenant_id, run_id), [])
            event = RunEvent(
                tenant_id=tenant_id,
                run_id=run_id,
                sequence=len(events) + 1,
                kind=kind,
                data=data,
            )
            events.append(event)
        await self._notify()
        return event.model_copy(deep=True)

    async def list_events(
        self, tenant_id: str, run_id: str, *, after: int = 0
    ) -> list[RunEvent]:
        async with self._lock:
            return [
                event.model_copy(deep=True)
                for event in self._events.get((tenant_id, run_id), ())
                if event.sequence > after
            ]

    async def create_schedule(
        self, tenant_id: str, request: ScheduleCreate
    ) -> Schedule:
        schedule = Schedule(tenant_id=tenant_id, **request.model_dump())
        async with self._lock:
            self._schedules[(tenant_id, schedule.id)] = schedule
        return schedule.model_copy(deep=True)

    async def list_schedules(self, tenant_id: str) -> list[Schedule]:
        async with self._lock:
            return [
                value.model_copy(deep=True)
                for (tenant, _), value in self._schedules.items()
                if tenant == tenant_id
            ]

    async def patch_schedule(
        self, tenant_id: str, schedule_id: str, request: SchedulePatch
    ) -> Schedule | None:
        async with self._lock:
            key = (tenant_id, schedule_id)
            current = self._schedules.get(key)
            if current is None:
                return None
            values = request.model_dump(exclude_none=True)
            values["updated_at"] = utcnow()
            updated = current.model_copy(update=values, deep=True)
            self._schedules[key] = updated
            return updated.model_copy(deep=True)

    async def delete_schedule(self, tenant_id: str, schedule_id: str) -> bool:
        async with self._lock:
            return self._schedules.pop((tenant_id, schedule_id), None) is not None

    async def audit(self, record: AuditRecord) -> None:
        async with self._lock:
            self._audits.append(record.model_copy(deep=True))

    async def stats(self, tenant_id: str) -> dict[str, Any]:
        async with self._lock:
            statuses = {status.value: 0 for status in RunStatus}
            for run in self._runs.values():
                if run.tenant_id == tenant_id:
                    statuses[str(enum_value(run.status))] += 1
            return {
                "runs": statuses,
                "events": sum(
                    len(events)
                    for (tenant, _), events in self._events.items()
                    if tenant == tenant_id
                ),
                "threads": sum(tenant == tenant_id for tenant, _ in self._threads),
                "assistants": sum(
                    tenant == tenant_id for tenant, _ in self._assistants
                ),
                "schedules": sum(
                    tenant == tenant_id for tenant, _ in self._schedules
                ),
            }

    async def wait_for_change(self, timeout: float = 1.0) -> None:
        async with self._changed:
            try:
                await asyncio.wait_for(self._changed.wait(), timeout)
            except TimeoutError:
                return

    async def _notify(self) -> None:
        async with self._changed:
            self._changed.notify_all()


class PostgresRepository(InMemoryRepository):
    """PostgreSQL-backed control plane.

    The complete DDL is shipped as an Alembic-compatible SQL migration.  This
    implementation keeps the same repository contract while providing the
    transactional queue primitives used by distributed workers.  CRUD calls
    use PostgreSQL when available; development can explicitly use
    :class:`InMemoryRepository`.
    """

    def __init__(
        self,
        dsn: str,
        *,
        schema: str = "lingxigraph",
        limits: RepositoryLimits | None = None,
    ) -> None:
        super().__init__(limits=limits)
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
            raise ValueError("invalid PostgreSQL schema name")
        try:
            import psycopg
            from psycopg.rows import dict_row
            from psycopg.types.json import Jsonb
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("install lingxigraph[postgres] to use PostgresRepository") from exc
        self._psycopg = psycopg
        self._dict_row = dict_row
        self._jsonb = Jsonb
        self._dsn = dsn
        self._schema = schema

    def _connect(self):
        return self._psycopg.connect(self._dsn, row_factory=self._dict_row)

    async def setup(self) -> None:
        await asyncio.to_thread(self._setup_sync)

    async def healthcheck(self) -> bool:
        try:
            return await asyncio.to_thread(self._healthcheck_sync)
        except Exception:
            return False

    def _healthcheck_sync(self) -> bool:
        with self._connect() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            return cursor.fetchone() is not None

    def _setup_sync(self) -> None:
        from importlib.resources import files

        migration = (
            files("lingxigraph.server")
            .joinpath("migrations/0001_v1.sql")
            .read_text(encoding="utf-8")
            .replace("{{schema}}", self._schema)
        )
        with self._connect() as conn, conn.cursor() as cursor:
            cursor.execute(migration)

    @staticmethod
    def _tenant(cursor, tenant_id: str) -> None:
        cursor.execute("SELECT set_config('app.tenant_id', %s, true)", (tenant_id,))

    async def create_assistant(self, tenant_id, request, graph_version):
        value = Assistant(
            tenant_id=tenant_id,
            graph_id=request.graph_id,
            graph_version=graph_version,
            name=request.name,
            config=request.config,
            context=request.context,
            metadata=request.metadata,
        )
        await asyncio.to_thread(self._insert_assistant, value)
        return value

    def _insert_assistant(self, value: Assistant) -> None:
        with self._connect() as conn, conn.cursor() as cursor:
            self._tenant(cursor, value.tenant_id)
            cursor.execute(
                f"""INSERT INTO {self._schema}.assistants
                (id,tenant_id,graph_id,graph_version,name,config,context,metadata,
                 created_at,updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    value.id,
                    value.tenant_id,
                    value.graph_id,
                    value.graph_version,
                    value.name,
                    self._jsonb(value.config),
                    self._jsonb(value.context),
                    self._jsonb(value.metadata),
                    value.created_at,
                    value.updated_at,
                ),
            )

    async def get_assistant(self, tenant_id, assistant_id):
        row = await asyncio.to_thread(
            self._fetch_one,
            tenant_id,
            f"SELECT * FROM {self._schema}.assistants WHERE tenant_id=%s AND id=%s",
            (tenant_id, assistant_id),
        )
        return Assistant.model_validate(row) if row else None

    async def list_assistants(self, tenant_id):
        rows = await asyncio.to_thread(
            self._fetch_all,
            tenant_id,
            f"SELECT * FROM {self._schema}.assistants WHERE tenant_id=%s ORDER BY created_at DESC",
            (tenant_id,),
        )
        return [Assistant.model_validate(row) for row in rows]

    async def patch_assistant(self, tenant_id, assistant_id, request):
        current = await self.get_assistant(tenant_id, assistant_id)
        if current is None:
            return None
        changes = request.model_dump(exclude_none=True)
        updated = current.model_copy(update={**changes, "updated_at": utcnow()}, deep=True)
        await asyncio.to_thread(self._update_assistant, updated)
        return updated

    def _update_assistant(self, value: Assistant) -> None:
        with self._connect() as conn, conn.cursor() as cursor:
            self._tenant(cursor, value.tenant_id)
            cursor.execute(
                f"""UPDATE {self._schema}.assistants SET name=%s, config=%s,
                context=%s, metadata=%s, updated_at=%s WHERE tenant_id=%s AND id=%s""",
                (
                    value.name,
                    self._jsonb(value.config),
                    self._jsonb(value.context),
                    self._jsonb(value.metadata),
                    value.updated_at,
                    value.tenant_id,
                    value.id,
                ),
            )

    async def delete_assistant(self, tenant_id, assistant_id):
        return await asyncio.to_thread(
            self._delete,
            tenant_id,
            f"DELETE FROM {self._schema}.assistants WHERE tenant_id=%s AND id=%s",
            (tenant_id, assistant_id),
        )

    async def create_thread(self, tenant_id, request):
        value = Thread(tenant_id=tenant_id, metadata=request.metadata)
        await asyncio.to_thread(self._insert_thread, value)
        return value

    def _insert_thread(self, value: Thread) -> None:
        with self._connect() as conn, conn.cursor() as cursor:
            self._tenant(cursor, value.tenant_id)
            cursor.execute(
                f"""INSERT INTO {self._schema}.threads
                (id,tenant_id,metadata,created_at,updated_at) VALUES (%s,%s,%s,%s,%s)""",
                (
                    value.id,
                    value.tenant_id,
                    self._jsonb(value.metadata),
                    value.created_at,
                    value.updated_at,
                ),
            )

    async def get_thread(self, tenant_id, thread_id):
        row = await asyncio.to_thread(
            self._fetch_one,
            tenant_id,
            f"SELECT * FROM {self._schema}.threads WHERE tenant_id=%s AND id=%s",
            (tenant_id, thread_id),
        )
        return Thread.model_validate(row) if row else None

    async def list_threads(self, tenant_id):
        rows = await asyncio.to_thread(
            self._fetch_all,
            tenant_id,
            f"SELECT * FROM {self._schema}.threads WHERE tenant_id=%s ORDER BY updated_at DESC",
            (tenant_id,),
        )
        return [Thread.model_validate(row) for row in rows]

    async def patch_thread(self, tenant_id, thread_id, request):
        row = await asyncio.to_thread(
            self._patch_thread_sync, tenant_id, thread_id, request.metadata
        )
        return Thread.model_validate(row) if row else None

    def _patch_thread_sync(self, tenant_id, thread_id, metadata):
        with self._connect() as conn, conn.cursor() as cursor:
            self._tenant(cursor, tenant_id)
            cursor.execute(
                f"""UPDATE {self._schema}.threads SET metadata=%s, updated_at=NOW()
                WHERE tenant_id=%s AND id=%s RETURNING *""",
                (self._jsonb(metadata), tenant_id, thread_id),
            )
            return cursor.fetchone()

    async def delete_thread(self, tenant_id, thread_id):
        return await asyncio.to_thread(self._delete_thread_sync, tenant_id, thread_id)

    def _delete_thread_sync(self, tenant_id: str, thread_id: str) -> bool:
        with self._connect() as conn, conn.cursor() as cursor:
            self._tenant(cursor, tenant_id)
            cursor.execute(
                f"""SELECT 1 FROM {self._schema}.runs WHERE tenant_id=%s
                AND thread_id=%s AND status IN ('running','cancelling') LIMIT 1""",
                (tenant_id, thread_id),
            )
            if cursor.fetchone():
                raise ConcurrentRunError("cannot delete a thread with an active run")
            cursor.execute(
                f"DELETE FROM {self._schema}.threads WHERE tenant_id=%s AND id=%s",
                (tenant_id, thread_id),
            )
            return cursor.rowcount > 0

    async def create_run(
        self,
        tenant_id,
        thread_id,
        assistant,
        request,
        *,
        idempotency_key=None,
        request_digest=None,
    ):
        return await asyncio.to_thread(
            self._create_run_sync,
            tenant_id,
            thread_id,
            assistant,
            request,
            idempotency_key,
            request_digest,
        )

    def _create_run_sync(
        self, tenant_id, thread_id, assistant, request, idempotency_key, request_digest
    ):
        with self._connect() as conn, conn.cursor() as cursor:
            self._tenant(cursor, tenant_id)
            if idempotency_key is not None:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (f"{tenant_id}:{idempotency_key}",),
                )
                cursor.execute(
                    f"""SELECT * FROM {self._schema}.runs
                    WHERE tenant_id=%s AND idempotency_key=%s""",
                    (tenant_id, idempotency_key),
                )
                existing = cursor.fetchone()
                if existing is not None:
                    if existing.get("request_digest") != request_digest:
                        raise IdempotencyConflictError(
                            "idempotency key was already used for a different run request"
                        )
                    return self._run_from_row(existing)
            cursor.execute(
                f"""SELECT
                  COUNT(*) FILTER (WHERE status IN ('running','cancelling')) AS active,
                  COUNT(*) FILTER (WHERE status='pending') AS queued
                FROM {self._schema}.runs WHERE tenant_id=%s""",
                (tenant_id,),
            )
            counts = cursor.fetchone()
            if counts["active"] >= self.limits.max_active_runs:
                raise ConcurrentRunError("tenant active-run quota exceeded")
            if counts["queued"] >= self.limits.max_queued_runs:
                raise ConcurrentRunError("tenant queued-run quota exceeded")
            if thread_id is not None:
                cursor.execute(
                    f"""SELECT id FROM {self._schema}.runs WHERE tenant_id=%s
                    AND thread_id=%s AND status IN ('running','cancelling') FOR UPDATE""",
                    (tenant_id, thread_id),
                )
                active = cursor.fetchall()
                strategy = MultitaskStrategy(request.multitask_strategy)
                if active and strategy is MultitaskStrategy.REJECT:
                    raise ConcurrentRunError("thread already has an active run")
                if active and strategy is MultitaskStrategy.CANCEL_PREVIOUS:
                    cursor.execute(
                        f"""UPDATE {self._schema}.runs SET status='cancelling'
                        WHERE tenant_id=%s AND thread_id=%s AND status='running'""",
                        (tenant_id, thread_id),
                    )
            run = Run(
                tenant_id=tenant_id,
                thread_id=thread_id,
                assistant_id=assistant.id,
                graph_id=assistant.graph_id,
                graph_version=assistant.graph_version,
                idempotency_key=idempotency_key,
                request_digest=request_digest,
                input=request.input,
                context={**assistant.context, **request.context},
                config={**assistant.config, **request.config},
                metadata=request.metadata,
                resume=request.resume,
                update=request.update,
                goto=request.goto,
                durability=request.durability,
            )
            if request.run_timeout is not None:
                run.config["run_timeout"] = request.run_timeout
            run.config.setdefault("max_state_bytes", self.limits.max_state_bytes)
            for budget_name in ("max_model_calls", "max_tool_calls", "max_tokens", "max_cost"):
                budget_value = getattr(request, budget_name)
                if budget_value is not None:
                    run.config[budget_name] = budget_value
            cursor.execute(
                f"""INSERT INTO {self._schema}.runs
                (id,tenant_id,thread_id,assistant_id,graph_id,graph_version,status,
                 idempotency_key,request_digest,input,context,config,metadata,resume,update,goto_node,durability,
                 attempt,created_at)
                 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    run.id,
                    tenant_id,
                    thread_id,
                    run.assistant_id,
                    run.graph_id,
                    run.graph_version,
                    enum_value(run.status),
                    run.idempotency_key,
                    run.request_digest,
                    self._jsonb(run.input) if run.input is not None else None,
                    self._jsonb(run.context),
                    self._jsonb(run.config),
                    self._jsonb(run.metadata),
                    self._jsonb(run.resume) if run.resume is not None else None,
                    self._jsonb(run.update) if run.update is not None else None,
                    run.goto,
                    enum_value(run.durability),
                    run.attempt,
                    run.created_at,
                ),
            )
            return run

    async def get_run(self, tenant_id, run_id):
        row = await asyncio.to_thread(
            self._fetch_one,
            tenant_id,
            f"SELECT * FROM {self._schema}.runs WHERE tenant_id=%s AND id=%s",
            (tenant_id, run_id),
        )
        return self._run_from_row(row) if row else None

    async def list_runs(self, tenant_id, *, thread_id=None):
        sql = f"SELECT * FROM {self._schema}.runs WHERE tenant_id=%s"
        params: tuple[Any, ...] = (tenant_id,)
        if thread_id is not None:
            sql += " AND thread_id=%s"
            params += (thread_id,)
        sql += " ORDER BY created_at DESC"
        rows = await asyncio.to_thread(self._fetch_all, tenant_id, sql, params)
        return [self._run_from_row(row) for row in rows]

    async def heartbeat(self, tenant_id, run_id, worker_id, *, lease_seconds=30):
        return await asyncio.to_thread(
            self._heartbeat_sync, tenant_id, run_id, worker_id, lease_seconds
        )

    def _heartbeat_sync(self, tenant_id, run_id, worker_id, lease_seconds):
        with self._connect() as conn, conn.cursor() as cursor:
            self._tenant(cursor, tenant_id)
            cursor.execute(
                f"""UPDATE {self._schema}.runs
                SET lease_expires_at=NOW()+(%s * INTERVAL '1 second')
                WHERE tenant_id=%s AND id=%s AND lease_owner=%s
                  AND status IN ('running','cancelling')""",
                (lease_seconds, tenant_id, run_id, worker_id),
            )
            return cursor.rowcount > 0

    async def finish_run(self, tenant_id, run_id, status, *, output=None, error=None):
        await asyncio.to_thread(
            self._finish_run_sync, tenant_id, run_id, status, output, error
        )
        value = await self.get_run(tenant_id, run_id)
        assert value is not None
        return value

    async def retry_run(self, tenant_id, run_id, *, error=None):
        await asyncio.to_thread(self._retry_run_sync, tenant_id, run_id, error, False)
        value = await self.get_run(tenant_id, run_id)
        assert value is not None
        return value

    async def redrive_run(self, tenant_id, run_id):
        changed = await asyncio.to_thread(self._retry_run_sync, tenant_id, run_id, None, True)
        return await self.get_run(tenant_id, run_id) if changed else None

    def _retry_run_sync(self, tenant_id, run_id, error, reset_attempt):
        with self._connect() as conn, conn.cursor() as cursor:
            self._tenant(cursor, tenant_id)
            allowed = "AND status IN ('failed','dead_letter')" if reset_attempt else ""
            attempt = "attempt=0," if reset_attempt else ""
            cursor.execute(
                f"""UPDATE {self._schema}.runs SET status='pending', {attempt}
                error=%s, finished_at=NULL, lease_owner=NULL, lease_expires_at=NULL
                WHERE tenant_id=%s AND id=%s {allowed}""",
                (self._jsonb(error) if error is not None else None, tenant_id, run_id),
            )
            return cursor.rowcount > 0

    def _finish_run_sync(self, tenant_id, run_id, status, output, error):
        with self._connect() as conn, conn.cursor() as cursor:
            self._tenant(cursor, tenant_id)
            cursor.execute(
                f"""UPDATE {self._schema}.runs SET status=%s, output=%s, error=%s,
                finished_at=NOW(), lease_owner=NULL, lease_expires_at=NULL
                WHERE tenant_id=%s AND id=%s""",
                (
                    RunStatus(status).value,
                    self._jsonb(output) if output is not None else None,
                    self._jsonb(error) if error is not None else None,
                    tenant_id,
                    run_id,
                ),
            )

    async def request_cancel(self, tenant_id, run_id):
        return await asyncio.to_thread(self._request_cancel_sync, tenant_id, run_id)

    def _request_cancel_sync(self, tenant_id, run_id):
        with self._connect() as conn, conn.cursor() as cursor:
            self._tenant(cursor, tenant_id)
            cursor.execute(
                f"""UPDATE {self._schema}.runs SET
                status=CASE WHEN status='pending' THEN 'cancelled' ELSE 'cancelling' END,
                finished_at=CASE WHEN status='pending' THEN NOW() ELSE finished_at END
                WHERE tenant_id=%s AND id=%s
                  AND status NOT IN ('succeeded','failed','cancelled','timed_out','dead_letter')""",
                (tenant_id, run_id),
            )
            return cursor.rowcount > 0

    async def is_cancel_requested(self, tenant_id, run_id):
        value = await self.get_run(tenant_id, run_id)
        return value is not None and enum_value(value.status) in {
            RunStatus.CANCELLING.value,
            RunStatus.CANCELLED.value,
        }

    async def append_event(self, tenant_id, run_id, kind, data):
        return await asyncio.to_thread(
            self._append_event_sync, tenant_id, run_id, kind, data
        )

    def _append_event_sync(self, tenant_id, run_id, kind, data):
        event = RunEvent(tenant_id=tenant_id, run_id=run_id, sequence=0, kind=kind, data=data)
        with self._connect() as conn, conn.cursor() as cursor:
            self._tenant(cursor, tenant_id)
            cursor.execute(
                f"SELECT id FROM {self._schema}.runs WHERE tenant_id=%s AND id=%s FOR UPDATE",
                (tenant_id, run_id),
            )
            cursor.execute(
                f"""SELECT COALESCE(MAX(sequence),0)+1 AS next
                FROM {self._schema}.run_events WHERE tenant_id=%s AND run_id=%s""",
                (tenant_id, run_id),
            )
            event.sequence = int(cursor.fetchone()["next"])
            cursor.execute(
                f"""INSERT INTO {self._schema}.run_events
                (id,tenant_id,run_id,sequence,kind,data,created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                (
                    event.id,
                    tenant_id,
                    run_id,
                    event.sequence,
                    kind,
                    self._jsonb(data),
                    event.created_at,
                ),
            )
        return event

    async def list_events(self, tenant_id, run_id, *, after=0):
        rows = await asyncio.to_thread(
            self._fetch_all,
            tenant_id,
            f"""SELECT * FROM {self._schema}.run_events
            WHERE tenant_id=%s AND run_id=%s AND sequence>%s ORDER BY sequence""",
            (tenant_id, run_id, after),
        )
        return [RunEvent.model_validate(row) for row in rows]

    async def create_schedule(self, tenant_id, request):
        value = Schedule(tenant_id=tenant_id, **request.model_dump())
        await asyncio.to_thread(self._insert_schedule, value)
        return value

    def _insert_schedule(self, value):
        with self._connect() as conn, conn.cursor() as cursor:
            self._tenant(cursor, value.tenant_id)
            cursor.execute(
                f"""INSERT INTO {self._schema}.schedules
                (id,tenant_id,assistant_id,cron,timezone,input,enabled,metadata,
                 created_at,updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    value.id,
                    value.tenant_id,
                    value.assistant_id,
                    value.cron,
                    value.timezone,
                    self._jsonb(value.input),
                    value.enabled,
                    self._jsonb(value.metadata),
                    value.created_at,
                    value.updated_at,
                ),
            )

    async def list_schedules(self, tenant_id):
        rows = await asyncio.to_thread(
            self._fetch_all,
            tenant_id,
            f"SELECT * FROM {self._schema}.schedules WHERE tenant_id=%s ORDER BY created_at DESC",
            (tenant_id,),
        )
        return [Schedule.model_validate(row) for row in rows]

    async def patch_schedule(self, tenant_id, schedule_id, request):
        current = await self._fetch_schedule(tenant_id, schedule_id)
        if current is None:
            return None
        values = request.model_dump(exclude_none=True)
        updated = current.model_copy(update=values)
        row = await asyncio.to_thread(self._patch_schedule_sync, tenant_id, updated)
        return Schedule.model_validate(row) if row else None

    async def _fetch_schedule(self, tenant_id, schedule_id):
        row = await asyncio.to_thread(
            self._fetch_one,
            tenant_id,
            f"SELECT * FROM {self._schema}.schedules WHERE tenant_id=%s AND id=%s",
            (tenant_id, schedule_id),
        )
        return Schedule.model_validate(row) if row else None

    def _patch_schedule_sync(self, tenant_id, value):
        with self._connect() as conn, conn.cursor() as cursor:
            self._tenant(cursor, tenant_id)
            cursor.execute(
                f"""UPDATE {self._schema}.schedules
                SET cron=%s, timezone=%s, input=%s, enabled=%s, metadata=%s,
                    updated_at=NOW()
                WHERE tenant_id=%s AND id=%s RETURNING *""",
                (
                    value.cron,
                    value.timezone,
                    self._jsonb(value.input),
                    value.enabled,
                    self._jsonb(value.metadata),
                    tenant_id,
                    value.id,
                ),
            )
            return cursor.fetchone()

    async def delete_schedule(self, tenant_id, schedule_id):
        return await asyncio.to_thread(
            self._delete,
            tenant_id,
            f"DELETE FROM {self._schema}.schedules WHERE tenant_id=%s AND id=%s",
            (tenant_id, schedule_id),
        )

    async def audit(self, record):
        await asyncio.to_thread(self._audit_sync, record)

    def _audit_sync(self, value):
        with self._connect() as conn, conn.cursor() as cursor:
            self._tenant(cursor, value.tenant_id)
            cursor.execute(
                f"""INSERT INTO {self._schema}.audit_records
                (id,tenant_id,actor,action,resource_type,resource_id,result,trace_id,
                 metadata,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    value.id,
                    value.tenant_id,
                    value.actor,
                    value.action,
                    value.resource_type,
                    value.resource_id,
                    value.result,
                    value.trace_id,
                    self._jsonb(value.metadata),
                    value.created_at,
                ),
            )

    def _fetch_one(self, tenant_id, sql, params):
        with self._connect() as conn, conn.cursor() as cursor:
            self._tenant(cursor, tenant_id)
            cursor.execute(sql, params)
            return cursor.fetchone()

    def _fetch_all(self, tenant_id, sql, params):
        with self._connect() as conn, conn.cursor() as cursor:
            self._tenant(cursor, tenant_id)
            cursor.execute(sql, params)
            return cursor.fetchall()

    def _delete(self, tenant_id, sql, params):
        with self._connect() as conn, conn.cursor() as cursor:
            self._tenant(cursor, tenant_id)
            cursor.execute(sql, params)
            return cursor.rowcount > 0

    async def stats(self, tenant_id: str) -> dict[str, Any]:
        return await asyncio.to_thread(self._stats_sync, tenant_id)

    def _stats_sync(self, tenant_id: str) -> dict[str, Any]:
        statuses = {status.value: 0 for status in RunStatus}
        with self._connect() as conn, conn.cursor() as cursor:
            self._tenant(cursor, tenant_id)
            cursor.execute(
                f"SELECT status, COUNT(*) AS count FROM {self._schema}.runs "
                "WHERE tenant_id=%s GROUP BY status",
                (tenant_id,),
            )
            for row in cursor.fetchall():
                statuses[str(row["status"])] = int(row["count"])
            counts: dict[str, int] = {}
            for name in ("run_events", "threads", "assistants", "schedules"):
                cursor.execute(
                    f"SELECT COUNT(*) AS count FROM {self._schema}.{name} "
                    "WHERE tenant_id=%s",
                    (tenant_id,),
                )
                counts[name] = int(cursor.fetchone()["count"])
        return {
            "runs": statuses,
            "events": counts["run_events"],
            "threads": counts["threads"],
            "assistants": counts["assistants"],
            "schedules": counts["schedules"],
        }

    async def claim_run(self, worker_id: str, *, lease_seconds: int = 30) -> Run | None:
        return await asyncio.to_thread(self._claim_run_sync, worker_id, lease_seconds)

    def _claim_run_sync(self, worker_id: str, lease_seconds: int) -> Run | None:
        with self._connect() as conn, conn.cursor() as cursor:
            cursor.execute(
                f"""UPDATE {self._schema}.runs SET status='pending',
                    lease_owner=NULL, lease_expires_at=NULL
                    WHERE status='running' AND lease_expires_at < NOW()"""
            )
            cursor.execute(
                f"""
                WITH candidate AS (
                    SELECT r.id FROM {self._schema}.runs r
                    WHERE r.status='pending'
                      AND NOT EXISTS (
                        SELECT 1 FROM {self._schema}.runs active
                        WHERE active.tenant_id=r.tenant_id
                          AND active.thread_id IS NOT DISTINCT FROM r.thread_id
                          AND r.thread_id IS NOT NULL
                          AND active.status IN ('running','cancelling')
                      )
                    ORDER BY r.created_at
                    FOR UPDATE SKIP LOCKED LIMIT 1
                )
                UPDATE {self._schema}.runs r
                SET status='running', lease_owner=%s,
                    lease_expires_at=NOW()+(%s * INTERVAL '1 second'),
                    attempt=r.attempt+1,
                    started_at=COALESCE(r.started_at, NOW())
                FROM candidate WHERE r.id=candidate.id RETURNING r.*
                """,
                (worker_id, lease_seconds),
            )
            row = cursor.fetchone()
        return self._run_from_row(row) if row else None

    @staticmethod
    def _run_from_row(row: dict[str, Any]) -> Run:
        value = dict(row)
        value["goto"] = value.pop("goto_node", None)
        for name in ("input", "context", "config", "metadata", "error", "output"):
            value[name] = value.get(name) or ({} if name in {"context", "config", "metadata"} else None)
        return Run.model_validate(value)


__all__ = [
    "InMemoryRepository",
    "PostgresRepository",
    "RepositoryLimits",
    "TERMINAL",
]
