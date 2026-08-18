"""Control-plane repositories and durable PostgreSQL run queue."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from ..errors import (
    ConcurrentRunError,
    IdempotencyConflictError,
    RunFinalizingError,
    RunResumeConflictError,
    RunSupersededError,
    RunTerminalError,
)
from ..steering import (
    MAX_STEERING_PAYLOAD_BYTES,
    SteeringConsumption,
    validate_steering_payload,
)
from ..types import MultitaskStrategy, RunStatus
from .models import (
    Assistant,
    AssistantCreate,
    AssistantPatch,
    AuditRecord,
    Run,
    RunCreate,
    RunEvent,
    RunSteeringEvent,
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
        self._steering: dict[tuple[str, str], list[RunSteeringEvent]] = {}
        self._steering_keys: dict[tuple[str, str, str], str] = {}
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
            run = self._create_run_locked(
                tenant_id,
                thread_id,
                assistant,
                request,
                idempotency_key=idempotency_key,
                request_digest=request_digest,
            )
        await self._notify()
        return run.model_copy(deep=True)

    def _create_run_locked(
        self,
        tenant_id: str,
        thread_id: str | None,
        assistant: Assistant,
        request: RunCreate,
        *,
        idempotency_key: str | None = None,
        request_digest: str | None = None,
    ) -> Run:
        """The body of :meth:`create_run` -- assumes ``self._lock`` is held.

        Factored out so :meth:`resume_run_with_pending_steering` can create
        the resumed Run and migrate its paused-run steering under a single
        lock acquisition (see issue #16 PR #17 review point 1): otherwise a
        worker's own lock acquisition (``claim_run``) could interleave
        between "new Run committed" and "steering migrated", claiming (and
        even finishing) the new Run before the migration ever happens.
        """

        if idempotency_key is not None:
            existing = next(
                (
                    run
                    for run in self._runs.values()
                    if run.tenant_id == tenant_id and run.idempotency_key == idempotency_key
                ),
                None,
            )
            if existing is not None:
                if existing.request_digest != request_digest:
                    raise IdempotencyConflictError(
                        "idempotency key was already used for a different run request"
                    )
                return existing
        tenant_runs = [run for run in self._runs.values() if run.tenant_id == tenant_id]
        active = [run for run in tenant_runs if enum_value(run.status) in ACTIVE]
        queued = [
            run for run in tenant_runs if enum_value(run.status) == RunStatus.PENDING.value
        ]
        if len(active) >= self.limits.max_active_runs:
            raise ConcurrentRunError("tenant active-run quota exceeded")
        if len(queued) >= self.limits.max_queued_runs:
            raise ConcurrentRunError("tenant queued-run quota exceeded")
        same_thread = [
            run for run in active if thread_id is not None and run.thread_id == thread_id
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
        return run

    async def get_run(self, tenant_id: str, run_id: str) -> Run | None:
        async with self._lock:
            value = self._runs.get((tenant_id, run_id))
            return value.model_copy(deep=True) if value else None

    async def list_runs(self, tenant_id: str, *, thread_id: str | None = None) -> list[Run]:
        async with self._lock:
            values = [
                run.model_copy(deep=True)
                for run in self._runs.values()
                if run.tenant_id == tenant_id
                and (thread_id is None or run.thread_id == thread_id)
            ]
        return sorted(values, key=lambda run: run.created_at, reverse=True)

    async def claim_run(self, worker_id: str, *, lease_seconds: int = 30) -> Run | None:
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
                            # A fresh delivery attempt must start with
                            # steering admission open -- ``steering_closed``
                            # only means anything for the attempt that set
                            # it (issue #16 PR #17 review round 6).
                            "steering_closed": False,
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
                    "steering_closed": False,
                }
            )
            self._runs[key] = updated
        await self._notify()
        return updated.model_copy(deep=True)

    async def finish_run_if_owned(
        self,
        tenant_id: str,
        run_id: str,
        worker_id: str,
        attempt: int,
        status: RunStatus | str,
        *,
        output: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
    ) -> Run | None:
        """Fenced/CAS'd terminal-status write for a specific lease + attempt.

        Issue #16 PR #17 review round 6, point 1 (BLOCKER): a stale
        worker whose lease has already expired (and been claimed by a new
        owner) must never be able to overwrite that new owner's status.
        This only writes when ``(lease_owner, attempt)`` still match the
        caller's *and* the run is still in an active status -- if either
        no longer holds, it returns ``None`` and touches nothing, and the
        caller must abandon the attempt without any further status
        mutation.

        Review round 6, point 2 (BLOCKER): re-checking the current
        control state (``cancelling`` vs ``running``) happens inside this
        *same* locked decision, atomically with the fence check -- if the
        run is already ``cancelling``, cancellation wins and the actual
        write is coerced to ``cancelled`` regardless of the caller's
        requested ``status``, so a stale in-flight ``succeeded`` intent
        can never overwrite a cancel that arrived during finalization.
        """

        async with self._lock:
            key = (tenant_id, run_id)
            run = self._runs.get(key)
            if (
                run is None
                or run.lease_owner != worker_id
                or run.attempt != attempt
                or enum_value(run.status) not in ACTIVE
            ):
                return None
            final_status = RunStatus(status).value
            final_output = output
            final_error = error
            if (
                enum_value(run.status) == RunStatus.CANCELLING.value
                and final_status != RunStatus.CANCELLED.value
            ):
                final_status = RunStatus.CANCELLED.value
                final_output = None
                final_error = {
                    "code": "run_cancelled",
                    "message": "run was cancelled during finalization",
                }
            updated = run.model_copy(
                update={
                    "status": final_status,
                    "output": final_output,
                    "error": final_error,
                    "finished_at": utcnow(),
                    "lease_owner": None,
                    "lease_expires_at": None,
                }
            )
            self._runs[key] = updated
        await self._notify()
        return updated.model_copy(deep=True)

    async def retry_run_if_owned(
        self,
        tenant_id: str,
        run_id: str,
        worker_id: str,
        attempt: int,
        *,
        error: dict[str, Any] | None = None,
    ) -> Run | None:
        """Fenced/CAS'd retry-to-pending write; see ``finish_run_if_owned``.

        A ``cancelling`` run must never be sent back to ``pending`` by a
        stale worker's retry decision -- cancellation still wins here too,
        resolving directly to ``cancelled`` instead.
        """

        async with self._lock:
            key = (tenant_id, run_id)
            run = self._runs.get(key)
            if (
                run is None
                or run.lease_owner != worker_id
                or run.attempt != attempt
                or enum_value(run.status) not in ACTIVE
            ):
                return None
            if enum_value(run.status) == RunStatus.CANCELLING.value:
                updated = run.model_copy(
                    update={
                        "status": RunStatus.CANCELLED.value,
                        "error": {
                            "code": "run_cancelled",
                            "message": "run was cancelled during finalization",
                        },
                        "finished_at": utcnow(),
                        "lease_owner": None,
                        "lease_expires_at": None,
                    }
                )
            else:
                updated = run.model_copy(
                    update={
                        "status": RunStatus.PENDING.value,
                        "error": error,
                        "finished_at": None,
                        "lease_owner": None,
                        "lease_expires_at": None,
                        "steering_closed": False,
                    }
                )
            self._runs[key] = updated
        await self._notify()
        return updated.model_copy(deep=True)

    async def close_steering(
        self, tenant_id: str, run_id: str, worker_id: str, attempt: int
    ) -> bool:
        """Atomically close new steering admission for this lease + attempt.

        Issue #16 PR #17 review round 6, point 3 (BLOCKER): once graph
        execution has ended there is no safe point left for a newly
        accepted steering event to ever be consumed, even though the Run
        row may still read ``running``/``cancelling`` while the final
        flush is in flight. The owning worker calls this the instant
        execution ends (before the final flush starts) so a concurrent
        ``/steer`` either lands before this gate closes (and gets synced
        in as usual) or after it closes (and is rejected with a stable
        error) -- never durably accepted into a channel nobody will ever
        drain again. Fenced the same way as ``finish_run_if_owned``: a
        stale worker that has already lost its lease cannot close the
        gate on the new owner's behalf.
        """

        async with self._lock:
            key = (tenant_id, run_id)
            run = self._runs.get(key)
            if (
                run is None
                or run.lease_owner != worker_id
                or run.attempt != attempt
                or enum_value(run.status) not in ACTIVE
            ):
                return False
            if not run.steering_closed:
                self._runs[key] = run.model_copy(update={"steering_closed": True})
        return True

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
                    "steering_closed": False,
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

    async def submit_steering(
        self,
        tenant_id: str,
        run_id: str,
        *,
        kind: str,
        payload: dict[str, Any],
        metadata: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
        max_payload_bytes: int = MAX_STEERING_PAYLOAD_BYTES,
    ) -> tuple[RunSteeringEvent, bool]:
        """Durably accept a steering event. Returns ``(event, created)``.

        Terminal runs raise :class:`RunTerminalError`. Running, queued,
        cancelling, and paused runs each accept durably -- the *consuming*
        safe point differs (see module docstring / issue #16 design doc),
        but acceptance semantics are uniform: it means "written to the
        durable inbox", never "the graph has processed it".
        """

        validate_steering_payload(payload, metadata, max_bytes=max_payload_bytes)
        async with self._lock:
            run = self._runs.get((tenant_id, run_id))
            if run is None:
                raise KeyError(f"run {run_id!r} not found")
            if enum_value(run.status) in TERMINAL:
                raise RunTerminalError(
                    f"run {run_id!r} is in terminal state "
                    f"{enum_value(run.status)!r} and cannot accept new steering input"
                )
            if run.steering_closed:
                raise RunFinalizingError(
                    f"run {run_id!r} has finished executing and is finalizing; "
                    "no further steering input can be safely consumed"
                )
            superseded_by = run.metadata.get("superseded_by_run_id")
            if superseded_by is not None:
                raise RunSupersededError(
                    f"run {run_id!r} was resumed as {superseded_by!r}; "
                    f"steer the new run instead"
                )
            if idempotency_key is not None:
                existing_id = self._steering_keys.get((tenant_id, run_id, idempotency_key))
                if existing_id is not None:
                    existing = next(
                        event
                        for event in self._steering[(tenant_id, run_id)]
                        if event.id == existing_id
                    )
                    # A prior attempt with this idempotency key may have
                    # committed the steering row but then failed before its
                    # ``run.steer.accepted`` event was recorded (review
                    # round 4, point 2) -- repair that gap on the retry
                    # instead of permanently skipping it.
                    self._ensure_steer_accepted_event_locked(tenant_id, run_id, existing)
                    return existing.model_copy(deep=True), False
            events = self._steering.setdefault((tenant_id, run_id), [])
            event = RunSteeringEvent(
                tenant_id=tenant_id,
                run_id=run_id,
                sequence=len(events) + 1,
                kind=kind,
                payload=dict(payload),
                metadata=dict(metadata or {}),
                idempotency_key=idempotency_key,
                status="pending",
            )
            events.append(event)
            if idempotency_key is not None:
                self._steering_keys[(tenant_id, run_id, idempotency_key)] = event.id
            self._ensure_steer_accepted_event_locked(tenant_id, run_id, event)
        await self._notify()
        return event.model_copy(deep=True), True

    def _ensure_steer_accepted_event_locked(
        self,
        tenant_id: str,
        run_id: str,
        steering_event: RunSteeringEvent,
        *,
        transferred_from_run_id: str | None = None,
    ) -> RunEvent:
        """Idempotently ensure ``run.steer.accepted`` exists for one event.

        Issue #16 PR #17 review round 4, point 2: appending the
        ``run.steer.accepted`` lifecycle event used to be a second,
        separately-committed step after ``submit_steering`` (or the
        paused-resume steering migration) -- a transient failure on that
        second step, followed by an idempotent-key retry that finds the
        steering row already created, permanently skipped the event
        forever. Doing both under the caller's already-held ``self._lock``
        and keying this on the steering event's id (never appending twice
        for the same id) makes "accept the steering row" and "record the
        accepted lifecycle event" a single atomic, idempotent unit -- a
        retry (of the request, or of this call) always converges on
        exactly one ``run.steer.accepted`` event per steering event id.
        Assumes ``self._lock`` is already held.
        """

        run_events = self._events.setdefault((tenant_id, run_id), [])
        for existing in run_events:
            if (
                existing.kind == "run.steer.accepted"
                and existing.data.get("steering_event_id") == steering_event.id
            ):
                return existing
        data: dict[str, Any] = {
            "steering_event_id": steering_event.id,
            "sequence": steering_event.sequence,
            "kind": steering_event.kind,
        }
        if transferred_from_run_id is not None:
            data["transferred_from_run_id"] = transferred_from_run_id
            data["source_event_id"] = steering_event.source_event_id
        event = RunEvent(
            tenant_id=tenant_id,
            run_id=run_id,
            sequence=len(run_events) + 1,
            kind="run.steer.accepted",
            data=data,
        )
        run_events.append(event)
        return event

    async def list_pending_steering(
        self, tenant_id: str, run_id: str
    ) -> list[RunSteeringEvent]:
        """Read-only view of never-consumed steering events, in order."""

        async with self._lock:
            return [
                event.model_copy(deep=True)
                for event in self._steering.get((tenant_id, run_id), ())
                if event.status in ("pending", "delivered")
            ]

    async def mark_steering_consumed(
        self, tenant_id: str, run_id: str, event_ids: Sequence[str]
    ) -> None:
        """Mark events consumed once the graph has actually drained them."""

        if not event_ids:
            return
        ids = set(event_ids)
        async with self._lock:
            events = self._steering.get((tenant_id, run_id), [])
            for index, event in enumerate(events):
                if event.id in ids and event.status != "consumed":
                    events[index] = event.model_copy(
                        update={"status": "consumed", "consumed_at": utcnow()}
                    )

    async def commit_steering_consumptions(
        self,
        tenant_id: str,
        run_id: str,
        consumptions: Sequence[SteeringConsumption],
    ) -> list[RunEvent]:
        """Durably record consumption (status + lifecycle event), atomically.

        Issue #16 PR #17 review point 2: ``mark_steering_consumed`` plus a
        separate ``append_event("run.steer.consumed")`` call left a window
        where a caller that destructively popped its local consumption log
        *before* calling either could lose the record forever on a
        transient failure -- the DB row could end up stuck ``pending``
        forever (no ``on_drain``/dedup path re-surfaces it once
        ``SteeringChannel._seen_ids`` has recorded the id), or the status
        could flip to ``consumed`` while the ``run.steer.consumed``
        observability event silently never appears. This method updates
        both under one lock acquisition so a caller (see
        ``Worker._sync_steering_out``) can safely defer clearing its own
        local bookkeeping until *after* this succeeds, and retry on the
        next safe point if it raises.
        """

        if not consumptions:
            return []
        async with self._lock:
            ids = {consumption.event.id for consumption in consumptions}
            events = self._steering.get((tenant_id, run_id), [])
            transitioned: set[str] = set()
            for index, event in enumerate(events):
                if event.id in ids and event.status != "consumed":
                    events[index] = event.model_copy(
                        update={"status": "consumed", "consumed_at": utcnow()}
                    )
                    transitioned.add(event.id)
            run_events = self._events.setdefault((tenant_id, run_id), [])
            stored: list[RunEvent] = []
            for consumption in consumptions:
                steering_event = consumption.event
                if steering_event.id not in transitioned:
                    # Already consumed by a previous call (idempotent
                    # retry, e.g. a worker that re-sends the same batch
                    # after an ack it never observed) -- do not emit a
                    # second, semantically duplicate ``run.steer.consumed``
                    # lifecycle event for it (issue #16 PR #17 review
                    # point 3).
                    continue
                data: dict[str, Any] = {
                    "steering_event_id": steering_event.id,
                    "sequence": steering_event.sequence,
                    "kind": steering_event.kind,
                    "queue_latency_seconds": consumption.queue_latency_seconds,
                    "node": consumption.node,
                    "namespace": list(consumption.namespace),
                    "task_id": consumption.task_id,
                }
                if steering_event.source_event_id is not None:
                    data["source_event_id"] = steering_event.source_event_id
                stored_event = RunEvent(
                    tenant_id=tenant_id,
                    run_id=run_id,
                    sequence=len(run_events) + 1,
                    kind="run.steer.consumed",
                    data=data,
                )
                run_events.append(stored_event)
                stored.append(stored_event.model_copy(deep=True))
        await self._notify()
        return stored

    async def list_steering(self, tenant_id: str, run_id: str) -> list[RunSteeringEvent]:
        async with self._lock:
            return [
                event.model_copy(deep=True)
                for event in self._steering.get((tenant_id, run_id), ())
            ]

    def _transfer_pending_steering_locked(
        self, tenant_id: str, old_run_id: str, new_run_id: str
    ) -> list[RunSteeringEvent]:
        """The body of the old-run -> new-run steering migration.

        Assumes ``self._lock`` is already held -- only called from
        :meth:`resume_run_with_pending_steering`, which creates the new Run
        and performs this migration under one lock acquisition (see issue
        #16 PR #17 review point 1's "not atomic" finding: this must never
        run as a second, separately-lockable step after the new Run is
        already visible to :meth:`claim_run`).

        Every event still ``pending``/``delivered`` under ``old_run_id`` is
        marked ``superseded`` in place (its terminal state -- it was never
        drained by the paused run) and re-inserted as a fresh ``pending``
        event under ``new_run_id``, preserving order, ``kind``, ``payload``,
        ``metadata`` and ``idempotency_key``. The new row's ``created_at``
        and ``source_event_id`` preserve the original event's identity
        (review point 3): external callers correlate the id a paused-run
        ``/steer`` call returned to the eventual ``consumed`` event via
        ``source_event_id``, and ``queue_latency_seconds`` computed from the
        preserved ``created_at`` includes the time spent waiting while
        paused, not just time since the transfer.
        """

        old_run = self._runs.get((tenant_id, old_run_id))
        if old_run is not None:
            self._runs[(tenant_id, old_run_id)] = old_run.model_copy(
                update={
                    "metadata": {
                        **old_run.metadata,
                        "superseded_by_run_id": new_run_id,
                    }
                }
            )
        old_events = self._steering.get((tenant_id, old_run_id), [])
        pending = sorted(
            (event for event in old_events if event.status in ("pending", "delivered")),
            key=lambda event: event.sequence,
        )
        if not pending:
            return []
        for index, event in enumerate(old_events):
            if event.status in ("pending", "delivered"):
                old_events[index] = event.model_copy(update={"status": "superseded"})
        new_events = self._steering.setdefault((tenant_id, new_run_id), [])
        transferred: list[RunSteeringEvent] = []
        for event in pending:
            new_event = RunSteeringEvent(
                tenant_id=tenant_id,
                run_id=new_run_id,
                sequence=len(new_events) + 1,
                kind=event.kind,
                payload=dict(event.payload),
                metadata=dict(event.metadata),
                idempotency_key=event.idempotency_key,
                status="pending",
                source_event_id=event.source_event_id or event.id,
                created_at=event.created_at,
            )
            new_events.append(new_event)
            if event.idempotency_key is not None:
                self._steering_keys[(tenant_id, new_run_id, event.idempotency_key)] = (
                    new_event.id
                )
            # Record the transferred event's ``run.steer.accepted`` inside
            # this same locked migration (review round 4, point 2) instead
            # of leaving it to a second, separately-committed
            # ``append_event`` call after the endpoint returns from this
            # method -- otherwise a transient failure of that later call
            # permanently loses the accepted record for the transferred run.
            self._ensure_steer_accepted_event_locked(
                tenant_id, new_run_id, new_event, transferred_from_run_id=old_run_id
            )
            transferred.append(new_event.model_copy(deep=True))
        return transferred

    async def resume_run_with_pending_steering(
        self,
        tenant_id: str,
        thread_id: str | None,
        assistant: Assistant,
        request: RunCreate,
        old_run_id: str,
    ) -> tuple[Run, list[RunSteeringEvent]]:
        """Atomically create the resumed Run and migrate paused steering.

        Issue #16 PR #17 review point 1: creating the resumed Run and
        transferring its predecessor's still-pending steering used to be
        two separately-locked operations (``create_run`` then
        ``transfer_pending_steering``); a worker could claim -- and even
        finish -- the brand-new ``pending`` Run in the window between them,
        so the migrated steering could land after nobody was left to
        consume it. Doing both under one ``self._lock`` acquisition (no
        ``await`` in between) means no other coroutine, including
        ``claim_run``, can observe the new Run row before its steering
        migration has also completed.

        Issue #16 PR #17 review round 4, point 1: the endpoint's own
        ``GET old run -> status == paused`` pre-check happens *before*
        this call and is therefore not atomic with it -- two concurrent
        resume requests can both pass it. Re-validate, inside this same
        lock acquisition, that the old Run is still ``paused`` and has no
        ``superseded_by_run_id`` set yet; the loser of the race gets
        :class:`RunResumeConflictError` instead of silently creating a
        second descendant Run and overwriting the winner's
        ``superseded_by_run_id``.
        """

        async with self._lock:
            old_run = self._runs.get((tenant_id, old_run_id))
            if (
                old_run is None
                or enum_value(old_run.status) != RunStatus.PAUSED.value
                or old_run.metadata.get("superseded_by_run_id") is not None
            ):
                raise RunResumeConflictError(
                    f"run {old_run_id!r} is no longer resumable "
                    "(it is not paused, or has already been resumed by a concurrent request)"
                )
            run = self._create_run_locked(tenant_id, thread_id, assistant, request)
            transferred = self._transfer_pending_steering_locked(tenant_id, old_run_id, run.id)
        await self._notify()
        return run.model_copy(deep=True), transferred

    async def create_schedule(self, tenant_id: str, request: ScheduleCreate) -> Schedule:
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
                "assistants": sum(tenant == tenant_id for tenant, _ in self._assistants),
                "schedules": sum(tenant == tenant_id for tenant, _ in self._schedules),
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
            raise RuntimeError(
                "install lingxigraph[postgres] to use PostgresRepository"
            ) from exc
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

        migrations_dir = files("lingxigraph.server").joinpath("migrations")
        names = sorted(
            entry.name for entry in migrations_dir.iterdir() if entry.name.endswith(".sql")
        )
        with self._connect() as conn, conn.cursor() as cursor:
            for name in names:
                migration = (
                    migrations_dir.joinpath(name)
                    .read_text(encoding="utf-8")
                    .replace("{{schema}}", self._schema)
                )
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
        await asyncio.to_thread(self._finish_run_sync, tenant_id, run_id, status, output, error)
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

    async def finish_run_if_owned(
        self, tenant_id, run_id, worker_id, attempt, status, *, output=None, error=None
    ) -> Run | None:
        changed = await asyncio.to_thread(
            self._finish_run_if_owned_sync,
            tenant_id,
            run_id,
            worker_id,
            attempt,
            status,
            output,
            error,
        )
        if not changed:
            return None
        value = await self.get_run(tenant_id, run_id)
        assert value is not None
        return value

    def _finish_run_if_owned_sync(
        self, tenant_id, run_id, worker_id, attempt, status, output, error
    ) -> bool:
        # Fenced/CAS'd terminal-status write (issue #16 PR #17 review round
        # 6, point 1, BLOCKER). Selecting the row ``FOR UPDATE`` first --
        # filtered on lease_owner/attempt/status, mirroring the pattern
        # ``_submit_steering_sync``/``_append_event_sync`` already use --
        # then issuing a plain follow-up ``UPDATE`` inside the same
        # transaction is deliberately simpler (and easier to verify by
        # inspection without a live Postgres) than trying to express the
        # fencing *and* the cancellation-wins re-check as one combined
        # CASE-heavy UPDATE statement.
        with self._connect() as conn, conn.cursor() as cursor:
            self._tenant(cursor, tenant_id)
            cursor.execute(
                f"""SELECT status FROM {self._schema}.runs
                WHERE tenant_id=%s AND id=%s AND lease_owner=%s AND attempt=%s
                  AND status IN ('running','cancelling') FOR UPDATE""",
                (tenant_id, run_id, worker_id, attempt),
            )
            row = cursor.fetchone()
            if row is None:
                return False
            final_status = RunStatus(status).value
            final_output = output
            final_error = error
            # Review round 6, point 2 (BLOCKER): re-check the current
            # control state inside this same locked transaction --
            # cancellation always wins over a stale non-cancel intent.
            if row["status"] == "cancelling" and final_status != RunStatus.CANCELLED.value:
                final_status = RunStatus.CANCELLED.value
                final_output = None
                final_error = {
                    "code": "run_cancelled",
                    "message": "run was cancelled during finalization",
                }
            cursor.execute(
                f"""UPDATE {self._schema}.runs SET status=%s, output=%s, error=%s,
                finished_at=NOW(), lease_owner=NULL, lease_expires_at=NULL
                WHERE tenant_id=%s AND id=%s""",
                (
                    final_status,
                    self._jsonb(final_output) if final_output is not None else None,
                    self._jsonb(final_error) if final_error is not None else None,
                    tenant_id,
                    run_id,
                ),
            )
            return True

    async def retry_run_if_owned(
        self, tenant_id, run_id, worker_id, attempt, *, error=None
    ) -> Run | None:
        changed = await asyncio.to_thread(
            self._retry_run_if_owned_sync, tenant_id, run_id, worker_id, attempt, error
        )
        if not changed:
            return None
        value = await self.get_run(tenant_id, run_id)
        assert value is not None
        return value

    def _retry_run_if_owned_sync(self, tenant_id, run_id, worker_id, attempt, error) -> bool:
        with self._connect() as conn, conn.cursor() as cursor:
            self._tenant(cursor, tenant_id)
            cursor.execute(
                f"""SELECT status FROM {self._schema}.runs
                WHERE tenant_id=%s AND id=%s AND lease_owner=%s AND attempt=%s
                  AND status IN ('running','cancelling') FOR UPDATE""",
                (tenant_id, run_id, worker_id, attempt),
            )
            row = cursor.fetchone()
            if row is None:
                return False
            if row["status"] == "cancelling":
                # Cancellation wins: never send a cancelling run back to
                # pending because of a stale worker's retry decision.
                cursor.execute(
                    f"""UPDATE {self._schema}.runs SET status='cancelled', error=%s,
                    finished_at=NOW(), lease_owner=NULL, lease_expires_at=NULL
                    WHERE tenant_id=%s AND id=%s""",
                    (
                        self._jsonb(
                            {
                                "code": "run_cancelled",
                                "message": "run was cancelled during finalization",
                            }
                        ),
                        tenant_id,
                        run_id,
                    ),
                )
            else:
                cursor.execute(
                    f"""UPDATE {self._schema}.runs SET status='pending',
                    error=%s, finished_at=NULL, lease_owner=NULL, lease_expires_at=NULL,
                    steering_closed=FALSE
                    WHERE tenant_id=%s AND id=%s""",
                    (self._jsonb(error) if error is not None else None, tenant_id, run_id),
                )
            return True

    async def close_steering(self, tenant_id, run_id, worker_id, attempt) -> bool:
        return await asyncio.to_thread(
            self._close_steering_sync, tenant_id, run_id, worker_id, attempt
        )

    def _close_steering_sync(self, tenant_id, run_id, worker_id, attempt) -> bool:
        with self._connect() as conn, conn.cursor() as cursor:
            self._tenant(cursor, tenant_id)
            cursor.execute(
                f"""UPDATE {self._schema}.runs SET steering_closed=TRUE
                WHERE tenant_id=%s AND id=%s AND lease_owner=%s AND attempt=%s
                  AND status IN ('running','cancelling')""",
                (tenant_id, run_id, worker_id, attempt),
            )
            return cursor.rowcount > 0

    def _retry_run_sync(self, tenant_id, run_id, error, reset_attempt):
        with self._connect() as conn, conn.cursor() as cursor:
            self._tenant(cursor, tenant_id)
            allowed = "AND status IN ('failed','dead_letter')" if reset_attempt else ""
            attempt = "attempt=0," if reset_attempt else ""
            cursor.execute(
                f"""UPDATE {self._schema}.runs SET status='pending', {attempt}
                error=%s, finished_at=NULL, lease_owner=NULL, lease_expires_at=NULL,
                steering_closed=FALSE
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
        return await asyncio.to_thread(self._append_event_sync, tenant_id, run_id, kind, data)

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

    async def submit_steering(
        self,
        tenant_id,
        run_id,
        *,
        kind,
        payload,
        metadata=None,
        idempotency_key=None,
        max_payload_bytes: int = MAX_STEERING_PAYLOAD_BYTES,
    ):
        validate_steering_payload(payload, metadata, max_bytes=max_payload_bytes)
        return await asyncio.to_thread(
            self._submit_steering_sync,
            tenant_id,
            run_id,
            kind,
            payload,
            metadata or {},
            idempotency_key,
        )

    def _submit_steering_sync(
        self, tenant_id, run_id, kind, payload, metadata, idempotency_key
    ):
        with self._connect() as conn, conn.cursor() as cursor:
            self._tenant(cursor, tenant_id)
            cursor.execute(
                f"""SELECT status, metadata, steering_closed FROM {self._schema}.runs
                WHERE tenant_id=%s AND id=%s FOR UPDATE""",
                (tenant_id, run_id),
            )
            run_row = cursor.fetchone()
            if run_row is None:
                raise KeyError(f"run {run_id!r} not found")
            if run_row["status"] in TERMINAL:
                raise RunTerminalError(
                    f"run {run_id!r} is in terminal state {run_row['status']!r} "
                    "and cannot accept new steering input"
                )
            if run_row.get("steering_closed"):
                raise RunFinalizingError(
                    f"run {run_id!r} has finished executing and is finalizing; "
                    "no further steering input can be safely consumed"
                )
            run_metadata = run_row.get("metadata") or {}
            superseded_by = run_metadata.get("superseded_by_run_id")
            if superseded_by is not None:
                raise RunSupersededError(
                    f"run {run_id!r} was resumed as {superseded_by!r}; "
                    f"steer the new run instead"
                )
            if idempotency_key is not None:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (f"{tenant_id}:{run_id}:{idempotency_key}",),
                )
                cursor.execute(
                    f"""SELECT * FROM {self._schema}.run_steering_events
                    WHERE tenant_id=%s AND run_id=%s AND idempotency_key=%s""",
                    (tenant_id, run_id, idempotency_key),
                )
                existing = cursor.fetchone()
                if existing is not None:
                    existing_event = RunSteeringEvent.model_validate(existing)
                    # A prior attempt with this idempotency key may have
                    # committed the steering row but then failed before its
                    # ``run.steer.accepted`` event was recorded (issue #16
                    # PR #17 review round 4, point 2) -- repair that gap on
                    # the retry instead of permanently skipping it.
                    self._ensure_steer_accepted_event_sync(
                        cursor, tenant_id, run_id, existing_event
                    )
                    return existing_event, False
            cursor.execute(
                f"""SELECT COALESCE(MAX(sequence),0)+1 AS next
                FROM {self._schema}.run_steering_events WHERE tenant_id=%s AND run_id=%s""",
                (tenant_id, run_id),
            )
            sequence = int(cursor.fetchone()["next"])
            event = RunSteeringEvent(
                tenant_id=tenant_id,
                run_id=run_id,
                sequence=sequence,
                kind=kind,
                payload=dict(payload),
                metadata=dict(metadata or {}),
                idempotency_key=idempotency_key,
                status="pending",
            )
            cursor.execute(
                f"""INSERT INTO {self._schema}.run_steering_events
                (id,tenant_id,run_id,sequence,kind,payload,metadata,idempotency_key,
                 status,created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    event.id,
                    tenant_id,
                    run_id,
                    event.sequence,
                    kind,
                    self._jsonb(event.payload),
                    self._jsonb(event.metadata),
                    idempotency_key,
                    event.status,
                    event.created_at,
                ),
            )
            # Accept the steering row and record its ``run.steer.accepted``
            # lifecycle event in the same transaction (issue #16 PR #17
            # review round 4, point 2) -- both commit together, or neither
            # does, so a retry can never observe the row without its
            # accepted event.
            self._ensure_steer_accepted_event_sync(cursor, tenant_id, run_id, event)
            return event, True

    def _ensure_steer_accepted_event_sync(
        self,
        cursor,
        tenant_id: str,
        run_id: str,
        steering_event: RunSteeringEvent,
        *,
        transferred_from_run_id: str | None = None,
    ) -> None:
        """Idempotently ensure ``run.steer.accepted`` exists for one event.

        Must be called with the target run's row already locked (``FOR
        UPDATE`` / an implicit lock from an ``INSERT`` earlier in this same
        transaction) so concurrent sequence-number assignment for
        ``run_events`` serializes -- same discipline as
        ``_append_event_sync`` / ``_commit_steering_consumptions_sync``.
        """

        cursor.execute(
            f"""SELECT 1 FROM {self._schema}.run_events
            WHERE tenant_id=%s AND run_id=%s AND kind='run.steer.accepted'
              AND data->>'steering_event_id'=%s""",
            (tenant_id, run_id, steering_event.id),
        )
        if cursor.fetchone() is not None:
            return
        cursor.execute(
            f"""SELECT COALESCE(MAX(sequence),0)+1 AS next
            FROM {self._schema}.run_events WHERE tenant_id=%s AND run_id=%s""",
            (tenant_id, run_id),
        )
        sequence = int(cursor.fetchone()["next"])
        data: dict[str, Any] = {
            "steering_event_id": steering_event.id,
            "sequence": steering_event.sequence,
            "kind": steering_event.kind,
        }
        if transferred_from_run_id is not None:
            data["transferred_from_run_id"] = transferred_from_run_id
            data["source_event_id"] = steering_event.source_event_id
        run_event = RunEvent(
            tenant_id=tenant_id,
            run_id=run_id,
            sequence=sequence,
            kind="run.steer.accepted",
            data=data,
        )
        cursor.execute(
            f"""INSERT INTO {self._schema}.run_events
            (id,tenant_id,run_id,sequence,kind,data,created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (
                run_event.id,
                tenant_id,
                run_id,
                sequence,
                "run.steer.accepted",
                self._jsonb(data),
                run_event.created_at,
            ),
        )

    async def list_pending_steering(self, tenant_id, run_id):
        rows = await asyncio.to_thread(
            self._fetch_all,
            tenant_id,
            f"""SELECT * FROM {self._schema}.run_steering_events
            WHERE tenant_id=%s AND run_id=%s AND status IN ('pending','delivered')
            ORDER BY sequence""",
            (tenant_id, run_id),
        )
        return [RunSteeringEvent.model_validate(row) for row in rows]

    async def list_steering(self, tenant_id, run_id):
        rows = await asyncio.to_thread(
            self._fetch_all,
            tenant_id,
            f"""SELECT * FROM {self._schema}.run_steering_events
            WHERE tenant_id=%s AND run_id=%s ORDER BY sequence""",
            (tenant_id, run_id),
        )
        return [RunSteeringEvent.model_validate(row) for row in rows]

    async def mark_steering_consumed(self, tenant_id, run_id, event_ids):
        if not event_ids:
            return
        await asyncio.to_thread(
            self._mark_steering_consumed_sync, tenant_id, run_id, list(event_ids)
        )

    def _mark_steering_consumed_sync(self, tenant_id, run_id, event_ids):
        with self._connect() as conn, conn.cursor() as cursor:
            self._tenant(cursor, tenant_id)
            cursor.execute(
                f"""UPDATE {self._schema}.run_steering_events
                SET status='consumed', consumed_at=NOW()
                WHERE tenant_id=%s AND run_id=%s AND id=ANY(%s) AND status!='consumed'""",
                (tenant_id, run_id, event_ids),
            )

    async def commit_steering_consumptions(self, tenant_id, run_id, consumptions):
        if not consumptions:
            return []
        return await asyncio.to_thread(
            self._commit_steering_consumptions_sync, tenant_id, run_id, list(consumptions)
        )

    def _commit_steering_consumptions_sync(
        self, tenant_id: str, run_id: str, consumptions: list[SteeringConsumption]
    ) -> list[RunEvent]:
        """SQL counterpart of
        :meth:`InMemoryRepository.commit_steering_consumptions` -- see its
        docstring (issue #16 PR #17 review point 2) for why the status
        update and the ``run.steer.consumed`` event append must commit
        together, in the same transaction, before the caller acks its
        local consumption log.
        """

        ids = [consumption.event.id for consumption in consumptions]
        with self._connect() as conn, conn.cursor() as cursor:
            self._tenant(cursor, tenant_id)
            cursor.execute(
                f"""UPDATE {self._schema}.run_steering_events
                SET status='consumed', consumed_at=NOW()
                WHERE tenant_id=%s AND run_id=%s AND id=ANY(%s) AND status!='consumed'
                RETURNING id""",
                (tenant_id, run_id, ids),
            )
            # Only ids that *actually* transitioned pending/delivered ->
            # consumed in this call. A retried batch (e.g. a worker that
            # re-sends consumptions after a commit whose ack it never saw)
            # must not emit a second, semantically duplicate
            # ``run.steer.consumed`` lifecycle event for a row some
            # earlier call already consumed (issue #16 PR #17 review
            # point 3).
            transitioned = {row["id"] for row in cursor.fetchall()}
            # Same locking discipline as ``_append_event_sync``: lock the
            # run row first so concurrent event appenders for this run
            # serialize their sequence assignment instead of racing.
            cursor.execute(
                f"SELECT id FROM {self._schema}.runs WHERE tenant_id=%s AND id=%s FOR UPDATE",
                (tenant_id, run_id),
            )
            cursor.execute(
                f"""SELECT COALESCE(MAX(sequence),0) AS next
                FROM {self._schema}.run_events WHERE tenant_id=%s AND run_id=%s""",
                (tenant_id, run_id),
            )
            next_sequence = int(cursor.fetchone()["next"])
            stored: list[RunEvent] = []
            for consumption in consumptions:
                event = consumption.event
                if event.id not in transitioned:
                    continue
                next_sequence += 1
                data: dict[str, Any] = {
                    "steering_event_id": event.id,
                    "sequence": event.sequence,
                    "kind": event.kind,
                    "queue_latency_seconds": consumption.queue_latency_seconds,
                    "node": consumption.node,
                    "namespace": list(consumption.namespace),
                    "task_id": consumption.task_id,
                }
                if event.source_event_id is not None:
                    data["source_event_id"] = event.source_event_id
                run_event = RunEvent(
                    tenant_id=tenant_id,
                    run_id=run_id,
                    sequence=next_sequence,
                    kind="run.steer.consumed",
                    data=data,
                )
                cursor.execute(
                    f"""INSERT INTO {self._schema}.run_events
                    (id,tenant_id,run_id,sequence,kind,data,created_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                    (
                        run_event.id,
                        tenant_id,
                        run_id,
                        run_event.sequence,
                        run_event.kind,
                        self._jsonb(run_event.data),
                        run_event.created_at,
                    ),
                )
                stored.append(run_event)
            return stored

    async def resume_run_with_pending_steering(
        self, tenant_id, thread_id, assistant, request, old_run_id
    ):
        return await asyncio.to_thread(
            self._resume_run_with_pending_steering_sync,
            tenant_id,
            thread_id,
            assistant,
            request,
            old_run_id,
        )

    def _resume_run_with_pending_steering_sync(
        self, tenant_id, thread_id, assistant, request, old_run_id
    ):
        """SQL counterpart of
        :meth:`InMemoryRepository.resume_run_with_pending_steering`.

        Issue #16 PR #17 review point 1: the resumed Run's INSERT and the
        old Run's steering migration must commit as one all-or-nothing unit
        so no other transaction can ever see the new Run row without its
        migrated steering already alongside it. Using a single connection
        for both means they share one PostgreSQL transaction (autocommit is
        off; the ``with self._connect()`` block only commits when it exits
        normally) -- ``claim_run`` runs its own ``UPDATE ... FOR UPDATE
        SKIP LOCKED`` against ``runs`` in a *different* connection/
        transaction, so it cannot observe the freshly INSERTed new-Run row
        at all until this transaction commits, by which point the
        migration below has already happened.

        This also closes the second race the review flagged: an ordinary
        concurrent ``/steer`` against the new run (``_submit_steering_sync``)
        takes ``SELECT ... FOR UPDATE`` on the **runs** row for
        ``new_run.id`` before computing ``MAX(sequence)+1`` over
        ``run_steering_events``. Because the new run's row was INSERTed --
        and is therefore implicitly locked -- inside *this* transaction,
        that concurrent ``FOR UPDATE`` blocks until this transaction
        commits, so the two ``MAX(sequence)+1`` computations for the same
        ``new_run_id`` can never race.
        """

        with self._connect() as conn, conn.cursor() as cursor:
            self._tenant(cursor, tenant_id)
            # Lock the paused run first so two concurrent resume attempts
            # against the same paused run_id serialize instead of both
            # migrating (and superseding) the same steering rows.
            cursor.execute(
                f"""SELECT status, metadata FROM {self._schema}.runs
                WHERE tenant_id=%s AND id=%s FOR UPDATE""",
                (tenant_id, old_run_id),
            )
            old_run_row = cursor.fetchone()
            # Issue #16 PR #17 review round 4, point 1: the endpoint's own
            # pre-check (``GET old run -> status == paused``) races with
            # concurrent resumes and is not itself atomic with this call.
            # Re-validate under the lock just acquired above -- still
            # ``paused``, and not already superseded by a resume that won
            # the race -- before creating a second descendant Run.
            if old_run_row is None:
                raise RunResumeConflictError(f"run {old_run_id!r} no longer exists")
            old_run_metadata = dict(old_run_row.get("metadata") or {})
            if (
                old_run_row.get("status") != "paused"
                or old_run_metadata.get("superseded_by_run_id") is not None
            ):
                raise RunResumeConflictError(
                    f"run {old_run_id!r} is no longer resumable "
                    "(it is not paused, or has already been resumed by a concurrent request)"
                )

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

            old_run_metadata["superseded_by_run_id"] = run.id
            cursor.execute(
                f"""UPDATE {self._schema}.runs SET metadata=%s
                WHERE tenant_id=%s AND id=%s""",
                (self._jsonb(old_run_metadata), tenant_id, old_run_id),
            )

            cursor.execute(
                f"""SELECT * FROM {self._schema}.run_steering_events
                WHERE tenant_id=%s AND run_id=%s AND status IN ('pending','delivered')
                ORDER BY sequence FOR UPDATE""",
                (tenant_id, old_run_id),
            )
            pending_rows = cursor.fetchall()
            transferred: list[RunSteeringEvent] = []
            if pending_rows:
                pending_ids = [row["id"] for row in pending_rows]
                cursor.execute(
                    f"""UPDATE {self._schema}.run_steering_events
                    SET status='superseded' WHERE tenant_id=%s AND id=ANY(%s)""",
                    (tenant_id, pending_ids),
                )
                # Safe against a concurrent ordinary /steer on the new run:
                # both this and ``_submit_steering_sync`` first take
                # ``FOR UPDATE`` on the *new run's* ``runs`` row -- which
                # this transaction already holds implicitly from the
                # INSERT above -- before computing MAX(sequence)+1 here.
                cursor.execute(
                    f"""SELECT COALESCE(MAX(sequence),0) AS next
                    FROM {self._schema}.run_steering_events WHERE tenant_id=%s AND run_id=%s""",
                    (tenant_id, run.id),
                )
                next_sequence = int(cursor.fetchone()["next"])
                for row in pending_rows:
                    next_sequence += 1
                    event = RunSteeringEvent(
                        tenant_id=tenant_id,
                        run_id=run.id,
                        sequence=next_sequence,
                        kind=row["kind"],
                        payload=dict(row["payload"] or {}),
                        metadata=dict(row["metadata"] or {}),
                        idempotency_key=row["idempotency_key"],
                        status="pending",
                        source_event_id=row["source_event_id"] or row["id"],
                        created_at=row["created_at"],
                    )
                    cursor.execute(
                        f"""INSERT INTO {self._schema}.run_steering_events
                        (id,tenant_id,run_id,sequence,kind,payload,metadata,idempotency_key,
                         status,created_at,source_event_id)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (
                            event.id,
                            tenant_id,
                            run.id,
                            event.sequence,
                            event.kind,
                            self._jsonb(event.payload),
                            self._jsonb(event.metadata),
                            event.idempotency_key,
                            event.status,
                            event.created_at,
                            event.source_event_id,
                        ),
                    )
                    # Record the transferred event's ``run.steer.accepted``
                    # inside this same migration transaction (review round
                    # 4, point 2) instead of a separate ``append_event``
                    # call after this method returns.
                    self._ensure_steer_accepted_event_sync(
                        cursor,
                        tenant_id,
                        run.id,
                        event,
                        transferred_from_run_id=old_run_id,
                    )
                    transferred.append(event)
            return run, transferred

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
                    f"SELECT COUNT(*) AS count FROM {self._schema}.{name} WHERE tenant_id=%s",
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
                    lease_owner=NULL, lease_expires_at=NULL, steering_closed=FALSE
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
            value[name] = value.get(name) or (
                {} if name in {"context", "config", "metadata"} else None
            )
        return Run.model_validate(value)


__all__ = [
    "ACTIVE",
    "InMemoryRepository",
    "PostgresRepository",
    "RepositoryLimits",
    "TERMINAL",
]
