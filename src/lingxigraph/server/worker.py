"""Lease-based distributed graph worker."""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import socket
import time
from collections.abc import Callable
from typing import Any
from uuid import uuid4

from ..cache import BaseCache
from ..checkpoint import Checkpointer, InMemorySaver
from ..errors import (
    BudgetExceededError,
    GraphCancelledError,
    GraphTimeoutError,
    GraphValidationError,
    InvalidUpdateError,
    PersistenceError,
)
from ..events import Event, EventKind
from ..runtime import CancellationToken
from ..serialization import JsonSerializer
from ..store import BaseStore, InMemoryStore
from ..types import Command, RunStatus
from .eventbus import EventBus, InMemoryEventBus
from .models import Run
from .registry import GraphRegistry
from .repository import InMemoryRepository

logger = logging.getLogger("lingxigraph.worker")


class Worker:
    def __init__(
        self,
        registry: GraphRegistry,
        repository: InMemoryRepository,
        *,
        checkpointer: Checkpointer | None = None,
        store_factory: Callable[[str], BaseStore] | None = None,
        cache: BaseCache | None = None,
        event_bus: EventBus | None = None,
        worker_id: str | None = None,
        lease_seconds: int = 30,
        heartbeat_seconds: float = 5.0,
        max_delivery_attempts: int = 5,
    ) -> None:
        self.registry = registry
        self.repository = repository
        self.checkpointer = checkpointer or InMemorySaver()
        shared_store = InMemoryStore()
        self.store_factory = store_factory or (lambda _tenant: shared_store)
        self.cache = cache
        self.event_bus = event_bus or InMemoryEventBus()
        self.worker_id = worker_id or f"{socket.gethostname()}:{uuid4()}"
        self.lease_seconds = lease_seconds
        self.heartbeat_seconds = heartbeat_seconds
        self.max_delivery_attempts = max_delivery_attempts
        self._stop = asyncio.Event()
        self._idle = asyncio.Event()
        self._idle.set()
        self._last_loop = time.monotonic()
        self._serializer = JsonSerializer()

    @property
    def draining(self) -> bool:
        return self._stop.is_set()

    @property
    def ready(self) -> bool:
        return not self.draining

    @property
    def live(self) -> bool:
        return time.monotonic() - self._last_loop < max(30.0, self.heartbeat_seconds * 4)

    async def run_once(self) -> bool:
        self._last_loop = time.monotonic()
        if self.draining:
            return False
        run = await self.repository.claim_run(
            self.worker_id, lease_seconds=self.lease_seconds
        )
        if run is None:
            return False
        logger.info(
            "run claimed",
            extra={
                "run_id": run.id,
                "tenant_id": run.tenant_id,
                "graph_id": run.graph_id,
                "graph_version": run.graph_version,
            },
        )
        self._idle.clear()
        try:
            await self._execute(run)
        finally:
            self._idle.set()
            self._last_loop = time.monotonic()
        return True

    async def run_forever(self, *, poll_interval: float = 0.25) -> None:
        while not self._stop.is_set():
            self._last_loop = time.monotonic()
            claimed = await self.run_once()
            if not claimed:
                waiter = getattr(self.repository, "wait_for_change", None)
                if waiter is not None:
                    await waiter(poll_interval)
                else:
                    await asyncio.sleep(poll_interval)

    def stop(self) -> None:
        self._stop.set()

    async def drain(self, *, timeout: float = 60.0) -> bool:
        """Stop claiming work and wait for the active delivery to finish."""

        self.stop()
        try:
            await asyncio.wait_for(self._idle.wait(), timeout=timeout)
            return True
        except TimeoutError:
            return False

    async def _execute(self, run: Run) -> None:
        token = CancellationToken()
        heartbeat = asyncio.create_task(self._heartbeat(run, token))
        output: dict[str, Any] | None = None
        try:
            graph = self.registry.get(run.graph_id, run.graph_version).with_runtime(
                checkpointer=self.checkpointer,
                store=self.store_factory(run.tenant_id),
                cache=self.cache,
            )
            config = {
                **run.config,
                "configurable": {
                    **dict(run.config.get("configurable", {})),
                    "tenant_id": run.tenant_id,
                    "thread_id": run.thread_id or f"stateless:{run.id}",
                },
            }
            graph_input: Any = run.input
            if run.resume is not None or run.update is not None or run.goto is not None:
                graph_input = Command(
                    resume=run.resume,
                    update=run.update,
                    goto=run.goto,
                )
            paused = False
            async for event in graph.astream(
                graph_input,
                config,
                context=run.context,
                durability=run.durability,
                stream_mode="events",
                run_id=run.id,
                cancellation=token,
            ):
                if event.kind is EventKind.RUN_COMPLETED:
                    output = dict(event.data.get("state", {}))
                if event.kind is EventKind.INTERRUPT_RAISED:
                    paused = True
                await self._append_event(run, event)
            if paused:
                snapshot = graph.get_state(config)
                output = {
                    **dict(snapshot.values),
                    "__interrupt__": [dataclasses.asdict(item) for item in snapshot.interrupts],
                }
                await self.repository.finish_run(
                    run.tenant_id, run.id, RunStatus.PAUSED, output=output
                )
            else:
                await self.repository.finish_run(
                    run.tenant_id, run.id, RunStatus.SUCCEEDED, output=output or {}
                )
                logger.info(
                    "run succeeded",
                    extra={"run_id": run.id, "tenant_id": run.tenant_id, "status": "succeeded"},
                )
        except GraphCancelledError as exc:
            await self.repository.finish_run(
                run.tenant_id,
                run.id,
                RunStatus.CANCELLED,
                error={"code": "run_cancelled", "message": str(exc)},
            )
        except GraphTimeoutError as exc:
            await self.repository.finish_run(
                run.tenant_id,
                run.id,
                RunStatus.TIMED_OUT,
                error={"code": "run_timed_out", "message": str(exc)},
            )
        except BudgetExceededError as exc:
            await self.repository.finish_run(
                run.tenant_id,
                run.id,
                RunStatus.FAILED,
                error={"code": "budget_exceeded", "message": str(exc)},
            )
        except Exception as exc:
            error = {"code": "run_failed", "message": str(exc), "type": type(exc).__name__}
            retryable = self._is_retryable(exc)
            if retryable and run.attempt < self.max_delivery_attempts:
                error["code"] = "delivery_retry"
                await self.repository.retry_run(run.tenant_id, run.id, error=error)
                stored = await self.repository.append_event(
                    run.tenant_id,
                    run.id,
                    "worker_retrying",
                    {
                        "attempt": run.attempt,
                        "max_attempts": self.max_delivery_attempts,
                        "error": error,
                    },
                )
                await self.event_bus.publish(run.tenant_id, run.id, stored.sequence)
                logger.warning(
                    "run delivery scheduled for retry",
                    extra={
                        "run_id": run.id,
                        "tenant_id": run.tenant_id,
                        "status": "pending",
                        "error_type": type(exc).__name__,
                    },
                )
            else:
                status = RunStatus.DEAD_LETTER if retryable else RunStatus.FAILED
                error["code"] = "dead_letter" if retryable else "run_failed"
                await self.repository.finish_run(
                    run.tenant_id,
                    run.id,
                    status,
                    error=error,
                )
                logger.error(
                    "run delivery failed",
                    extra={
                        "run_id": run.id,
                        "tenant_id": run.tenant_id,
                        "status": status.value,
                        "error_type": type(exc).__name__,
                    },
                )
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)

    async def _heartbeat(self, run: Run, token: CancellationToken) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_seconds)
            if await self.repository.is_cancel_requested(run.tenant_id, run.id):
                token.cancel()
            alive = await self.repository.heartbeat(
                run.tenant_id,
                run.id,
                self.worker_id,
                lease_seconds=self.lease_seconds,
            )
            if not alive:
                token.cancel()
                return

    async def _append_event(self, run: Run, event: Event) -> None:
        encoded = self._serializer.dumps(dataclasses.asdict(event))
        if len(encoded) > self.repository.limits.max_event_bytes:
            raise PersistenceError(
                f"event size {len(encoded)} exceeds max_event_bytes="
                f"{self.repository.limits.max_event_bytes}"
            )
        data = self._serializer.loads(encoded)
        stored = await self.repository.append_event(
            run.tenant_id,
            run.id,
            event.kind.value,
            data,
        )
        await self.event_bus.publish(run.tenant_id, run.id, stored.sequence)

    @staticmethod
    def _is_retryable(exc: Exception) -> bool:
        if isinstance(exc, (GraphValidationError, InvalidUpdateError, KeyError, ValueError)):
            return False
        if isinstance(exc, (ConnectionError, TimeoutError, PersistenceError)):
            return True
        if isinstance(exc, RuntimeError):
            return True
        module = type(exc).__module__
        name = type(exc).__name__.lower()
        return module.startswith("httpx") or any(
            marker in name for marker in ("timeout", "network", "connection", "temporary")
        )


__all__ = ["Worker"]
