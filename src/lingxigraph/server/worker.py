"""Lease-based distributed graph worker."""

from __future__ import annotations

import asyncio
import dataclasses
import socket
from collections.abc import Callable
from typing import Any
from uuid import uuid4

from ..cache import BaseCache
from ..checkpoint import Checkpointer, InMemorySaver
from ..errors import GraphCancelledError, GraphTimeoutError
from ..events import Event, EventKind
from ..runtime import CancellationToken
from ..serialization import JsonSerializer
from ..store import BaseStore, InMemoryStore
from ..types import Command, RunStatus
from .eventbus import EventBus, InMemoryEventBus
from .models import Run
from .registry import GraphRegistry
from .repository import InMemoryRepository


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
        self._serializer = JsonSerializer()

    async def run_once(self) -> bool:
        run = await self.repository.claim_run(
            self.worker_id, lease_seconds=self.lease_seconds
        )
        if run is None:
            return False
        await self._execute(run)
        return True

    async def run_forever(self, *, poll_interval: float = 0.25) -> None:
        while not self._stop.is_set():
            claimed = await self.run_once()
            if not claimed:
                waiter = getattr(self.repository, "wait_for_change", None)
                if waiter is not None:
                    await waiter(poll_interval)
                else:
                    await asyncio.sleep(poll_interval)

    def stop(self) -> None:
        self._stop.set()

    async def _execute(self, run: Run) -> None:
        token = CancellationToken()
        heartbeat = asyncio.create_task(self._heartbeat(run, token))
        output: dict[str, Any] | None = None
        try:
            graph = self.registry.get(run.graph_id).with_runtime(
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
        except Exception as exc:
            code = "dead_letter" if run.attempt >= self.max_delivery_attempts else "run_failed"
            await self.repository.finish_run(
                run.tenant_id,
                run.id,
                RunStatus.FAILED,
                error={"code": code, "message": str(exc), "type": type(exc).__name__},
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
        data = self._serializer.loads(self._serializer.dumps(dataclasses.asdict(event)))
        stored = await self.repository.append_event(
            run.tenant_id,
            run.id,
            event.kind.value,
            data,
        )
        await self.event_bus.publish(run.tenant_id, run.id, stored.sequence)


__all__ = ["Worker"]
