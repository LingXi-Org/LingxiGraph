"""Ephemeral run signaling; durable events remain in the repository."""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from typing import Protocol


class EventBus(Protocol):
    async def publish(self, tenant_id: str, run_id: str, sequence: int) -> None: ...

    async def wait(
        self, tenant_id: str, run_id: str, *, timeout: float = 15.0
    ) -> None: ...


class InMemoryEventBus:
    """In-process wake signal used by the heartbeat's low-latency fast path.

    Backed by :class:`asyncio.Event`, not :class:`asyncio.Condition`. This
    is a deliberate choice, not a style preference: ``Condition.wait()``
    must re-acquire its lock before it can propagate cancellation, and a
    ``notify_all()`` racing a concurrent ``cancel()`` on the *same*
    condition can leave that re-acquire permanently stuck (a long-standing
    CPython gotcha -- see https://github.com/python/cpython/issues/86296).
    ``Worker._heartbeat`` is cancelled from ``Worker._execute`` right after
    the run finishes, at exactly the moment ``_append_event`` is also
    calling ``publish()`` for the run's own completion/interrupt events on
    this same bus -- i.e. exactly the racing pattern the bug needs, and it
    was reproducible here as a real hang once the finalization path in
    ``_execute`` was restructured (issue #16 PR #17 review round 5) to
    settle the heartbeat cancellation a little earlier relative to that
    publish. ``asyncio.Event`` has no such lock-reacquire step, so
    cancelling a waiter is always immediate and safe.
    """

    def __init__(self) -> None:
        self._events: dict[tuple[str, str], asyncio.Event] = defaultdict(asyncio.Event)

    async def publish(self, tenant_id: str, run_id: str, sequence: int) -> None:
        del sequence
        self._events[(tenant_id, run_id)].set()

    async def wait(self, tenant_id: str, run_id: str, *, timeout: float = 15.0) -> None:
        event = self._events[(tenant_id, run_id)]
        # Deliberately *not* ``asyncio.wait_for(event.wait(), timeout)``:
        # on Python's asyncio (still true on the 3.11 runtime this project
        # targets), ``wait_for`` wraps the awaitable in its own inner Task
        # and, if the *outer* coroutine awaiting ``wait_for`` is itself
        # cancelled (exactly what happens here -- ``Worker._heartbeat`` is
        # cancelled by ``Worker._execute`` right as this call is
        # in-flight), there is a real, reproducible race where that inner
        # Task's cancellation is never actually awaited to completion,
        # hanging forever (https://github.com/python/cpython/issue/86296,
        # fixed by ``asyncio.timeout()`` in 3.12). ``asyncio.wait`` with an
        # explicit sibling timeout task does not share that failure mode:
        # an external cancellation of *this* coroutine while it is
        # suspended inside ``asyncio.wait`` propagates immediately and
        # unconditionally, the same as any other plain ``await``.
        waiter = asyncio.ensure_future(event.wait())
        timer = asyncio.ensure_future(asyncio.sleep(timeout))
        try:
            await asyncio.wait({waiter, timer}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for pending in (waiter, timer):
                if not pending.done():
                    pending.cancel()
            await asyncio.gather(waiter, timer, return_exceptions=True)
            # Reset for the next waiter. A publish() landing in the
            # instant between this waiter waking and the clear() below is
            # simply an extra, harmless early wake for whoever waits next
            # -- this bus is purely a latency optimization on top of the
            # unconditional PostgreSQL poll each heartbeat tick also does,
            # so an occasional spurious or missed wake here never loses a
            # steering event or a cancellation signal.
            event.clear()


class RedisEventBus:
    """Redis pub/sub signaling with polling-compatible failure behavior."""

    def __init__(self, url: str, *, prefix: str = "lingxigraph:runs") -> None:
        try:
            from redis.asyncio import Redis
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("install lingxigraph[redis] to use RedisEventBus") from exc
        self._redis = Redis.from_url(url)
        self._prefix = prefix

    def _channel(self, tenant_id: str, run_id: str) -> str:
        return f"{self._prefix}:{tenant_id}:{run_id}"

    async def publish(self, tenant_id: str, run_id: str, sequence: int) -> None:
        try:
            await self._redis.publish(
                self._channel(tenant_id, run_id),
                json.dumps({"sequence": sequence}),
            )
        except Exception:
            return

    async def wait(self, tenant_id: str, run_id: str, *, timeout: float = 15.0) -> None:
        pubsub = self._redis.pubsub()
        try:
            await pubsub.subscribe(self._channel(tenant_id, run_id))
            await asyncio.wait_for(pubsub.get_message(ignore_subscribe_messages=True), timeout)
        except Exception:
            return
        finally:
            try:
                await pubsub.aclose()
            except Exception:
                pass

    async def close(self) -> None:
        await self._redis.aclose()


__all__ = ["EventBus", "InMemoryEventBus", "RedisEventBus"]
