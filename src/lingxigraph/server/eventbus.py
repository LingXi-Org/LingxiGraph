"""Ephemeral run signaling; durable events remain in the repository."""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from collections.abc import Awaitable
from typing import Protocol, TypeVar

_T = TypeVar("_T")


async def _wait_cancellation_safe(awaitable: Awaitable[_T], timeout: float) -> _T | None:
    """Await ``awaitable`` with a timeout, safely under external cancellation.

    Issue #16 PR #17 review round 5, point 4 / round 6, point 4: this is
    deliberately *not* ``asyncio.wait_for(awaitable, timeout)``. On
    Python's asyncio (still true on the 3.11 runtime this project
    targets), ``wait_for`` wraps the awaitable in its own inner Task and,
    if the *outer* coroutine awaiting ``wait_for`` is itself cancelled --
    exactly what happens when ``Worker._heartbeat`` is cancelled by
    ``Worker._execute`` right as an ``EventBus.wait()`` call is in-flight
    -- there is a real, reproducible race where that inner Task's
    cancellation is never actually awaited to completion, hanging forever
    (https://github.com/python/cpython/issues/86296, fixed by
    ``asyncio.timeout()`` in 3.12). Racing an explicit sibling timer task
    with ``asyncio.wait`` does not share that failure mode: an external
    cancellation of *this* coroutine while it is suspended inside
    ``asyncio.wait`` propagates immediately and unconditionally, the same
    as any other plain ``await``.

    Both :class:`InMemoryEventBus` and :class:`RedisEventBus` share this
    helper so the fix applies to the production Redis path too, not just
    the in-memory/test bus -- a heartbeat cancelled while blocked in
    ``RedisEventBus.wait()`` must return just as promptly as one blocked
    in ``InMemoryEventBus.wait()``.

    Returns the awaitable's result, or ``None`` on a genuine timeout.

    Issue #16 PR #17 review round 7, point 4 (Redis robustness): this used
    to also swallow any exception the awaitable itself raised into
    ``None``, indistinguishable from a timeout. ``RedisEventBus.wait()``'s
    deadline loop treats a ``None`` result as "no message yet, keep
    looping" -- so if Redis disconnects right after ``subscribe()``
    succeeds and every subsequent ``get_message()`` call raises
    immediately, that used to turn into a tight busy-loop for the rest of
    the heartbeat timeout window (rapidly creating new waiter/timer tasks
    with no backoff), never reaching ``RedisEventBus.wait()``'s own outer
    ``except Exception: return``. Only a real timeout (the timer won the
    race, the waiter never completed) is folded into ``None`` now; an
    exception raised by the awaitable itself propagates to the caller, so
    it reaches that outer handler and returns promptly instead of spinning.
    """

    waiter = asyncio.ensure_future(awaitable)
    timer = asyncio.ensure_future(asyncio.sleep(timeout))
    try:
        await asyncio.wait({waiter, timer}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for pending in (waiter, timer):
            if not pending.done():
                pending.cancel()
        await asyncio.gather(waiter, timer, return_exceptions=True)
    if not waiter.done() or waiter.cancelled():
        # Timed out (or the waiter was cancelled alongside us) -- nothing
        # arrived in time.
        return None
    exc = waiter.exception()
    if exc is not None:
        raise exc
    return waiter.result()


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
        try:
            await _wait_cancellation_safe(event.wait(), timeout)
        finally:
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
        # Issue #16 PR #17 review round 6, point 4 (REQUIRED): this used to
        # be ``asyncio.wait_for(pubsub.get_message(...), timeout)`` -- the
        # exact pattern this same round's ``InMemoryEventBus.wait()`` fix
        # rewrote away from because it can hang forever when the *caller*
        # (``Worker._heartbeat``) is cancelled mid-wait. The Worker
        # cancels a Redis-backed heartbeat exactly the same way as an
        # in-memory one, so the production Redis path needs the same
        # cancellation-safe explicit task/timer pattern, via the bus-
        # agnostic ``_wait_cancellation_safe`` helper both classes share.
        pubsub = self._redis.pubsub()
        try:
            await pubsub.subscribe(self._channel(tenant_id, run_id))
            # Two things had to be fixed here, not just one:
            #
            # 1. ``get_message``'s own ``timeout`` defaults to 0.0 --
            #    "return immediately, blocking for nothing" -- not "block
            #    until a message arrives". Passing it explicitly makes
            #    redis-py perform an actual blocking read instead of a
            #    poll.
            # 2. Even with that fixed, the *first* ``get_message()`` call
            #    right after ``subscribe()`` reliably returns near-
            #    instantly with ``None`` regardless of the timeout given --
            #    it is consuming the buffered SUBSCRIBE acknowledgement
            #    frame that ``ignore_subscribe_messages=True`` then
            #    filters out, not genuinely waiting for a publish.
            #    (Confirmed empirically against a real Redis server: a
            #    second call on the same subscription does block for the
            #    full requested timeout.) A single non-looping call here
            #    would therefore make ``wait()`` resolve immediately on
            #    every tick, in production, independent of whether
            #    anything was ever published -- exactly the bug the round-
            #    6 cancellation regression test caught, for a reason
            #    unrelated to cancellation. Loop against a shared
            #    deadline, re-issuing ``get_message()`` for whatever time
            #    remains, until either a real message arrives (a non-
            #    ``None`` result) or the deadline is reached.
            deadline = asyncio.get_event_loop().time() + timeout
            while True:
                remaining = deadline - asyncio.get_event_loop().time()
                if remaining <= 0:
                    return
                message = await _wait_cancellation_safe(
                    pubsub.get_message(ignore_subscribe_messages=True, timeout=remaining),
                    remaining,
                )
                if message is not None:
                    return
        except asyncio.CancelledError:
            # A bare ``except Exception`` below would not normally catch
            # this (CancelledError is a BaseException on 3.8+), but some
            # libraries' internal read/retry loops (e.g. redis-py's
            # connection handling) can translate an in-flight
            # cancellation into a different exception type while
            # unwinding. Re-raise explicitly and unconditionally so a
            # heartbeat cancellation can never be silently absorbed here
            # regardless of what the client library does internally.
            raise
        except Exception:
            return
        finally:
            try:
                await pubsub.aclose()
            except asyncio.CancelledError:
                raise
            except Exception:
                pass

    async def close(self) -> None:
        await self._redis.aclose()


__all__ = ["EventBus", "InMemoryEventBus", "RedisEventBus"]
