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
from ..steering import SteeringEvent
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
        # ``with_runtime`` returns a distinct bound graph instance -- build
        # it first so the heartbeat loop shares the exact same steering
        # channel the executor actually reads from, not a different
        # instance's.
        graph = self.registry.get(run.graph_id, run.graph_version).with_runtime(
            checkpointer=self.checkpointer,
            store=self.store_factory(run.tenant_id),
            cache=self.cache,
        )
        # Register the steering channel *before* the graph ever executes a
        # single task, even when there is nothing pending yet. This marks
        # it server-owned (``owned_by_executor=False``, see
        # SteeringChannel/CompiledStateGraph._run) so the executor's own
        # embedded-run cleanup never releases it out from under us --
        # otherwise a run claimed with no steering pending yet would get an
        # executor-owned channel by default, and steering that arrived
        # later plus the worker's final flush could race the executor
        # popping the channel the moment the run finishes.
        graph.get_steering_channel(run.id)
        # Recover any steering events that were durably accepted while this
        # run was queued (or before a prior worker crashed) -- the channel
        # is the same live object the executor reads from at every safe
        # point, so this is the "recover after restart" and "queued run"
        # cases from issue #16 in one code path.
        await self._sync_steering_in(run, graph)
        heartbeat = asyncio.create_task(self._heartbeat(run, token, graph))
        output: dict[str, Any] | None = None

        # Issue #16 PR #17 review round 5, point 1: the *intended* outcome
        # is computed here, purely in memory -- it is deliberately NOT
        # written to the repository yet. Writing ``finish_run``/
        # ``retry_run`` here (as round <=4 did) makes a terminal or paused
        # status externally observable via GET/join/SSE *before* we know
        # whether the run's final steering consumptions actually made it
        # to durable storage below. If that flush then failed, overriding
        # the already-visible status back to pending/retrying breaks the
        # single most important Run state-machine invariant: once a
        # terminal/paused status is externally observed, the run must
        # never appear non-terminal again. So instead we settle on
        # ``_Intent`` here, stop the heartbeat, durably flush steering, and
        # only then -- once -- commit the outcome to the repository.
        intent: Worker._Intent
        try:
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
                intent = Worker._Intent(kind="finish", status=RunStatus.PAUSED, output=output)
            else:
                intent = Worker._Intent(
                    kind="finish", status=RunStatus.SUCCEEDED, output=output or {}
                )
        except GraphCancelledError as exc:
            intent = Worker._Intent(
                kind="finish",
                status=RunStatus.CANCELLED,
                error={"code": "run_cancelled", "message": str(exc)},
            )
        except GraphTimeoutError as exc:
            intent = Worker._Intent(
                kind="finish",
                status=RunStatus.TIMED_OUT,
                error={"code": "run_timed_out", "message": str(exc)},
            )
        except BudgetExceededError as exc:
            intent = Worker._Intent(
                kind="finish",
                status=RunStatus.FAILED,
                error={"code": "budget_exceeded", "message": str(exc)},
            )
        except Exception as exc:
            error = {"code": "run_failed", "message": str(exc), "type": type(exc).__name__}
            retryable = self._is_retryable(exc)
            if retryable and run.attempt < self.max_delivery_attempts:
                error["code"] = "delivery_retry"
                intent = Worker._Intent(kind="retry", status=RunStatus.PENDING, error=error)
            else:
                status = RunStatus.DEAD_LETTER if retryable else RunStatus.FAILED
                error["code"] = "dead_letter" if retryable else "run_failed"
                intent = Worker._Intent(kind="finish", status=status, error=error)
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)

        # Issue #16 PR #17 review round 6, point 3 (BLOCKER): close new
        # steering admission the instant graph execution ends and *before*
        # the final flush/commit start -- there is no safe point left for
        # the graph to ever consume a steering event accepted after this.
        # This is fenced on our own (lease_owner, attempt): if we no
        # longer hold the lease (e.g. it already expired and a new worker
        # claimed the run), this fails and we must abandon the attempt
        # without touching the run's status at all -- the new owner is now
        # solely responsible for it (review round 6, point 1, BLOCKER).
        #
        # A ``paused`` outcome is deliberately excluded: pausing does not
        # mean "no safe point left" the way a terminal outcome does --
        # ``/steer`` against a paused run is an intentional, documented
        # part of issue #16's design (the pending steering is transferred
        # to the resumed run at ``/resume`` time), so it must keep
        # accepting new steering input, not be permanently gated shut by
        # a worker that is merely yielding control, not finishing.
        closing_steering = not (
            intent.kind == "finish" and intent.status is RunStatus.PAUSED
        )
        if closing_steering:
            gate_closed = await self.repository.close_steering(
                run.tenant_id, run.id, self.worker_id, run.attempt
            )
            if not gate_closed:
                logger.warning(
                    "run finalization abandoned: lease no longer owned before steering close",
                    extra={"run_id": run.id, "tenant_id": run.tenant_id},
                )
                return

        # Issue #16 PR #17 review round 6, point 1 (BLOCKER): the lease
        # lifecycle must stay alive through the *entire* finalization
        # window, independent of the now-cancelled steering/event
        # heartbeat -- the final flush can legitimately retry for a long
        # time, and without this the lease can simply expire mid-flush,
        # letting a new worker claim and even finish the run while we are
        # still blocked, unaware our writes are no longer safe.
        renewal = asyncio.create_task(self._lease_renewal_loop(run))
        try:
            # Final flush: anything drained between the last heartbeat tick
            # and run completion must still be recorded as consumed,
            # durably, before the run's outcome above is committed. See
            # ``_final_steering_flush`` for why this retries for as long as
            # it holds the lease rather than giving up after a handful of
            # attempts (issue #16 PR #17 review round 5, point 2).
            flushed = await self._final_steering_flush(run, graph)
            if flushed:
                committed = await self._commit_intent(run, intent)
                if committed:
                    graph.forget_steering(run.id)
                return

            # The flush never durably succeeded (the worker is draining, or
            # this run's lease was lost to another worker) -- the intended
            # outcome computed above must NEVER be written; doing so would
            # either expose a false terminal/paused status or silently
            # strand the drained-but-uncommitted steering consumptions.
            # Route the run into the same retry/dead-letter path an
            # ordinary delivery failure uses, so a later delivery attempt
            # resyncs and retries the commit from the still-``pending`` DB
            # row. Every write below is fenced on (lease_owner, attempt) --
            # see ``finish_run_if_owned``/``retry_run_if_owned`` -- so a
            # worker that has lost its lease mid-flush can never revert a
            # new owner's run back to pending or overwrite its status
            # (review round 6, point 1, BLOCKER).
            flush_error = {
                "code": "steering_flush_failed",
                "message": (
                    "durable steering consumption commit could not complete before this "
                    "worker gave up its lease; run finalization was withheld to avoid "
                    "reporting a false status or losing the consumption"
                ),
            }
            if run.attempt < self.max_delivery_attempts:
                updated = await self.repository.retry_run_if_owned(
                    run.tenant_id, run.id, self.worker_id, run.attempt, error=flush_error
                )
                if updated is None:
                    logger.warning(
                        "run finalization abandoned: lease lost during flush-failed retry",
                        extra={"run_id": run.id, "tenant_id": run.tenant_id},
                    )
                    return
                stored = await self.repository.append_event(
                    run.tenant_id,
                    run.id,
                    "worker_retrying",
                    {
                        "attempt": run.attempt,
                        "max_attempts": self.max_delivery_attempts,
                        "error": flush_error,
                    },
                )
                await self.event_bus.publish(run.tenant_id, run.id, stored.sequence)
                logger.warning(
                    "run finalization retried: final steering flush failed",
                    extra={"run_id": run.id, "tenant_id": run.tenant_id, "status": "pending"},
                )
            else:
                updated = await self.repository.finish_run_if_owned(
                    run.tenant_id,
                    run.id,
                    self.worker_id,
                    run.attempt,
                    RunStatus.DEAD_LETTER,
                    error=flush_error,
                )
                if updated is None:
                    logger.warning(
                        "run finalization abandoned: lease lost before dead-lettering",
                        extra={"run_id": run.id, "tenant_id": run.tenant_id},
                    )
                    return
                logger.error(
                    "run dead-lettered: final steering flush failed after max attempts",
                    extra={
                        "run_id": run.id,
                        "tenant_id": run.tenant_id,
                        "status": updated.status
                        if isinstance(updated.status, str)
                        else updated.status.value,
                    },
                )
            # Do NOT forget_steering here. Not because this channel's local
            # consumption log will be reused later -- ``with_runtime()``
            # builds a brand new graph/channel on every delivery attempt
            # (issue #16 PR #17 review round 5, point 2), so this exact
            # object is discarded regardless. Recovery is driven entirely
            # by the still-``pending`` DB row a future attempt re-reads via
            # ``list_pending_steering``, not by anything held in this
            # channel across the attempt boundary -- forgetting here is
            # simply unnecessary, not unsafe.
        finally:
            renewal.cancel()
            await asyncio.gather(renewal, return_exceptions=True)

    @dataclasses.dataclass
    class _Intent:
        """The finalization outcome ``_execute`` intends to commit.

        Held purely in memory until the final steering flush durably
        succeeds -- see the long comment in ``_execute`` for why.
        """

        kind: str  # "finish" or "retry"
        status: RunStatus
        output: dict[str, Any] | None = None
        error: dict[str, Any] | None = None

    async def _commit_intent(self, run: Run, intent: "Worker._Intent") -> bool:
        """Write the previously-computed intended outcome, exactly once.

        Only ever called after the final steering flush has durably
        succeeded, so this is the single point where a terminal/paused
        status first becomes externally observable.

        Issue #16 PR #17 review round 6, points 1 and 2 (BLOCKER): this
        write is fenced on our own ``(lease_owner, attempt)`` via
        ``finish_run_if_owned``/``retry_run_if_owned`` -- if we no longer
        hold the lease (a new worker claimed the run while our flush was
        still in flight), the write is refused entirely and this returns
        ``False`` so the caller does not call ``forget_steering()`` on a
        channel whose consumption record may still matter. The fenced
        write also re-checks the run's current control state atomically:
        if it is ``cancelling``, cancellation wins over a stale
        ``succeeded``/``failed`` intent regardless of arrival order.
        """

        if intent.kind == "retry":
            updated = await self.repository.retry_run_if_owned(
                run.tenant_id, run.id, self.worker_id, run.attempt, error=intent.error
            )
            if updated is None:
                logger.warning(
                    "run retry commit abandoned: lease no longer owned",
                    extra={"run_id": run.id, "tenant_id": run.tenant_id},
                )
                return False
            stored = await self.repository.append_event(
                run.tenant_id,
                run.id,
                "worker_retrying",
                {
                    "attempt": run.attempt,
                    "max_attempts": self.max_delivery_attempts,
                    "error": intent.error,
                },
            )
            await self.event_bus.publish(run.tenant_id, run.id, stored.sequence)
            logger.warning(
                "run delivery scheduled for retry",
                extra={"run_id": run.id, "tenant_id": run.tenant_id, "status": "pending"},
            )
            return True

        updated = await self.repository.finish_run_if_owned(
            run.tenant_id,
            run.id,
            self.worker_id,
            run.attempt,
            intent.status,
            output=intent.output,
            error=intent.error,
        )
        if updated is None:
            logger.warning(
                "run finalize commit abandoned: lease no longer owned",
                extra={"run_id": run.id, "tenant_id": run.tenant_id},
            )
            return False
        final_status = updated.status if isinstance(updated.status, str) else updated.status.value
        if final_status == RunStatus.SUCCEEDED.value:
            logger.info(
                "run succeeded",
                extra={"run_id": run.id, "tenant_id": run.tenant_id, "status": "succeeded"},
            )
        elif final_status == RunStatus.CANCELLED.value and intent.status != RunStatus.CANCELLED:
            logger.info(
                "run cancelled during finalization, overriding stale %s intent",
                intent.status.value if hasattr(intent.status, "value") else intent.status,
                extra={"run_id": run.id, "tenant_id": run.tenant_id, "status": "cancelled"},
            )
        elif final_status in (RunStatus.DEAD_LETTER.value, RunStatus.FAILED.value) and intent.error:
            logger.error(
                "run delivery failed",
                extra={
                    "run_id": run.id,
                    "tenant_id": run.tenant_id,
                    "status": final_status,
                },
            )
        return True

    async def _lease_renewal_loop(self, run: Run) -> None:
        """Keep the run's lease alive through the whole finalization window.

        Issue #16 PR #17 review round 6, point 1 (BLOCKER): coupling lease
        renewal solely to the ordinary steering/event heartbeat -- which
        ``_execute`` cancels the moment the graph finishes -- lets the
        lease simply expire while the final flush is still retrying,
        letting a new worker claim (and even finish) the run out from
        under us. This loop renews the lease on its own schedule for as
        long as it runs; ``_execute`` cancels it once finalization (flush
        + fenced commit) is fully done, one way or another.
        """

        try:
            while True:
                await asyncio.sleep(self.heartbeat_seconds)
                alive = await self.repository.heartbeat(
                    run.tenant_id, run.id, self.worker_id, lease_seconds=self.lease_seconds
                )
                if not alive:
                    return
        except asyncio.CancelledError:
            raise

    async def _final_steering_flush(
        self, run: Run, graph: Any, *, base_delay: float = 0.5, max_delay: float = 10.0
    ) -> bool:
        """Durable flush of drained steering before finalizing.

        Unlike :meth:`_sync_steering_out` (used at heartbeat ticks, where a
        transient failure can simply be retried at the next tick), this is
        called once the graph has already reached its terminal/paused
        outcome and the heartbeat loop has stopped -- there is no later
        safe point *within this delivery attempt*. Critically, there is
        also no safe "next delivery attempt" to fall back on the way an
        ordinary mid-run failure has: ``_execute`` returning discards this
        locally-bound ``graph`` (and therefore its steering channel and
        local consumption log) entirely, because ``with_runtime`` builds a
        brand new ``CompiledStateGraph`` -- with a brand new, empty
        ``_run_steering`` map -- on every call (issue #16 PR #17 review
        round 5, point 2). A future redelivery would construct a fresh
        channel with no memory of what this attempt already drained,
        risking double-execution or a permanently stuck ``pending`` row.

        So instead of a small bounded number of attempts, this retries
        with capped exponential backoff for as long as it continues to
        hold the run's lease and the worker is not draining, renewing the
        lease between attempts so another worker does not reclaim the run
        (and construct its own competing channel) out from under it mid
        retry. It only gives up -- returning ``False`` -- if the worker
        itself is shutting down, or the lease is confirmed lost to another
        worker; both are cases where holding on any longer would be
        pointless or actively harmful, and the caller falls back to the
        ordinary retry/dead-letter path.
        """

        channel = graph.get_steering_channel(run.id)
        attempt = 0
        # Iterative capped growth rather than ``base_delay * 2 ** (attempt
        # - 1)``: the latter computes a potentially enormous exponent
        # before ``min()`` ever clamps it, which is wasted (and, given an
        # unbounded retry count against a very long DB outage,
        # eventually pathological) work. Doubling and clamping in place
        # reaches the same steady-state ``max_delay`` without ever
        # computing an intermediate value larger than that cap.
        delay = base_delay
        while True:
            consumed = channel.peek_consumed()
            if not consumed:
                return True
            attempt += 1
            try:
                stored_events = await self.repository.commit_steering_consumptions(
                    run.tenant_id, run.id, consumed
                )
            except Exception:
                logger.warning(
                    "final steering flush attempt %d failed",
                    attempt,
                    extra={"run_id": run.id, "tenant_id": run.tenant_id},
                    exc_info=True,
                )
                if self._stop.is_set():
                    return False
                await asyncio.sleep(delay)
                delay = min(max_delay, delay * 2)
                # Keep the lease alive while we keep retrying so another
                # worker does not reclaim this run mid-flush and construct
                # a competing graph/channel of its own.
                alive = await self.repository.heartbeat(
                    run.tenant_id, run.id, self.worker_id, lease_seconds=self.lease_seconds
                )
                if not alive:
                    return False
                continue
            channel.ack_consumed(consumption.event.id for consumption in consumed)
            for stored in stored_events:
                await self.event_bus.publish(run.tenant_id, run.id, stored.sequence)
            return True

    async def _heartbeat(self, run: Run, token: CancellationToken, graph: Any) -> None:
        while True:
            # Issue #16 PR #17 review round 4, point 5: wait on the
            # EventBus (Redis, when configured) instead of a plain
            # ``asyncio.sleep`` so a ``/steer`` call's ``publish()`` can
            # wake this loop early -- low-latency steering discovery --
            # while ``timeout=self.heartbeat_seconds`` still bounds the
            # wait, so this remains a correctness-preserving fallback to
            # the unconditional PostgreSQL poll below even if the publish
            # is dropped, Redis is unavailable, or no EventBus wake ever
            # arrives at all (``InMemoryEventBus``/``RedisEventBus.wait``
            # both return on timeout rather than raising).
            await self.event_bus.wait(run.tenant_id, run.id, timeout=self.heartbeat_seconds)
            # Guarantee a real scheduler yield every iteration regardless of
            # how quickly a particular ``EventBus.wait`` implementation
            # returns (a degenerate/no-op bus could otherwise return without
            # ever suspending, starving the run's own coroutine).
            await asyncio.sleep(0)
            if await self.repository.is_cancel_requested(run.tenant_id, run.id):
                # Cancellation always takes priority; steering must never
                # undo or delay it. We still sync steering below so an
                # already-accepted event is not lost, but we do not let a
                # steer block or reorder the cancel signal.
                token.cancel()
            # PostgreSQL is the source of truth for steering; this poll
            # keeps working even if a Redis ``run.steer.available`` notify
            # was dropped or Redis is unavailable entirely.
            await self._sync_steering_in(run, graph)
            await self._sync_steering_out(run, graph)
            alive = await self.repository.heartbeat(
                run.tenant_id,
                run.id,
                self.worker_id,
                lease_seconds=self.lease_seconds,
            )
            if not alive:
                token.cancel()
                return

    async def _sync_steering_in(self, run: Run, graph: Any) -> None:
        """Pull durably-accepted-but-not-yet-delivered events into the graph."""

        pending = await self.repository.list_pending_steering(run.tenant_id, run.id)
        if not pending:
            return
        channel = graph.get_steering_channel(run.id)
        for row in pending:
            channel.ingest(
                SteeringEvent(
                    id=row.id,
                    run_id=run.id,
                    sequence=row.sequence,
                    kind=row.kind,
                    payload=row.payload,
                    metadata=row.metadata,
                    created_at=row.created_at,
                    source_event_id=row.source_event_id,
                )
            )

    async def _sync_steering_out(self, run: Run, graph: Any) -> None:
        """Flush events the graph has drained back to PostgreSQL + SSE.

        Reads (peeks) the channel's local consumption log, durably commits
        it -- steering status *and* the ``run.steer.consumed`` lifecycle
        event, together, in one repository call -- and only then acks
        those entries out of the local log. If the durable commit fails
        (e.g. a transient DB error), nothing is acked: the entries stay in
        the channel's consumption log and are retried on the next sync
        (next heartbeat tick, or the final flush in ``_execute``). This is
        the issue #16 PR #17 review point 2 fix -- the previous
        pop-then-write ordering could destroy the local record before the
        durable write was known to have succeeded, permanently losing a
        consumption (and its observability event) on a transient failure.
        """

        channel = graph.get_steering_channel(run.id)
        consumed = channel.peek_consumed()
        if not consumed:
            return
        try:
            stored_events = await self.repository.commit_steering_consumptions(
                run.tenant_id, run.id, consumed
            )
        except Exception:
            logger.warning(
                "steering consumption commit failed; will retry at the next safe point",
                extra={"run_id": run.id, "tenant_id": run.tenant_id},
                exc_info=True,
            )
            return
        # Only ack the ids we actually just committed -- a concurrent
        # drain() may have appended more to the log between the peek above
        # and here, and those must be left for the next cycle.
        channel.ack_consumed(consumption.event.id for consumption in consumed)
        for stored in stored_events:
            await self.event_bus.publish(run.tenant_id, run.id, stored.sequence)

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
