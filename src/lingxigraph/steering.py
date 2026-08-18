"""Durable mid-run steering: provider-neutral data model + in-process channel.

Steering lets an external caller inject new structured input into a Run
while it is executing.  LingxiGraph guarantees durable delivery, ordering,
dedup and safe consumption; it never interprets what a steering event
*means* -- that is entirely up to the application graph (see
``runtime.drain_steering()`` in :mod:`lingxigraph.runtime`).

This module is intentionally dependency-free so it works the same way in
embedded/library usage (no Agent Server, no PostgreSQL) as it does behind
the HTTP API -- the "degraded" implementation for SQLite/in-memory/embedded
runtimes *is* this module, not a separate code path.
"""

from __future__ import annotations

import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

#: Default cap on the serialized size of a steering event payload+metadata.
MAX_STEERING_PAYLOAD_BYTES = 32_768


class SteeringPayloadTooLarge(ValueError):
    """Raised when a steering event payload exceeds the configured size cap."""


@dataclass(frozen=True, slots=True)
class SteeringEvent:
    """A single durable, ordered piece of external input for a Run.

    ``id`` doubles as the idempotency key used for dedup; ``sequence`` is a
    monotonically increasing, per-run ordering position assigned at
    acceptance time.  ``payload``/``metadata`` must already be safe to
    serialize through :mod:`lingxigraph.serialization`.
    """

    id: str
    run_id: str
    sequence: int
    kind: str
    payload: Mapping[str, Any]
    metadata: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))


def validate_steering_payload(
    payload: Mapping[str, Any],
    metadata: Mapping[str, Any] | None = None,
    *,
    max_bytes: int = MAX_STEERING_PAYLOAD_BYTES,
) -> None:
    """Raise :class:`SteeringPayloadTooLarge` if the payload is oversized.

    Also verifies the payload goes through the safe JSON serialization
    boundary (no unsafe/unserializable values).
    """

    from .serialization import JsonSerializer, SerializationError

    serializer = JsonSerializer()
    try:
        encoded = serializer.dumps(dict(payload))
        encoded_metadata = serializer.dumps(dict(metadata or {}))
    except SerializationError as exc:
        raise SteeringPayloadTooLarge(f"steering payload is not safely serializable: {exc}") from exc
    total = len(encoded) + len(encoded_metadata)
    if total > max_bytes:
        raise SteeringPayloadTooLarge(
            f"steering payload size {total} exceeds max_bytes={max_bytes}"
        )


def new_steering_id() -> str:
    return str(uuid4())


@dataclass(frozen=True, slots=True)
class SteeringConsumption:
    """Where and how long an event waited before a safe point drained it.

    Produced by :meth:`SteeringChannel.drain` for every event it returns,
    and surfaced end-to-end as the ``run.steer.consumed`` observability
    event's payload (see :mod:`lingxigraph.server.worker`) so a production
    deployment can answer "where did this get consumed, and how long did it
    queue" -- the two pieces of information issue #16's acceptance criteria
    call out that a bare ``steering_event_id/sequence/kind`` record cannot
    answer on its own.
    """

    event: SteeringEvent
    consumed_at: datetime
    #: The node (task) whose call to ``runtime.drain_steering()`` drained
    #: this event -- ``None`` when drained outside node execution (e.g. a
    #: direct ``channel.drain()`` call with no consumer context attached).
    node: str | None = None
    #: The subgraph namespace path of that node, e.g. ``("supervisor",)``.
    namespace: tuple[str, ...] = ()
    #: The specific task id (distinguishes parallel/``Send`` fan-out tasks
    #: that share one node name within the same superstep).
    task_id: str | None = None

    @property
    def queue_latency_seconds(self) -> float:
        """Wall-clock time between durable acceptance and consumption."""

        return max(0.0, (self.consumed_at - self.event.created_at).total_seconds())


class SteeringChannel:
    """Thread-safe, ordered, dedup'd inbox of :class:`SteeringEvent`.

    This is the concrete "safe-point" primitive used by
    :class:`lingxigraph.runtime.Runtime`. It is deliberately simple:

    * ``submit`` is idempotent by ``event.id`` -- resubmitting the same id
      is a no-op (supports at-least-once delivery from a durable inbox).
    * ``drain`` atomically returns *and clears* all currently pending
      events, in ascending ``sequence`` order. Once drained, an event is
      never re-exposed by this channel again (though upstream at-least-once
      delivery may still resubmit the same id from PostgreSQL after a
      crash -- callers must dedup on ``event.id`` if that matters to them).
    * ``peek`` is the read-only variant: it does not consume anything.

    A ``on_drain`` callback (sync, best-effort) can be supplied so a server
    integration (e.g. the durable-repository-backed worker) can mark events
    ``consumed`` in PostgreSQL when the graph actually drains them.
    """

    def __init__(self, run_id: str = "", *, owned_by_executor: bool = True) -> None:
        self.run_id = run_id
        self._lock = threading.Lock()
        self._pending: dict[str, SteeringEvent] = {}
        self._seen_ids: set[str] = set()
        self._next_sequence = 1
        self._consumed_log: list[SteeringConsumption] = []
        self.on_drain: Any = None
        #: Whether the graph executor's own embedded run lifecycle may
        #: release (``forget``) this channel automatically once its run
        #: finishes. ``True`` for channels created implicitly by
        #: ``CompiledStateGraph.steer()``/``_run()`` (plain embedded
        #: ``invoke``/``ainvoke`` with no server involved). A server
        #: integration explicitly registers a channel ahead of time via
        #: ``get_steering_channel()`` and flips this to ``False`` -- it owns
        #: the channel's lifecycle end to end (see ``Worker._execute``'s
        #: explicit ``forget_steering`` call) since a queued run may receive
        #: steering before any worker has claimed it.
        self.owned_by_executor = owned_by_executor

    def submit(
        self,
        *,
        kind: str,
        payload: Mapping[str, Any],
        metadata: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
        max_bytes: int = MAX_STEERING_PAYLOAD_BYTES,
    ) -> tuple[SteeringEvent, bool]:
        """Append a new event. Returns ``(event, created)``.

        ``created`` is ``False`` when ``idempotency_key`` matches an event
        already known to this channel (pending or already drained).
        """

        validate_steering_payload(payload, metadata, max_bytes=max_bytes)
        event_id = idempotency_key or new_steering_id()
        with self._lock:
            existing = self._pending.get(event_id)
            if existing is not None:
                return existing, False
            if event_id in self._seen_ids:
                # Already drained/consumed previously -- durable dedup.
                return SteeringEvent(
                    id=event_id,
                    run_id=self.run_id,
                    sequence=-1,
                    kind=kind,
                    payload=payload,
                    metadata=metadata or {},
                ), False
            event = SteeringEvent(
                id=event_id,
                run_id=self.run_id,
                sequence=self._next_sequence,
                kind=kind,
                payload=dict(payload),
                metadata=dict(metadata or {}),
            )
            self._next_sequence += 1
            self._pending[event_id] = event
            self._seen_ids.add(event_id)
            return event, True

    def ingest(self, event: SteeringEvent) -> bool:
        """Accept an already-constructed durable event (server integration).

        Preserves the caller-provided sequence number when it is larger
        than anything seen so far, so ordering follows the durable source
        of truth (PostgreSQL) rather than local submission order.
        """

        with self._lock:
            if event.id in self._seen_ids:
                return False
            self._seen_ids.add(event.id)
            self._pending[event.id] = event
            self._next_sequence = max(self._next_sequence, event.sequence + 1)
            return True

    @property
    def has_pending(self) -> bool:
        with self._lock:
            return bool(self._pending)

    def peek(self) -> tuple[SteeringEvent, ...]:
        with self._lock:
            return tuple(sorted(self._pending.values(), key=lambda item: item.sequence))

    def drain(
        self,
        *,
        node: str | None = None,
        namespace: tuple[str, ...] = (),
        task_id: str | None = None,
    ) -> tuple[SteeringEvent, ...]:
        """Atomically consume and return pending events, in order.

        ``node``/``namespace``/``task_id`` identify *where* the drain
        happened -- the executor's safe point (see
        :meth:`lingxigraph.runtime.Runtime.drain_steering`) passes its own
        task coordinates through so the consumption record (see
        :meth:`pop_consumed`) can answer "which node consumed this, in
        which subgraph namespace" for observability. Callers outside node
        execution (e.g. a bare ``channel.drain()``) may omit them.
        """

        now = datetime.now(UTC)
        with self._lock:
            drained = tuple(sorted(self._pending.values(), key=lambda item: item.sequence))
            self._pending.clear()
            self._consumed_log.extend(
                SteeringConsumption(
                    event=event, consumed_at=now, node=node, namespace=namespace, task_id=task_id
                )
                for event in drained
            )
        if drained and self.on_drain is not None:
            try:
                self.on_drain(drained)
            except Exception:
                # Observability/persistence side-effects must never break
                # graph execution -- the DB inbox remains the source of
                # truth and a later safe point can retry marking consumed.
                pass
        return drained

    def pop_consumed(self) -> tuple[SteeringConsumption, ...]:
        """Return and clear the log of events drained since the last call.

        A server-mode worker polls this (e.g. on its heartbeat cadence) to
        mark the corresponding PostgreSQL rows ``consumed`` and emit
        ``run.steer.consumed`` observability events -- consumption is
        recorded as soon as the graph drains an event, independent of
        checkpoint commit timing in this implementation (documented scope
        cut; see issue #16 design notes). Each entry is a
        :class:`SteeringConsumption`, carrying queue latency and the
        consuming node/namespace/task_id alongside the raw event.
        """

        with self._lock:
            consumed = tuple(self._consumed_log)
            self._consumed_log.clear()
            return consumed


__all__ = [
    "MAX_STEERING_PAYLOAD_BYTES",
    "SteeringChannel",
    "SteeringConsumption",
    "SteeringEvent",
    "SteeringPayloadTooLarge",
    "new_steering_id",
    "validate_steering_payload",
]
