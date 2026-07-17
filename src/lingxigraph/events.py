"""Transport-neutral lifecycle events emitted by graph runs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any
from uuid import uuid4

from .types import _utc_now


class EventKind(str, Enum):
    RUN_STARTED = "run_started"
    RUN_PAUSED = "run_paused"
    NODE_STARTED = "node_started"
    NODE_COMPLETED = "node_completed"
    NODE_FAILED = "node_failed"
    NODE_RETRYING = "node_retrying"
    NODE_CACHED = "node_cached"
    STEP_STARTED = "step_started"
    STEP_COMPLETED = "step_completed"
    STATE_UPDATED = "state_updated"
    CHECKPOINT_SAVED = "checkpoint_saved"
    INTERRUPT_RAISED = "interrupt_raised"
    CUSTOM = "custom"
    MESSAGE = "message"
    RUN_COMPLETED = "run_completed"
    RUN_FAILED = "run_failed"
    RUN_CANCELLED = "run_cancelled"
    RUN_TIMED_OUT = "run_timed_out"
    RUN_BUDGET_EXCEEDED = "run_budget_exceeded"


@dataclass(frozen=True, slots=True)
class Event:
    kind: EventKind
    run_id: str
    step: int | None = None
    node: str | None = None
    data: Mapping[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=_utc_now)
    version: str = "1"
    event_id: str = field(default_factory=lambda: str(uuid4()))
    sequence: int = 0
    namespace: tuple[str, ...] = ()
    task_id: str | None = None
    checkpoint_id: str | None = None
    graph_id: str | None = None
    assistant_id: str | None = None
    thread_id: str | None = None
    trace_id: str | None = None
    span_id: str | None = None


__all__ = ["Event", "EventKind"]
