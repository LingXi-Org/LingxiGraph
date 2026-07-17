"""Public value types and dynamic interrupt support."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Generic, TypeVar

from .errors import GraphInterrupt

T = TypeVar("T")
StreamWriter = Callable[[Any], None]


class CommandScope(str, Enum):
    """Where a dynamic command should be resolved."""

    SELF = "self"
    PARENT = "parent"


class Durability(str, Enum):
    """Checkpoint durability requested by a graph invocation."""

    SYNC = "sync"
    ASYNC = "async"
    EXIT = "exit"


class RunStatus(str, Enum):
    """Stable lifecycle states exposed by Agent Server."""

    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"
    DEAD_LETTER = "dead_letter"


class MultitaskStrategy(str, Enum):
    """How a new run behaves when its thread already has an active run."""

    ENQUEUE = "enqueue"
    REJECT = "reject"
    CANCEL_PREVIOUS = "cancel_previous"


class SubgraphPersistence(str, Enum):
    """Lifetime of a subgraph checkpoint namespace."""

    INVOCATION = "invocation"
    THREAD = "thread"
    STATELESS = "stateless"


@dataclass(frozen=True, slots=True)
class Send:
    """Schedule one task for ``node`` in the next superstep with a private input.

    Unlike normal edges, a ``Send`` does not hand the node the shared state; it
    hands it ``arg``.  Returning several ``Send`` objects from a conditional
    edge or ``Command.goto`` fans work out map-reduce style: each task runs in
    parallel and their updates are merged back through the state reducers.
    """

    node: str
    arg: Any


@dataclass(frozen=True, slots=True)
class Command(Generic[T]):
    """A node update, explicit jump, or invocation resume value."""

    update: Mapping[str, Any] | None = None
    goto: str | Send | Sequence[str | Send] | None = None
    resume: T | None = None
    scope: CommandScope = CommandScope.SELF


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Node-level retry configuration applied around each task attempt.

    ``retry_on`` filters which exceptions trigger a retry; interrupts and
    other control-flow signals are never retried.  The delay before attempt
    ``n`` is ``initial_interval * backoff_factor ** (n - 1)`` capped at
    ``max_interval``, with up to 50% random jitter when ``jitter`` is set.
    """

    max_attempts: int = 3
    initial_interval: float = 0.5
    backoff_factor: float = 2.0
    max_interval: float = 30.0
    jitter: bool = True
    retry_on: type[Exception] | tuple[type[Exception], ...] = Exception

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.initial_interval < 0 or self.max_interval < 0:
            raise ValueError("retry intervals must be non-negative")
        if self.backoff_factor < 1:
            raise ValueError("backoff_factor must be at least 1")


@dataclass(frozen=True, slots=True)
class CachePolicy:
    """Node-result cache configuration.

    ``key_fields`` limits the state fields used to form a cache key.  When it
    is empty the complete node input is hashed.  Runtime control-flow values
    (commands and interrupts) are never cached.
    """

    ttl: float | None = None
    key_fields: tuple[str, ...] = ()
    namespace: str = "nodes"

    def __post_init__(self) -> None:
        if self.ttl is not None and self.ttl <= 0:
            raise ValueError("cache ttl must be positive")


@dataclass(frozen=True, slots=True)
class Interrupt:
    """A durable request for external input."""

    value: Any
    resumable: bool = True
    id: str | None = None
    when: str = "during"
    task_id: str | None = None
    namespace: tuple[str, ...] = ()
    task_path: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class TaskSnapshot:
    """Persisted user-facing task state for one scheduled node execution."""

    id: str
    name: str
    path: tuple[str, ...] = ()
    error: str | None = None
    interrupts: tuple[Interrupt, ...] = ()
    result: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class StateSnapshot:
    """A user-facing view of the latest persisted graph state."""

    values: Mapping[str, Any]
    next: tuple[str, ...]
    config: Mapping[str, Any]
    metadata: Mapping[str, Any] = field(default_factory=dict)
    created_at: str | None = None
    interrupts: tuple[Interrupt, ...] = ()
    tasks: tuple[TaskSnapshot, ...] = ()
    parent_config: Mapping[str, Any] | None = None


RunConfig = Mapping[str, Any]


@dataclass(slots=True)
class _InterruptContext:
    resumable: bool
    resume_values: tuple[Any, ...] = ()
    call_index: int = 0
    task_id: str = ""
    namespace: tuple[str, ...] = ()
    task_path: tuple[str, ...] = ()


_interrupt_context: ContextVar[_InterruptContext | None] = ContextVar(
    "lingxigraph_interrupt_context", default=None
)


def interrupt(value: Any) -> Any:
    """Pause a node and return the matching resume value on re-execution."""

    context = _interrupt_context.get()
    if context is None:
        raise RuntimeError("interrupt() must be called while a graph node is executing")
    if not context.resumable:
        raise RuntimeError("interrupt() requires a checkpointer and a configurable thread_id")
    index = context.call_index
    context.call_index += 1
    if index < len(context.resume_values):
        return context.resume_values[index]
    marker = Interrupt(
        value=value,
        id=f"{context.task_id}:{index}",
        when="during",
        task_id=context.task_id,
        namespace=context.namespace,
        task_path=context.task_path,
    )
    raise GraphInterrupt(marker)


def _get_interrupt_context() -> _InterruptContext | None:
    return _interrupt_context.get()


def _set_interrupt_context(context: _InterruptContext) -> Token[_InterruptContext | None]:
    return _interrupt_context.set(context)


def _reset_interrupt_context(token: Token[_InterruptContext | None]) -> None:
    _interrupt_context.reset(token)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


__all__ = [
    "CachePolicy",
    "Command",
    "CommandScope",
    "Durability",
    "Interrupt",
    "MultitaskStrategy",
    "RetryPolicy",
    "RunConfig",
    "RunStatus",
    "Send",
    "StateSnapshot",
    "StreamWriter",
    "SubgraphPersistence",
    "TaskSnapshot",
    "interrupt",
]
