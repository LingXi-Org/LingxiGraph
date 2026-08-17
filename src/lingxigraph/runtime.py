"""Task-scoped runtime context readable from inside executing nodes."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Callable, Mapping
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import UTC, datetime
from threading import Event as ThreadEvent
from threading import Lock
from typing import Any, Generic, TypeVar

from .steering import SteeringChannel, SteeringEvent
from .types import StreamWriter

ContextT = TypeVar("ContextT")
StreamEmitter = Callable[[str, Any], None]


@dataclass(slots=True)
class ExecutionBudget:
    """Concurrency-safe counters shared by every task in one graph run."""

    max_tool_calls: int | None = None
    max_model_calls: int | None = None
    max_tokens: int | None = None
    max_cost: float | None = None
    tool_calls: int = 0
    model_calls: int = 0
    tokens: int = 0
    cost: float = 0.0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    cache_hit_tokens: int = 0
    cache_miss_tokens: int = 0
    cache_write_tokens: int = 0
    cache_requests: int = 0
    cache_metrics_requests: int = 0
    usage_total_tokens: int = 0
    token_savings: int = 0
    estimated_cost: float = 0.0
    estimated_cost_savings: float = 0.0
    _lock: Lock = field(default_factory=Lock, repr=False)

    def consume_tool_call(self, name: str) -> None:
        from .errors import BudgetExceededError

        with self._lock:
            next_value = self.tool_calls + 1
            if self.max_tool_calls is not None and next_value > self.max_tool_calls:
                raise BudgetExceededError(
                    f"tool-call budget exceeded before invoking {name!r}: "
                    f"limit={self.max_tool_calls}"
                )
            self.tool_calls = next_value

    def consume_model_call(self) -> None:
        from .errors import BudgetExceededError

        with self._lock:
            next_calls = self.model_calls + 1
            if self.max_model_calls is not None and next_calls > self.max_model_calls:
                raise BudgetExceededError(
                    f"model-call budget exceeded: used={next_calls}, limit={self.max_model_calls}"
                )
            self.model_calls = next_calls

    def consume_model_usage(self, usage: Mapping[str, Any]) -> None:
        from .cache_first import normalize_usage
        from .errors import BudgetExceededError

        token_value = usage.get("total_tokens", usage.get("total_token_count", 0)) or 0
        normalized = normalize_usage(usage)
        if normalized.total_tokens is not None:
            token_value = normalized.total_tokens
        cost_value = (
            usage.get(
                "cost",
                usage.get("total_cost", usage.get("estimated_cost", 0.0)),
            )
            or 0.0
        )
        tokens = int(token_value)
        cost = float(cost_value)
        if tokens < 0 or cost < 0 or not math.isfinite(cost):
            raise ValueError("model usage tokens and cost must be finite non-negative values")
        with self._lock:
            next_tokens = self.tokens + tokens
            next_cost = self.cost + cost
            if self.max_tokens is not None and next_tokens > self.max_tokens:
                raise BudgetExceededError(
                    f"model token budget exceeded: used={next_tokens}, limit={self.max_tokens}"
                )
            if self.max_cost is not None and next_cost > self.max_cost:
                raise BudgetExceededError(
                    f"model cost budget exceeded: used={next_cost}, limit={self.max_cost}"
                )
            self.tokens = next_tokens
            self.cost = next_cost
            self.cache_requests += 1
            self.usage_total_tokens += tokens
            self.prompt_tokens += int(normalized.prompt_tokens or 0)
            self.completion_tokens += int(normalized.completion_tokens or 0)
            self.cache_hit_tokens += int(normalized.cache_hit_tokens or 0)
            self.cache_miss_tokens += int(normalized.cache_miss_tokens or 0)
            self.cache_write_tokens += int(normalized.cache_write_tokens or 0)
            self.token_savings += int(normalized.token_savings or 0)
            self.estimated_cost += float(normalized.estimated_cost or 0.0)
            self.estimated_cost_savings += float(normalized.estimated_cost_savings or 0.0)
            if normalized.provider_metrics_available:
                self.cache_metrics_requests += 1

    def cache_snapshot(self) -> dict[str, int | float | None]:
        """Return the small, privacy-safe telemetry projection for checkpoints."""

        with self._lock:
            denominator = self.cache_hit_tokens + self.cache_miss_tokens
            return {
                "version": 1,
                "prompt_tokens": self.prompt_tokens,
                "completion_tokens": self.completion_tokens,
                "total_tokens": self.usage_total_tokens,
                "cache_hit_tokens": self.cache_hit_tokens,
                "cache_miss_tokens": self.cache_miss_tokens,
                "cache_write_tokens": self.cache_write_tokens,
                "cache_requests": self.cache_requests,
                "cache_metrics_requests": self.cache_metrics_requests,
                "cache_hit_rate": self.cache_hit_tokens / denominator if denominator else None,
                "total_input_token_hit_rate": (
                    self.cache_hit_tokens / self.prompt_tokens if self.prompt_tokens else None
                ),
                "token_savings": self.token_savings,
                "estimated_cost": self.estimated_cost,
                "estimated_cost_savings": self.estimated_cost_savings,
            }

    def restore_cache_snapshot(self, snapshot: Mapping[str, Any]) -> None:
        """Hydrate cumulative cache counters from checkpoint metadata."""

        with self._lock:
            self.usage_total_tokens = int(snapshot.get("total_tokens", 0) or 0)
            self.prompt_tokens = int(snapshot.get("prompt_tokens", 0) or 0)
            self.completion_tokens = int(snapshot.get("completion_tokens", 0) or 0)
            self.cache_hit_tokens = int(snapshot.get("cache_hit_tokens", 0) or 0)
            self.cache_miss_tokens = int(snapshot.get("cache_miss_tokens", 0) or 0)
            self.cache_write_tokens = int(snapshot.get("cache_write_tokens", 0) or 0)
            self.cache_requests = int(snapshot.get("cache_requests", 0) or 0)
            self.cache_metrics_requests = int(snapshot.get("cache_metrics_requests", 0) or 0)
            self.token_savings = int(snapshot.get("token_savings", 0) or 0)
            self.estimated_cost = float(snapshot.get("estimated_cost", 0.0) or 0.0)
            self.estimated_cost_savings = float(
                snapshot.get("estimated_cost_savings", 0.0) or 0.0
            )

    def snapshot(self) -> dict[str, int | float | None]:
        with self._lock:
            denominator = self.cache_hit_tokens + self.cache_miss_tokens
            return {
                "model_calls": self.model_calls,
                "tool_calls": self.tool_calls,
                "tokens": self.tokens,
                "cost": self.cost,
                "max_model_calls": self.max_model_calls,
                "max_tool_calls": self.max_tool_calls,
                "max_tokens": self.max_tokens,
                "max_cost": self.max_cost,
                "prompt_tokens": self.prompt_tokens,
                "completion_tokens": self.completion_tokens,
                "cache_hit_tokens": self.cache_hit_tokens,
                "cache_miss_tokens": self.cache_miss_tokens,
                "cache_write_tokens": self.cache_write_tokens,
                "cache_requests": self.cache_requests,
                "cache_metrics_requests": self.cache_metrics_requests,
                "usage_total_tokens": self.usage_total_tokens,
                "token_savings": self.token_savings,
                "estimated_cost": self.estimated_cost,
                "estimated_cost_savings": self.estimated_cost_savings,
                "cache_hit_rate": self.cache_hit_tokens / denominator if denominator else None,
                "total_input_token_hit_rate": (
                    self.cache_hit_tokens / self.prompt_tokens if self.prompt_tokens else None
                ),
            }


class CancellationToken:
    """Thread-safe cooperative cancellation shared with sync and async nodes."""

    def __init__(self) -> None:
        self._event = ThreadEvent()

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()

    def cancel(self) -> None:
        self._event.set()

    def raise_if_cancelled(self) -> None:
        if self.cancelled:
            from .errors import GraphCancelledError

            raise GraphCancelledError("graph run was cancelled")

    async def wait(self) -> None:
        while not self.cancelled:
            await asyncio.sleep(0.05)


@dataclass(frozen=True, slots=True)
class Runtime(Generic[ContextT]):
    """Stable runtime services injected into nodes that request ``runtime``."""

    context: ContextT | None
    config: Mapping[str, Any]
    store: Any | None = None
    cache: Any | None = None
    cancellation: CancellationToken | None = None
    deadline: datetime | None = None
    run_id: str = ""
    task_id: str = ""
    checkpoint_id: str | None = None
    namespace: tuple[str, ...] = ()
    idempotency_key: str = ""
    metadata: Mapping[str, Any] | None = None
    remaining_steps: int | None = None
    stream_mode: str | None = None
    stream_subgraphs: bool = False
    budget: ExecutionBudget | None = None
    _emit: StreamEmitter | None = None
    _steering: SteeringChannel | None = None

    @property
    def has_steering(self) -> bool:
        """Whether one or more steering events are waiting to be drained.

        This is a stable, generic read -- it never interprets the payload.
        Safe to call from sync or async nodes, and from any subgraph
        namespace (steering is scoped per top-level run, not per namespace).
        """

        return self._steering is not None and self._steering.has_pending

    #: Alias suggested by the design doc for "should I consider replanning".
    steering_pending = has_steering

    def peek_steering(self) -> tuple[SteeringEvent, ...]:
        """Read pending steering events without consuming them."""

        if self._steering is None:
            return ()
        return self._steering.peek()

    def drain_steering(self) -> tuple[SteeringEvent, ...]:
        """Atomically consume and return pending steering events in order.

        Once drained, the same events are not re-exposed by this channel.
        The graph decides what to do with them (update state, replan, goto
        another node, ignore, ask the user) -- LingxiGraph only guarantees
        durable delivery, ordering, dedup and safe consumption.
        """

        if self._steering is None:
            return ()
        return self._steering.drain()

    @property
    def cancelled(self) -> bool:
        return self.cancellation.cancelled if self.cancellation is not None else False

    def raise_if_cancelled(self) -> None:
        if self.cancellation is not None:
            self.cancellation.raise_if_cancelled()
        if self.deadline is not None and datetime.now(UTC) >= self.deadline:
            from .errors import GraphTimeoutError

            raise GraphTimeoutError("graph run deadline exceeded")

    def emit(self, channel: str, value: Any) -> None:
        """Emit named provider-neutral stream data without waiting for node completion.

        ``emit`` is the LingxiGraph extension for named channels.  Portable
        LangGraph-style nodes should call :attr:`stream_writer` or
        :func:`get_stream_writer` with one value instead.
        """

        if not isinstance(channel, str) or not channel:
            raise ValueError("stream channel must be a non-empty string")
        if self._emit is not None:
            self._emit(channel, value)

    @property
    def stream_writer(self) -> StreamWriter:
        """Return the standard single-argument custom stream writer.

        The optional second argument is retained as a compatibility bridge for
        the earlier ``writer(channel, value)`` LingxiGraph API.
        """

        def write(value: Any, *legacy: Any) -> None:
            if legacy:
                if len(legacy) != 1 or not isinstance(value, str):
                    raise TypeError("stream writer expects writer(value)")
                self.emit(value, legacy[0])
                return
            self.emit("custom", value)

        return write

    def emit_message(self, message: Any, metadata: Mapping[str, Any] | None = None) -> None:
        """Emit a model message/chunk with task-scoped stream metadata."""

        envelope = (
            message,
            {
                "run_id": self.run_id,
                "task_id": self.task_id,
                "namespace": self.namespace,
                **dict(metadata or {}),
            },
        )
        self.emit("messages", envelope)

    def consume_tool_call(self, name: str) -> None:
        if self.budget is not None:
            self.budget.consume_tool_call(name)

    def consume_model_usage(self, usage: Mapping[str, Any]) -> None:
        if self.budget is not None:
            self.budget.consume_model_usage(usage)

    def consume_model_call(self) -> None:
        if self.budget is not None:
            self.budget.consume_model_call()


@dataclass(frozen=True, slots=True)
class _RuntimeContext:
    config: Mapping[str, Any]
    store: Any | None
    runtime: Runtime[Any] | None = None


_runtime_context: ContextVar[_RuntimeContext | None] = ContextVar(
    "lingxigraph_runtime_context", default=None
)


def get_config() -> Mapping[str, Any]:
    """Return the run config of the currently executing node."""

    context = _runtime_context.get()
    if context is None:
        raise RuntimeError("get_config() must be called while a graph node is executing")
    return context.config


def get_store() -> Any:
    """Return the store the graph was compiled with, from inside a node."""

    context = _runtime_context.get()
    if context is None:
        raise RuntimeError("get_store() must be called while a graph node is executing")
    if context.store is None:
        raise RuntimeError("no store is configured; pass compile(store=...) to enable it")
    return context.store


def get_runtime() -> Runtime[Any]:
    """Return the full runtime context of the executing node."""

    context = _runtime_context.get()
    if context is None or context.runtime is None:
        raise RuntimeError("get_runtime() must be called while a graph node is executing")
    return context.runtime


def get_stream_writer() -> StreamWriter:
    """Return the task-local LangGraph-compatible ``writer(value)`` callable."""

    return get_runtime().stream_writer


def _set_runtime_context(context: _RuntimeContext) -> Token[_RuntimeContext | None]:
    return _runtime_context.set(context)


def _reset_runtime_context(token: Token[_RuntimeContext | None]) -> None:
    _runtime_context.reset(token)


__all__ = [
    "CancellationToken",
    "ExecutionBudget",
    "Runtime",
    "SteeringChannel",
    "SteeringEvent",
    "StreamWriter",
    "get_config",
    "get_runtime",
    "get_store",
    "get_stream_writer",
]
