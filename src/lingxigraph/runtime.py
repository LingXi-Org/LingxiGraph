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
        from .errors import BudgetExceededError

        token_value = usage.get("total_tokens", usage.get("total_token_count", 0)) or 0
        cost_value = usage.get("cost", usage.get("total_cost", 0.0)) or 0.0
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

    def snapshot(self) -> dict[str, int | float | None]:
        with self._lock:
            return {
                "model_calls": self.model_calls,
                "tool_calls": self.tool_calls,
                "tokens": self.tokens,
                "cost": self.cost,
                "max_model_calls": self.max_model_calls,
                "max_tool_calls": self.max_tool_calls,
                "max_tokens": self.max_tokens,
                "max_cost": self.max_cost,
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
    "StreamWriter",
    "get_config",
    "get_runtime",
    "get_store",
    "get_stream_writer",
]
