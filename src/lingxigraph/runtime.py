"""Task-scoped runtime context readable from inside executing nodes."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from contextvars import ContextVar, Token
from dataclasses import dataclass
from datetime import UTC, datetime
from threading import Event as ThreadEvent
from typing import Any, Generic, TypeVar

ContextT = TypeVar("ContextT")
StreamEmitter = Callable[[str, Any], None]


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
        """Emit provider-neutral custom or message stream data."""

        if not isinstance(channel, str) or not channel:
            raise ValueError("stream channel must be a non-empty string")
        if self._emit is not None:
            self._emit(channel, value)

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


def get_stream_writer() -> StreamEmitter:
    """Return a writer compatible with ``writer(channel, value)``."""

    return get_runtime().emit


def _set_runtime_context(context: _RuntimeContext) -> Token[_RuntimeContext | None]:
    return _runtime_context.set(context)


def _reset_runtime_context(token: Token[_RuntimeContext | None]) -> None:
    _runtime_context.reset(token)


__all__ = [
    "CancellationToken",
    "Runtime",
    "get_config",
    "get_runtime",
    "get_store",
    "get_stream_writer",
]
