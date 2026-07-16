"""Checkpoint protocol and persisted value types."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from ..types import Interrupt, Send, TaskSnapshot


@dataclass(frozen=True, slots=True)
class PendingWrite:
    """One durable task result written before its superstep commits."""

    checkpoint_id: str
    task_id: str
    index: int
    values: Mapping[str, Any]
    task_path: tuple[str, ...] = ()
    goto: tuple[str | Send, ...] = ()
    error: str | None = None


@dataclass(frozen=True, slots=True)
class Checkpoint:
    id: str
    ts: str
    step: int
    channel_values: Mapping[str, Any]
    next: tuple[str, ...]
    pending_sends: tuple[Send, ...] = ()
    pending_interrupts: tuple[Interrupt, ...] = ()
    parent_id: str | None = None
    namespace: tuple[str, ...] = ()
    run_id: str | None = None
    channel_versions: Mapping[str, int] = field(default_factory=dict)
    tasks: tuple[TaskSnapshot, ...] = ()
    schema_version: int = 1


@dataclass(frozen=True, slots=True)
class CheckpointTuple:
    config: Mapping[str, Any]
    checkpoint: Checkpoint
    metadata: Mapping[str, Any] = field(default_factory=dict)


@runtime_checkable
class Checkpointer(Protocol):
    def put(
        self,
        config: Mapping[str, Any],
        checkpoint: Checkpoint,
        metadata: Mapping[str, Any],
    ) -> Mapping[str, Any]: ...

    def get_tuple(self, config: Mapping[str, Any]) -> CheckpointTuple | None: ...

    def list(self, config: Mapping[str, Any]) -> Iterable[CheckpointTuple]: ...

    def put_writes(
        self,
        config: Mapping[str, Any],
        checkpoint_id: str,
        writes: Iterable[PendingWrite],
    ) -> None: ...

    def get_writes(
        self, config: Mapping[str, Any], checkpoint_id: str
    ) -> Iterable[PendingWrite]: ...

    def delete_thread(self, config: Mapping[str, Any]) -> None: ...


@runtime_checkable
class AsyncCheckpointer(Protocol):
    async def aput(
        self,
        config: Mapping[str, Any],
        checkpoint: Checkpoint,
        metadata: Mapping[str, Any],
    ) -> Mapping[str, Any]: ...

    async def aget_tuple(self, config: Mapping[str, Any]) -> CheckpointTuple | None: ...

    def alist(self, config: Mapping[str, Any]) -> AsyncIterator[CheckpointTuple]: ...

    async def aput_writes(
        self,
        config: Mapping[str, Any],
        checkpoint_id: str,
        writes: Iterable[PendingWrite],
    ) -> None: ...

    async def aget_writes(
        self, config: Mapping[str, Any], checkpoint_id: str
    ) -> Iterable[PendingWrite]: ...


from .memory import InMemorySaver
from .postgres import AsyncPostgresSaver, PostgresSaver
from .sqlite import SqliteSaver

__all__ = [
    "AsyncCheckpointer",
    "Checkpoint",
    "Checkpointer",
    "CheckpointTuple",
    "InMemorySaver",
    "PendingWrite",
    "PostgresSaver",
    "AsyncPostgresSaver",
    "SqliteSaver",
]
