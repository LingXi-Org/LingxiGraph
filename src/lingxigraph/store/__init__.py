"""Long-term memory shared across threads, keyed by namespace and key."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class Item:
    """One stored memory: a dict value addressed by ``(namespace, key)``."""

    namespace: tuple[str, ...]
    key: str
    value: Mapping[str, Any]
    created_at: str
    updated_at: str


@dataclass(frozen=True, slots=True)
class StoreOperation:
    """Portable operation accepted by store batch APIs."""

    kind: str
    namespace: tuple[str, ...]
    key: str | None = None
    value: Mapping[str, Any] | None = None
    query: str | None = None
    filter: Mapping[str, Any] | None = None
    limit: int = 10
    offset: int = 0


@runtime_checkable
class BaseStore(Protocol):
    """Cross-thread key-value memory with namespace-prefix search."""

    def put(self, namespace: Sequence[str], key: str, value: Mapping[str, Any]) -> None: ...

    def get(self, namespace: Sequence[str], key: str) -> Item | None: ...

    def delete(self, namespace: Sequence[str], key: str) -> None: ...

    def search(
        self,
        namespace_prefix: Sequence[str],
        *,
        query: str | None = None,
        filter: Mapping[str, Any] | None = None,
        limit: int = 10,
        offset: int = 0,
    ) -> list[Item]: ...

    def list_namespaces(self, *, prefix: Sequence[str] = ()) -> list[tuple[str, ...]]: ...

    def batch(self, operations: Sequence[StoreOperation]) -> list[Any]: ...


@runtime_checkable
class AsyncStore(Protocol):
    async def aput(
        self, namespace: Sequence[str], key: str, value: Mapping[str, Any]
    ) -> None: ...

    async def aget(self, namespace: Sequence[str], key: str) -> Item | None: ...

    async def adelete(self, namespace: Sequence[str], key: str) -> None: ...

    async def asearch(
        self,
        namespace_prefix: Sequence[str],
        *,
        query: str | None = None,
        filter: Mapping[str, Any] | None = None,
        limit: int = 10,
        offset: int = 0,
    ) -> list[Item]: ...

    async def abatch(self, operations: Sequence[StoreOperation]) -> list[Any]: ...


def _validate_namespace(namespace: Sequence[str]) -> tuple[str, ...]:
    parts = tuple(namespace)
    if not parts:
        raise ValueError("namespace must contain at least one label")
    for part in parts:
        if not isinstance(part, str) or not part:
            raise ValueError("namespace labels must be non-empty strings")
    return parts


from .memory import InMemoryStore
from .postgres import AsyncPostgresStore, PostgresStore

__all__ = [
    "AsyncStore",
    "BaseStore",
    "InMemoryStore",
    "Item",
    "PostgresStore",
    "AsyncPostgresStore",
    "StoreOperation",
]
