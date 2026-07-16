"""Thread-safe in-process store for long-term memory."""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Mapping, Sequence
from threading import RLock
from typing import Any

from ..types import _utc_now
from . import Item, StoreOperation, _validate_namespace


class InMemoryStore:
    """Store dict values under ``(namespace, key)``, searchable by prefix."""

    def __init__(self) -> None:
        self._data: dict[tuple[str, ...], dict[str, Item]] = {}
        self._lock = RLock()

    def put(self, namespace: Sequence[str], key: str, value: Mapping[str, Any]) -> None:
        parts = _validate_namespace(namespace)
        if not isinstance(key, str) or not key:
            raise ValueError("store key must be a non-empty string")
        if not isinstance(value, Mapping):
            raise ValueError("store value must be a mapping")
        now = _utc_now()
        with self._lock:
            bucket = self._data.setdefault(parts, {})
            existing = bucket.get(key)
            bucket[key] = Item(
                namespace=parts,
                key=key,
                value=copy.deepcopy(dict(value)),
                created_at=existing.created_at if existing else now,
                updated_at=now,
            )

    def get(self, namespace: Sequence[str], key: str) -> Item | None:
        parts = _validate_namespace(namespace)
        with self._lock:
            item = self._data.get(parts, {}).get(key)
            return copy.deepcopy(item)

    def delete(self, namespace: Sequence[str], key: str) -> None:
        parts = _validate_namespace(namespace)
        with self._lock:
            bucket = self._data.get(parts)
            if bucket is not None:
                bucket.pop(key, None)
                if not bucket:
                    del self._data[parts]

    def search(
        self,
        namespace_prefix: Sequence[str],
        *,
        query: str | None = None,
        filter: Mapping[str, Any] | None = None,
        limit: int = 10,
        offset: int = 0,
    ) -> list[Item]:
        prefix = tuple(namespace_prefix)
        if limit < 0 or offset < 0:
            raise ValueError("limit and offset must be non-negative")
        with self._lock:
            matches: list[Item] = []
            for namespace in sorted(self._data):
                if namespace[: len(prefix)] != prefix:
                    continue
                for key in sorted(self._data[namespace]):
                    item = self._data[namespace][key]
                    if filter is not None and any(
                        item.value.get(field) != expected for field, expected in filter.items()
                    ):
                        continue
                    if query is not None and query.lower() not in repr(item.value).lower():
                        continue
                    matches.append(copy.deepcopy(item))
            return matches[offset : offset + limit]

    def list_namespaces(self, *, prefix: Sequence[str] = ()) -> list[tuple[str, ...]]:
        head = tuple(prefix)
        with self._lock:
            return sorted(
                namespace for namespace in self._data if namespace[: len(head)] == head
            )

    def batch(self, operations: Sequence[StoreOperation]) -> list[Any]:
        results: list[Any] = []
        for operation in operations:
            if operation.kind == "get":
                results.append(self.get(operation.namespace, operation.key or ""))
            elif operation.kind == "put":
                if operation.key is None or operation.value is None:
                    raise ValueError("put operation requires key and value")
                self.put(operation.namespace, operation.key, operation.value)
                results.append(None)
            elif operation.kind == "delete":
                self.delete(operation.namespace, operation.key or "")
                results.append(None)
            elif operation.kind == "search":
                results.append(
                    self.search(
                        operation.namespace,
                        query=operation.query,
                        filter=operation.filter,
                        limit=operation.limit,
                        offset=operation.offset,
                    )
                )
            else:
                raise ValueError(f"unknown store operation {operation.kind!r}")
        return results

    async def aput(self, namespace, key, value):
        await asyncio.to_thread(self.put, namespace, key, value)

    async def aget(self, namespace, key):
        return await asyncio.to_thread(self.get, namespace, key)

    async def adelete(self, namespace, key):
        await asyncio.to_thread(self.delete, namespace, key)

    async def asearch(self, namespace_prefix, **kwargs):
        return await asyncio.to_thread(self.search, namespace_prefix, **kwargs)

    async def abatch(self, operations):
        return await asyncio.to_thread(self.batch, operations)


__all__ = ["InMemoryStore"]
