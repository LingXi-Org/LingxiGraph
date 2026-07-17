"""Thread-safe in-process store for long-term memory."""

from __future__ import annotations

import asyncio
import copy
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from threading import RLock
from typing import Any

from ..types import _utc_now
from . import Embedder, Item, StoreOperation, _validate_namespace


class InMemoryStore:
    """Store dict values under ``(namespace, key)``, searchable by prefix."""

    def __init__(self, *, embedder: Embedder | None = None) -> None:
        self._data: dict[tuple[str, ...], dict[str, Item]] = {}
        self._lock = RLock()
        self._embedder = embedder
        self._vectors: dict[tuple[tuple[str, ...], str], tuple[float, ...]] = {}

    def put(
        self,
        namespace: Sequence[str],
        key: str,
        value: Mapping[str, Any],
        *,
        ttl: float | None = None,
    ) -> None:
        parts = _validate_namespace(namespace)
        if not isinstance(key, str) or not key:
            raise ValueError("store key must be a non-empty string")
        if not isinstance(value, Mapping):
            raise ValueError("store value must be a mapping")
        if ttl is not None and ttl <= 0:
            raise ValueError("ttl must be positive")
        now = _utc_now()
        expires_at = (
            (datetime.now(UTC) + timedelta(seconds=ttl)).isoformat()
            if ttl is not None
            else None
        )
        with self._lock:
            bucket = self._data.setdefault(parts, {})
            existing = bucket.get(key)
            bucket[key] = Item(
                namespace=parts,
                key=key,
                value=copy.deepcopy(dict(value)),
                created_at=existing.created_at if existing else now,
                updated_at=now,
                expires_at=expires_at,
            )
            if self._embedder is not None:
                self._vectors[(parts, key)] = tuple(
                    float(item) for item in self._embedder.embed(repr(dict(value)))
                )

    def get(self, namespace: Sequence[str], key: str) -> Item | None:
        parts = _validate_namespace(namespace)
        with self._lock:
            self._purge_expired()
            item = self._data.get(parts, {}).get(key)
            return copy.deepcopy(item)

    def delete(self, namespace: Sequence[str], key: str) -> None:
        parts = _validate_namespace(namespace)
        with self._lock:
            bucket = self._data.get(parts)
            if bucket is not None:
                bucket.pop(key, None)
                self._vectors.pop((parts, key), None)
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
            self._purge_expired()
            matches: list[tuple[float, Item]] = []
            query_vector = (
                tuple(float(item) for item in self._embedder.embed(query))
                if query is not None and self._embedder is not None
                else None
            )
            for namespace in sorted(self._data):
                if namespace[: len(prefix)] != prefix:
                    continue
                for key in sorted(self._data[namespace]):
                    item = self._data[namespace][key]
                    if filter is not None and any(
                        item.value.get(field) != expected for field, expected in filter.items()
                    ):
                        continue
                    score = 0.0
                    if query_vector is not None:
                        score = self._cosine(
                            query_vector, self._vectors.get((namespace, key), ())
                        )
                    elif query is not None:
                        if query.lower() not in repr(item.value).lower():
                            continue
                    matches.append((score, copy.deepcopy(item)))
            matches.sort(key=lambda pair: (-pair[0], pair[1].namespace, pair[1].key))
            return [item for _, item in matches[offset : offset + limit]]

    def list_namespaces(self, *, prefix: Sequence[str] = ()) -> list[tuple[str, ...]]:
        head = tuple(prefix)
        with self._lock:
            self._purge_expired()
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
                self.put(
                    operation.namespace,
                    operation.key,
                    operation.value,
                    ttl=operation.ttl,
                )
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

    async def aput(self, namespace, key, value, *, ttl=None):
        await asyncio.to_thread(self.put, namespace, key, value, ttl=ttl)

    async def aget(self, namespace, key):
        return await asyncio.to_thread(self.get, namespace, key)

    async def adelete(self, namespace, key):
        await asyncio.to_thread(self.delete, namespace, key)

    async def asearch(self, namespace_prefix, **kwargs):
        return await asyncio.to_thread(self.search, namespace_prefix, **kwargs)

    async def abatch(self, operations):
        return await asyncio.to_thread(self.batch, operations)

    def _purge_expired(self) -> None:
        now = datetime.now(UTC)
        for namespace, bucket in list(self._data.items()):
            for key, item in list(bucket.items()):
                if item.expires_at is not None and datetime.fromisoformat(item.expires_at) <= now:
                    bucket.pop(key, None)
                    self._vectors.pop((namespace, key), None)
            if not bucket:
                self._data.pop(namespace, None)

    @staticmethod
    def _cosine(left: Sequence[float], right: Sequence[float]) -> float:
        if not left or len(left) != len(right):
            return 0.0
        denominator = math.sqrt(sum(x * x for x in left)) * math.sqrt(
            sum(x * x for x in right)
        )
        return sum(x * y for x, y in zip(left, right, strict=True)) / denominator if denominator else 0.0


__all__ = ["InMemoryStore"]
