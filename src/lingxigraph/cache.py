"""Provider-neutral cache protocol and in-memory implementation."""

from __future__ import annotations

import asyncio
import copy
import time
from dataclasses import dataclass
from threading import RLock
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class CacheEntry:
    value: Any
    expires_at: float | None = None


@runtime_checkable
class BaseCache(Protocol):
    def get(self, key: str) -> Any | None: ...

    def set(self, key: str, value: Any, *, ttl: float | None = None) -> None: ...

    def delete(self, key: str) -> None: ...

    def clear(self, *, namespace: str | None = None) -> None: ...


@runtime_checkable
class AsyncCache(Protocol):
    async def aget(self, key: str) -> Any | None: ...

    async def aset(self, key: str, value: Any, *, ttl: float | None = None) -> None: ...

    async def adelete(self, key: str) -> None: ...


class InMemoryCache:
    """Thread-safe TTL cache intended for tests and embedded deployments."""

    def __init__(self) -> None:
        self._items: dict[str, CacheEntry] = {}
        self._lock = RLock()

    def get(self, key: str) -> Any | None:
        with self._lock:
            entry = self._items.get(key)
            if entry is None:
                return None
            if entry.expires_at is not None and entry.expires_at <= time.monotonic():
                self._items.pop(key, None)
                return None
            return copy.deepcopy(entry.value)

    def set(self, key: str, value: Any, *, ttl: float | None = None) -> None:
        expires_at = time.monotonic() + ttl if ttl is not None else None
        with self._lock:
            self._items[key] = CacheEntry(copy.deepcopy(value), expires_at)

    def delete(self, key: str) -> None:
        with self._lock:
            self._items.pop(key, None)

    def clear(self, *, namespace: str | None = None) -> None:
        with self._lock:
            if namespace is None:
                self._items.clear()
                return
            prefix = f"{namespace}:"
            for key in [item for item in self._items if item.startswith(prefix)]:
                self._items.pop(key, None)

    async def aget(self, key: str) -> Any | None:
        return await asyncio.to_thread(self.get, key)

    async def aset(self, key: str, value: Any, *, ttl: float | None = None) -> None:
        await asyncio.to_thread(self.set, key, value, ttl=ttl)

    async def adelete(self, key: str) -> None:
        await asyncio.to_thread(self.delete, key)


__all__ = ["AsyncCache", "BaseCache", "CacheEntry", "InMemoryCache"]
