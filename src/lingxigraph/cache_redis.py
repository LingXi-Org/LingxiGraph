"""Optional Redis-backed node cache."""

from __future__ import annotations

import asyncio
from typing import Any

from .serialization import JsonSerializer


class RedisCache:
    """Async-first Redis cache with strict JSON payloads and namespace clearing."""

    def __init__(
        self,
        url: str = "redis://localhost:6379/0",
        *,
        prefix: str = "lingxigraph:cache",
        serializer: JsonSerializer | None = None,
    ) -> None:
        try:
            from redis.asyncio import Redis
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("install lingxigraph[redis] to use RedisCache") from exc
        self._redis = Redis.from_url(url)
        self._prefix = prefix.rstrip(":")
        self._serializer = serializer or JsonSerializer()

    def _key(self, key: str) -> str:
        return f"{self._prefix}:{key}"

    async def aget(self, key: str) -> Any | None:
        payload = await self._redis.get(self._key(key))
        return None if payload is None else self._serializer.loads(payload)

    async def aset(self, key: str, value: Any, *, ttl: float | None = None) -> None:
        milliseconds = max(1, int(ttl * 1000)) if ttl is not None else None
        await self._redis.set(
            self._key(key), self._serializer.dumps(value), px=milliseconds
        )

    async def adelete(self, key: str) -> None:
        await self._redis.delete(self._key(key))

    async def aclear(self, *, namespace: str | None = None) -> None:
        pattern = self._key(f"{namespace}:*" if namespace else "*")
        async for key in self._redis.scan_iter(match=pattern, count=500):
            await self._redis.delete(key)

    def get(self, key: str) -> Any | None:
        return asyncio.run(self.aget(key))

    def set(self, key: str, value: Any, *, ttl: float | None = None) -> None:
        asyncio.run(self.aset(key, value, ttl=ttl))

    def delete(self, key: str) -> None:
        asyncio.run(self.adelete(key))

    def clear(self, *, namespace: str | None = None) -> None:
        asyncio.run(self.aclear(namespace=namespace))

    async def close(self) -> None:
        await self._redis.aclose()


__all__ = ["RedisCache"]
