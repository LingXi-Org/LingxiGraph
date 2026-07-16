"""Ephemeral run signaling; durable events remain in the repository."""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from typing import Protocol


class EventBus(Protocol):
    async def publish(self, tenant_id: str, run_id: str, sequence: int) -> None: ...

    async def wait(
        self, tenant_id: str, run_id: str, *, timeout: float = 15.0
    ) -> None: ...


class InMemoryEventBus:
    def __init__(self) -> None:
        self._conditions: dict[tuple[str, str], asyncio.Condition] = defaultdict(
            asyncio.Condition
        )

    async def publish(self, tenant_id: str, run_id: str, sequence: int) -> None:
        del sequence
        condition = self._conditions[(tenant_id, run_id)]
        async with condition:
            condition.notify_all()

    async def wait(self, tenant_id: str, run_id: str, *, timeout: float = 15.0) -> None:
        condition = self._conditions[(tenant_id, run_id)]
        async with condition:
            try:
                await asyncio.wait_for(condition.wait(), timeout)
            except TimeoutError:
                return


class RedisEventBus:
    """Redis pub/sub signaling with polling-compatible failure behavior."""

    def __init__(self, url: str, *, prefix: str = "lingxigraph:runs") -> None:
        try:
            from redis.asyncio import Redis
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("install lingxigraph[redis] to use RedisEventBus") from exc
        self._redis = Redis.from_url(url)
        self._prefix = prefix

    def _channel(self, tenant_id: str, run_id: str) -> str:
        return f"{self._prefix}:{tenant_id}:{run_id}"

    async def publish(self, tenant_id: str, run_id: str, sequence: int) -> None:
        try:
            await self._redis.publish(
                self._channel(tenant_id, run_id),
                json.dumps({"sequence": sequence}),
            )
        except Exception:
            return

    async def wait(self, tenant_id: str, run_id: str, *, timeout: float = 15.0) -> None:
        pubsub = self._redis.pubsub()
        try:
            await pubsub.subscribe(self._channel(tenant_id, run_id))
            await asyncio.wait_for(pubsub.get_message(ignore_subscribe_messages=True), timeout)
        except Exception:
            return
        finally:
            try:
                await pubsub.aclose()
            except Exception:
                pass

    async def close(self) -> None:
        await self._redis.aclose()


__all__ = ["EventBus", "InMemoryEventBus", "RedisEventBus"]
