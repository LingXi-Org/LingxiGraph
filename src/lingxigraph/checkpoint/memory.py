"""Thread-safe in-process checkpoint storage."""

from __future__ import annotations

import asyncio
import copy
from collections import defaultdict
from collections.abc import Iterable, Mapping
from threading import RLock
from typing import Any

from . import Checkpoint, CheckpointTuple, PendingWrite


def _thread_id(config: Mapping[str, Any]) -> str:
    configurable = config.get("configurable")
    if not isinstance(configurable, Mapping) or not configurable.get("thread_id"):
        raise ValueError("checkpoint operations require config['configurable']['thread_id']")
    return str(configurable["thread_id"])


def _thread_key(config: Mapping[str, Any]) -> tuple[str, str]:
    thread_id = _thread_id(config)
    namespace = str(config.get("configurable", {}).get("checkpoint_ns", ""))
    return thread_id, namespace


class InMemorySaver:
    """Store immutable checkpoint histories grouped by ``thread_id``."""

    def __init__(self) -> None:
        self._storage: dict[tuple[str, str], list[CheckpointTuple]] = defaultdict(list)
        self._writes: dict[
            tuple[str, str, str], dict[tuple[str, int], PendingWrite]
        ] = defaultdict(dict)
        self._lock = RLock()

    def put(
        self,
        config: Mapping[str, Any],
        checkpoint: Checkpoint,
        metadata: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        thread_id = _thread_id(config)
        thread_key = _thread_key(config)
        stored_config = {
            **copy.deepcopy(dict(config)),
            "configurable": {
                **copy.deepcopy(dict(config.get("configurable", {}))),
                "thread_id": thread_id,
                "checkpoint_id": checkpoint.id,
            },
        }
        item = CheckpointTuple(
            stored_config,
            copy.deepcopy(checkpoint),
            copy.deepcopy(dict(metadata)),
        )
        with self._lock:
            self._storage[thread_key].append(item)
        return stored_config

    def get_tuple(self, config: Mapping[str, Any]) -> CheckpointTuple | None:
        thread_key = _thread_key(config)
        checkpoint_id = config.get("configurable", {}).get("checkpoint_id")
        with self._lock:
            items = self._storage.get(thread_key, ())
            if checkpoint_id is None:
                item = items[-1] if items else None
            else:
                item = next(
                    (entry for entry in reversed(items) if entry.checkpoint.id == checkpoint_id),
                    None,
                )
            return copy.deepcopy(item)

    def list(self, config: Mapping[str, Any]) -> Iterable[CheckpointTuple]:
        thread_key = _thread_key(config)
        with self._lock:
            return iter(copy.deepcopy(list(reversed(self._storage.get(thread_key, ())))))

    def put_writes(
        self,
        config: Mapping[str, Any],
        checkpoint_id: str,
        writes: Iterable[PendingWrite],
    ) -> None:
        thread_id, namespace = _thread_key(config)
        with self._lock:
            target = self._writes[(thread_id, namespace, checkpoint_id)]
            for write in writes:
                target[(write.task_id, write.index)] = copy.deepcopy(write)

    def get_writes(
        self, config: Mapping[str, Any], checkpoint_id: str
    ) -> Iterable[PendingWrite]:
        thread_id, namespace = _thread_key(config)
        with self._lock:
            values = self._writes.get((thread_id, namespace, checkpoint_id), {})
            return tuple(
                copy.deepcopy(values[key])
                for key in sorted(values, key=lambda item: (item[1], item[0]))
            )

    def delete_thread(self, config: Mapping[str, Any]) -> None:
        thread_id = _thread_id(config)
        with self._lock:
            for storage_key in [key for key in self._storage if key[0] == thread_id]:
                self._storage.pop(storage_key, None)
            for write_key in [key for key in self._writes if key[0] == thread_id]:
                self._writes.pop(write_key, None)

    async def aput(self, config, checkpoint, metadata):
        return await asyncio.to_thread(self.put, config, checkpoint, metadata)

    async def aget_tuple(self, config):
        return await asyncio.to_thread(self.get_tuple, config)

    async def alist(self, config):
        for item in await asyncio.to_thread(lambda: list(self.list(config))):
            yield item

    async def aput_writes(self, config, checkpoint_id, writes):
        await asyncio.to_thread(self.put_writes, config, checkpoint_id, tuple(writes))

    async def aget_writes(self, config, checkpoint_id):
        return await asyncio.to_thread(self.get_writes, config, checkpoint_id)


__all__ = ["InMemorySaver"]
