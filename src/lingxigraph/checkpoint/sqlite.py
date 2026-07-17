"""Safe JSON SQLite checkpointer for local development.

Persisted values are versioned JSON and are never executed while loading.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Iterable, Mapping
from threading import RLock
from typing import Any

from ..errors import PersistenceError
from ..serialization import JsonSerializer
from ..types import Interrupt, Send, TaskSnapshot
from . import Checkpoint, CheckpointTuple, PendingWrite


def _thread_id(config: Mapping[str, Any]) -> str:
    configurable = config.get("configurable")
    if not isinstance(configurable, Mapping) or not configurable.get("thread_id"):
        raise ValueError("checkpoint operations require config['configurable']['thread_id']")
    return str(configurable["thread_id"])


def _interrupt(value: Mapping[str, Any] | Interrupt) -> Interrupt:
    if isinstance(value, Interrupt):
        return value
    return Interrupt(
        value=value.get("value"),
        resumable=bool(value.get("resumable", True)),
        id=value.get("id"),
        when=str(value.get("when", "during")),
        task_id=value.get("task_id"),
        namespace=tuple(value.get("namespace", ())),
        task_path=tuple(value.get("task_path", ())),
    )


def _task(value: Mapping[str, Any] | TaskSnapshot) -> TaskSnapshot:
    if isinstance(value, TaskSnapshot):
        return value
    return TaskSnapshot(
        id=str(value["id"]),
        name=str(value["name"]),
        path=tuple(value.get("path", ())),
        error=value.get("error"),
        interrupts=tuple(_interrupt(item) for item in value.get("interrupts", ())),
        result=value.get("result"),
    )


def _checkpoint(value: Mapping[str, Any] | Checkpoint) -> Checkpoint:
    if isinstance(value, Checkpoint):
        return value
    schema_version = int(value.get("schema_version", 1))
    if schema_version > 2:
        raise PersistenceError(
            f"checkpoint schema_version={schema_version} is newer than supported version 2"
        )
    return Checkpoint(
        id=str(value["id"]),
        ts=str(value["ts"]),
        step=int(value["step"]),
        channel_values=dict(value.get("channel_values", {})),
        next=tuple(value.get("next", ())),
        pending_sends=tuple(
            item if isinstance(item, Send) else Send(str(item["node"]), item.get("arg"))
            for item in value.get("pending_sends", ())
        ),
        pending_interrupts=tuple(
            _interrupt(item) for item in value.get("pending_interrupts", ())
        ),
        parent_id=value.get("parent_id"),
        namespace=tuple(value.get("namespace", ())),
        run_id=value.get("run_id"),
        channel_versions={
            str(key): int(version)
            for key, version in value.get("channel_versions", {}).items()
        },
        tasks=tuple(_task(item) for item in value.get("tasks", ())),
        schema_version=2,
    )


class SqliteSaver:
    """Thread-safe checkpoint history backed by SQLite and strict JSON."""

    def __init__(
        self,
        database: str | sqlite3.Connection = ":memory:",
        *,
        serializer: JsonSerializer | None = None,
    ) -> None:
        if isinstance(database, sqlite3.Connection):
            self._conn = database
        else:
            self._conn = sqlite3.connect(database, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._serializer = serializer or JsonSerializer()
        self._lock = RLock()
        self.setup()

    def setup(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS checkpoints_v1 (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    thread_id TEXT NOT NULL,
                    checkpoint_id TEXT NOT NULL,
                    namespace TEXT NOT NULL DEFAULT '',
                    ts TEXT NOT NULL,
                    step INTEGER NOT NULL,
                    config_json BLOB NOT NULL,
                    checkpoint_json BLOB NOT NULL,
                    metadata_json BLOB NOT NULL,
                    UNIQUE (thread_id, namespace, checkpoint_id)
                );
                CREATE INDEX IF NOT EXISTS checkpoints_v1_by_thread
                    ON checkpoints_v1 (thread_id, namespace, seq);
                CREATE TABLE IF NOT EXISTS checkpoint_writes_v2 (
                    thread_id TEXT NOT NULL,
                    namespace TEXT NOT NULL DEFAULT '',
                    checkpoint_id TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    write_index INTEGER NOT NULL,
                    write_json BLOB NOT NULL,
                    PRIMARY KEY (thread_id, namespace, checkpoint_id, task_id, write_index)
                );
                """
            )
            legacy = self._conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='checkpoint_writes_v1'"
            ).fetchone()
            if legacy is not None:
                self._conn.execute(
                    """INSERT OR IGNORE INTO checkpoint_writes_v2
                       (thread_id, namespace, checkpoint_id, task_id, write_index, write_json)
                       SELECT thread_id, '', checkpoint_id, task_id, write_index, write_json
                       FROM checkpoint_writes_v1"""
                )
                self._conn.execute("DROP TABLE checkpoint_writes_v1")

    @classmethod
    def from_conn_string(cls, database: str) -> SqliteSaver:
        return cls(database)

    def put(
        self,
        config: Mapping[str, Any],
        checkpoint: Checkpoint,
        metadata: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        thread_id = _thread_id(config)
        stored_config = {
            **dict(config),
            "configurable": {
                **dict(config.get("configurable", {})),
                "thread_id": thread_id,
                "checkpoint_id": checkpoint.id,
                "checkpoint_ns": "|".join(checkpoint.namespace),
            },
        }
        namespace = "|".join(checkpoint.namespace)
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT OR IGNORE INTO checkpoints_v1
                (thread_id, checkpoint_id, namespace, ts, step,
                 config_json, checkpoint_json, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    thread_id,
                    checkpoint.id,
                    namespace,
                    checkpoint.ts,
                    checkpoint.step,
                    self._serializer.dumps(stored_config),
                    self._serializer.dumps(checkpoint),
                    self._serializer.dumps(dict(metadata)),
                ),
            )
        return stored_config

    def get_tuple(self, config: Mapping[str, Any]) -> CheckpointTuple | None:
        thread_id = _thread_id(config)
        configurable = config.get("configurable", {})
        checkpoint_id = configurable.get("checkpoint_id")
        namespace = str(configurable.get("checkpoint_ns", ""))
        with self._lock:
            if checkpoint_id is None:
                row = self._conn.execute(
                    """SELECT config_json, checkpoint_json, metadata_json
                       FROM checkpoints_v1
                       WHERE thread_id = ? AND namespace = ?
                       ORDER BY seq DESC LIMIT 1""",
                    (thread_id, namespace),
                ).fetchone()
            else:
                row = self._conn.execute(
                    """SELECT config_json, checkpoint_json, metadata_json
                       FROM checkpoints_v1
                       WHERE thread_id = ? AND namespace = ? AND checkpoint_id = ?""",
                    (thread_id, namespace, str(checkpoint_id)),
                ).fetchone()
        return self._row_to_tuple(row) if row is not None else None

    def list(self, config: Mapping[str, Any]) -> Iterable[CheckpointTuple]:
        thread_id = _thread_id(config)
        namespace = str(config.get("configurable", {}).get("checkpoint_ns", ""))
        with self._lock:
            rows = self._conn.execute(
                """SELECT config_json, checkpoint_json, metadata_json
                   FROM checkpoints_v1
                   WHERE thread_id = ? AND namespace = ? ORDER BY seq DESC""",
                (thread_id, namespace),
            ).fetchall()
        return tuple(self._row_to_tuple(row) for row in rows)

    def put_writes(
        self,
        config: Mapping[str, Any],
        checkpoint_id: str,
        writes: Iterable[PendingWrite],
    ) -> None:
        thread_id = _thread_id(config)
        namespace = str(config.get("configurable", {}).get("checkpoint_ns", ""))
        rows = [
            (
                thread_id,
                namespace,
                checkpoint_id,
                write.task_id,
                write.index,
                self._serializer.dumps(write),
            )
            for write in writes
        ]
        if not rows:
            return
        with self._lock, self._conn:
            self._conn.executemany(
                """INSERT OR REPLACE INTO checkpoint_writes_v2
                   (thread_id, namespace, checkpoint_id, task_id, write_index, write_json)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                rows,
            )

    def get_writes(
        self, config: Mapping[str, Any], checkpoint_id: str
    ) -> Iterable[PendingWrite]:
        thread_id = _thread_id(config)
        namespace = str(config.get("configurable", {}).get("checkpoint_ns", ""))
        with self._lock:
            rows = self._conn.execute(
                """SELECT write_json FROM checkpoint_writes_v2
                   WHERE thread_id = ? AND namespace = ? AND checkpoint_id = ?
                   ORDER BY write_index, task_id""",
                (thread_id, namespace, checkpoint_id),
            ).fetchall()
        result: list[PendingWrite] = []
        for row in rows:
            value = self._serializer.loads(row[0])
            result.append(
                PendingWrite(
                    checkpoint_id=str(value["checkpoint_id"]),
                    task_id=str(value["task_id"]),
                    index=int(value["index"]),
                    values=dict(value.get("values", {})),
                    task_path=tuple(value.get("task_path", ())),
                    goto=tuple(
                        item if isinstance(item, Send) else Send(str(item["node"]), item.get("arg"))
                        if isinstance(item, Mapping) and "node" in item
                        else str(item)
                        for item in value.get("goto", ())
                    ),
                    error=value.get("error"),
                )
            )
        return tuple(result)

    def delete_thread(self, config: Mapping[str, Any]) -> None:
        thread_id = _thread_id(config)
        with self._lock, self._conn:
            self._conn.execute(
                "DELETE FROM checkpoint_writes_v2 WHERE thread_id = ?", (thread_id,)
            )
            self._conn.execute(
                "DELETE FROM checkpoints_v1 WHERE thread_id = ?", (thread_id,)
            )

    def _row_to_tuple(self, row: sqlite3.Row) -> CheckpointTuple:
        return CheckpointTuple(
            config=self._serializer.loads(row[0]),
            checkpoint=_checkpoint(self._serializer.loads(row[1])),
            metadata=self._serializer.loads(row[2]),
        )

    async def aput(self, config, checkpoint, metadata):
        return await asyncio.to_thread(self.put, config, checkpoint, metadata)

    async def aget_tuple(self, config):
        return await asyncio.to_thread(self.get_tuple, config)

    async def alist(self, config):
        for item in await asyncio.to_thread(lambda: tuple(self.list(config))):
            yield item

    async def aput_writes(self, config, checkpoint_id, writes):
        await asyncio.to_thread(self.put_writes, config, checkpoint_id, tuple(writes))

    async def aget_writes(self, config, checkpoint_id):
        return await asyncio.to_thread(self.get_writes, config, checkpoint_id)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> SqliteSaver:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


__all__ = ["SqliteSaver"]
