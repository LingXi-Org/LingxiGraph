"""PostgreSQL production checkpointer with pending-write recovery."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Mapping
from typing import Any

from ..serialization import JsonSerializer
from ..types import Send
from . import CheckpointTuple, PendingWrite
from .sqlite import _checkpoint


def _scope(config: Mapping[str, Any]) -> tuple[str, str, str]:
    configurable = config.get("configurable")
    if not isinstance(configurable, Mapping) or not configurable.get("thread_id"):
        raise ValueError("checkpoint operations require config['configurable']['thread_id']")
    tenant_id = str(configurable.get("tenant_id") or config.get("tenant_id") or "default")
    return tenant_id, str(configurable["thread_id"]), str(
        configurable.get("checkpoint_ns", "")
    )


class PostgresSaver:
    """Transactional PostgreSQL saver.

    Connections are short-lived by default so this class has no mandatory pool
    dependency.  Deployments may pass a ``connect`` callable backed by their
    own psycopg pool.
    """

    def __init__(
        self,
        dsn: str,
        *,
        schema: str = "lingxigraph",
        serializer: JsonSerializer | None = None,
        connect: Any | None = None,
    ) -> None:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
            raise ValueError("invalid PostgreSQL schema name")
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("install lingxigraph[postgres] to use PostgresSaver") from exc
        self._psycopg = psycopg
        self._dsn = dsn
        self._schema = schema
        self._serializer = serializer or JsonSerializer()
        self._connect_factory = connect

    def _connect(self):
        if self._connect_factory is not None:
            return self._connect_factory()
        return self._psycopg.connect(self._dsn)

    @staticmethod
    def _set_tenant(cursor: Any, tenant_id: str) -> None:
        cursor.execute("SELECT set_config('app.tenant_id', %s, true)", (tenant_id,))

    def setup(self) -> None:
        with self._connect() as conn, conn.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{self._schema}"')
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS "{self._schema}".checkpoints (
                    seq BIGSERIAL PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    thread_id TEXT NOT NULL,
                    namespace TEXT NOT NULL DEFAULT '',
                    checkpoint_id TEXT NOT NULL,
                    ts TIMESTAMPTZ NOT NULL,
                    step BIGINT NOT NULL,
                    config_json BYTEA NOT NULL,
                    checkpoint_json BYTEA NOT NULL,
                    metadata_json BYTEA NOT NULL,
                    UNIQUE (tenant_id, thread_id, namespace, checkpoint_id)
                )
                """
            )
            cursor.execute(
                f"""CREATE INDEX IF NOT EXISTS checkpoints_by_thread
                ON "{self._schema}".checkpoints
                (tenant_id, thread_id, namespace, seq DESC)"""
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS "{self._schema}".checkpoint_writes (
                    tenant_id TEXT NOT NULL,
                    thread_id TEXT NOT NULL,
                    namespace TEXT NOT NULL DEFAULT '',
                    checkpoint_id TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    write_index INTEGER NOT NULL,
                    write_json BYTEA NOT NULL,
                    PRIMARY KEY
                    (tenant_id, thread_id, namespace, checkpoint_id, task_id, write_index)
                )
                """
            )

    @classmethod
    def from_conn_string(cls, dsn: str) -> PostgresSaver:
        return cls(dsn)

    def put(self, config, checkpoint, metadata):
        tenant_id, thread_id, namespace = _scope(config)
        stored_config = {
            **dict(config),
            "configurable": {
                **dict(config.get("configurable", {})),
                "tenant_id": tenant_id,
                "thread_id": thread_id,
                "checkpoint_ns": namespace,
                "checkpoint_id": checkpoint.id,
            },
        }
        with self._connect() as conn, conn.cursor() as cursor:
            self._set_tenant(cursor, tenant_id)
            cursor.execute(
                f"""INSERT INTO "{self._schema}".checkpoints
                (tenant_id, thread_id, namespace, checkpoint_id, ts, step,
                 config_json, checkpoint_json, metadata_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tenant_id, thread_id, namespace, checkpoint_id)
                DO NOTHING""",
                (
                    tenant_id,
                    thread_id,
                    namespace,
                    checkpoint.id,
                    checkpoint.ts,
                    checkpoint.step,
                    self._serializer.dumps(stored_config),
                    self._serializer.dumps(checkpoint),
                    self._serializer.dumps(dict(metadata)),
                ),
            )
        return stored_config

    def get_tuple(self, config):
        tenant_id, thread_id, namespace = _scope(config)
        checkpoint_id = config.get("configurable", {}).get("checkpoint_id")
        with self._connect() as conn, conn.cursor() as cursor:
            self._set_tenant(cursor, tenant_id)
            if checkpoint_id is None:
                cursor.execute(
                    f"""SELECT config_json, checkpoint_json, metadata_json
                    FROM "{self._schema}".checkpoints
                    WHERE tenant_id=%s AND thread_id=%s AND namespace=%s
                    ORDER BY seq DESC LIMIT 1""",
                    (tenant_id, thread_id, namespace),
                )
            else:
                cursor.execute(
                    f"""SELECT config_json, checkpoint_json, metadata_json
                    FROM "{self._schema}".checkpoints
                    WHERE tenant_id=%s AND thread_id=%s AND namespace=%s
                      AND checkpoint_id=%s""",
                    (tenant_id, thread_id, namespace, str(checkpoint_id)),
                )
            row = cursor.fetchone()
        return self._row(row) if row is not None else None

    def list(self, config):
        tenant_id, thread_id, namespace = _scope(config)
        with self._connect() as conn, conn.cursor() as cursor:
            self._set_tenant(cursor, tenant_id)
            cursor.execute(
                f"""SELECT config_json, checkpoint_json, metadata_json
                FROM "{self._schema}".checkpoints
                WHERE tenant_id=%s AND thread_id=%s AND namespace=%s
                ORDER BY seq DESC""",
                (tenant_id, thread_id, namespace),
            )
            rows = cursor.fetchall()
        return tuple(self._row(row) for row in rows)

    def put_writes(self, config, checkpoint_id, writes):
        tenant_id, thread_id, namespace = _scope(config)
        rows = [
            (
                tenant_id,
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
        with self._connect() as conn, conn.cursor() as cursor:
            self._set_tenant(cursor, tenant_id)
            cursor.executemany(
                f"""INSERT INTO "{self._schema}".checkpoint_writes
                (tenant_id, thread_id, namespace, checkpoint_id,
                 task_id, write_index, write_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT
                (tenant_id, thread_id, namespace, checkpoint_id, task_id, write_index)
                DO UPDATE SET write_json=EXCLUDED.write_json""",
                rows,
            )

    def get_writes(self, config, checkpoint_id):
        tenant_id, thread_id, namespace = _scope(config)
        with self._connect() as conn, conn.cursor() as cursor:
            self._set_tenant(cursor, tenant_id)
            cursor.execute(
                f"""SELECT write_json FROM "{self._schema}".checkpoint_writes
                WHERE tenant_id=%s AND thread_id=%s AND namespace=%s
                  AND checkpoint_id=%s ORDER BY write_index, task_id""",
                (tenant_id, thread_id, namespace, checkpoint_id),
            )
            rows = cursor.fetchall()
        writes: list[PendingWrite] = []
        for row in rows:
            value = self._serializer.loads(bytes(row[0]))
            writes.append(
                PendingWrite(
                    checkpoint_id=str(value["checkpoint_id"]),
                    task_id=str(value["task_id"]),
                    index=int(value["index"]),
                    values=dict(value.get("values", {})),
                    task_path=tuple(value.get("task_path", ())),
                    goto=tuple(
                        Send(str(item["node"]), item.get("arg"))
                        if isinstance(item, Mapping) and "node" in item
                        else str(item)
                        for item in value.get("goto", ())
                    ),
                    error=value.get("error"),
                )
            )
        return tuple(writes)

    def delete_thread(self, config):
        tenant_id, thread_id, _namespace = _scope(config)
        with self._connect() as conn, conn.cursor() as cursor:
            self._set_tenant(cursor, tenant_id)
            cursor.execute(
                f"""DELETE FROM "{self._schema}".checkpoint_writes
                WHERE tenant_id=%s AND thread_id=%s""",
                (tenant_id, thread_id),
            )
            cursor.execute(
                f"""DELETE FROM "{self._schema}".checkpoints
                WHERE tenant_id=%s AND thread_id=%s""",
                (tenant_id, thread_id),
            )

    def _row(self, row) -> CheckpointTuple:
        return CheckpointTuple(
            config=self._serializer.loads(bytes(row[0])),
            checkpoint=_checkpoint(self._serializer.loads(bytes(row[1]))),
            metadata=self._serializer.loads(bytes(row[2])),
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


AsyncPostgresSaver = PostgresSaver

__all__ = ["AsyncPostgresSaver", "PostgresSaver"]
