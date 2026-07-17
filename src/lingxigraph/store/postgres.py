"""Tenant-scoped PostgreSQL long-term memory store."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Sequence
from typing import Any

from . import Item, StoreOperation, _validate_namespace


class PostgresStore:
    def __init__(
        self,
        dsn: str,
        *,
        tenant_id: str,
        schema: str = "lingxigraph",
        connect: Any | None = None,
    ) -> None:
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
            raise ValueError("invalid PostgreSQL schema name")
        try:
            import psycopg
            from psycopg.types.json import Jsonb
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("install lingxigraph[postgres] to use PostgresStore") from exc
        self._psycopg = psycopg
        self._jsonb = Jsonb
        self._dsn = dsn
        self._tenant_id = tenant_id
        self._schema = schema
        self._connect_factory = connect

    def _connect(self):
        if self._connect_factory is not None:
            return self._connect_factory()
        return self._psycopg.connect(self._dsn)

    def _set_tenant(self, cursor: Any) -> None:
        cursor.execute(
            "SELECT set_config('app.tenant_id', %s, true)", (self._tenant_id,)
        )

    def for_tenant(self, tenant_id: str) -> PostgresStore:
        return PostgresStore(
            self._dsn,
            tenant_id=tenant_id,
            schema=self._schema,
            connect=self._connect_factory,
        )

    def setup(self) -> None:
        with self._connect() as conn, conn.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{self._schema}"')
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS "{self._schema}".store_items (
                    tenant_id TEXT NOT NULL,
                    namespace TEXT[] NOT NULL,
                    key TEXT NOT NULL,
                    value JSONB NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    expires_at TIMESTAMPTZ,
                    PRIMARY KEY (tenant_id, namespace, key)
                )
                """
            )
            cursor.execute(
                f'ALTER TABLE "{self._schema}".store_items '
                "ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ"
            )
            cursor.execute(
                f"""CREATE INDEX IF NOT EXISTS store_items_namespace
                ON "{self._schema}".store_items USING GIN (namespace)"""
            )
            cursor.execute(
                f"""CREATE INDEX IF NOT EXISTS store_items_value
                ON "{self._schema}".store_items USING GIN (value)"""
            )

    def put(self, namespace, key, value, *, ttl=None):
        namespace = _validate_namespace(namespace)
        if not key:
            raise ValueError("store key must be non-empty")
        if ttl is not None and ttl <= 0:
            raise ValueError("ttl must be positive")
        with self._connect() as conn, conn.cursor() as cursor:
            self._set_tenant(cursor)
            cursor.execute(
                f"""INSERT INTO "{self._schema}".store_items
                (tenant_id, namespace, key, value, expires_at)
                VALUES (%s, %s, %s, %s, CASE WHEN %s IS NULL THEN NULL ELSE NOW() + (%s * INTERVAL '1 second') END)
                ON CONFLICT (tenant_id, namespace, key) DO UPDATE
                SET value=EXCLUDED.value, updated_at=NOW(), expires_at=EXCLUDED.expires_at""",
                (self._tenant_id, list(namespace), key, self._jsonb(dict(value)), ttl, ttl),
            )

    def get(self, namespace, key):
        namespace = _validate_namespace(namespace)
        with self._connect() as conn, conn.cursor() as cursor:
            self._set_tenant(cursor)
            cursor.execute(
                f"""SELECT namespace, key, value, created_at, updated_at, expires_at
                FROM "{self._schema}".store_items
                WHERE tenant_id=%s AND namespace=%s AND key=%s
                  AND (expires_at IS NULL OR expires_at > NOW())""",
                (self._tenant_id, list(namespace), key),
            )
            row = cursor.fetchone()
        return self._item(row) if row is not None else None

    def delete(self, namespace, key):
        namespace = _validate_namespace(namespace)
        with self._connect() as conn, conn.cursor() as cursor:
            self._set_tenant(cursor)
            cursor.execute(
                f"""DELETE FROM "{self._schema}".store_items
                WHERE tenant_id=%s AND namespace=%s AND key=%s""",
                (self._tenant_id, list(namespace), key),
            )

    def search(
        self,
        namespace_prefix,
        *,
        query=None,
        filter=None,
        limit=10,
        offset=0,
    ):
        prefix = tuple(namespace_prefix)
        if limit < 1 or offset < 0:
            raise ValueError("limit must be positive and offset non-negative")
        clauses = ["tenant_id=%s", "(expires_at IS NULL OR expires_at > NOW())"]
        params: list[Any] = [self._tenant_id]
        if prefix:
            clauses.append(f"namespace[1:{len(prefix)}]=%s")
            params.append(list(prefix))
        if query:
            clauses.append("value::text ILIKE %s")
            params.append(f"%{query}%")
        if filter:
            clauses.append("value @> %s")
            params.append(self._jsonb(dict(filter)))
        params.extend((limit, offset))
        with self._connect() as conn, conn.cursor() as cursor:
            self._set_tenant(cursor)
            cursor.execute(
                f"""SELECT namespace, key, value, created_at, updated_at, expires_at
                FROM "{self._schema}".store_items
                WHERE {' AND '.join(clauses)}
                ORDER BY updated_at DESC, key LIMIT %s OFFSET %s""",
                params,
            )
            rows = cursor.fetchall()
        return [self._item(row) for row in rows]

    def list_namespaces(self, *, prefix=()):
        prefix = tuple(prefix)
        clauses = ["tenant_id=%s"]
        params: list[Any] = [self._tenant_id]
        if prefix:
            clauses.append(f"namespace[1:{len(prefix)}]=%s")
            params.append(list(prefix))
        with self._connect() as conn, conn.cursor() as cursor:
            self._set_tenant(cursor)
            cursor.execute(
                f"""SELECT DISTINCT namespace
                FROM "{self._schema}".store_items
                WHERE {' AND '.join(clauses)} ORDER BY namespace""",
                params,
            )
            return [tuple(row[0]) for row in cursor.fetchall()]

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

    @staticmethod
    def _item(row) -> Item:
        return Item(
            namespace=tuple(row[0]),
            key=str(row[1]),
            value=dict(row[2]),
            created_at=row[3].isoformat(),
            updated_at=row[4].isoformat(),
            expires_at=row[5].isoformat() if len(row) > 5 and row[5] is not None else None,
        )

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


AsyncPostgresStore = PostgresStore

__all__ = ["AsyncPostgresStore", "PostgresStore"]
