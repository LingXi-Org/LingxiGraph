"""Deterministic JSON and tool-schema canonicalization."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from typing import Any


def canonicalize(value: Any, *, schema: bool = False) -> Any:
    """Return a JSON-compatible value with deterministic mapping order.

    Object key order is not semantically meaningful for JSON Schema or tool
    arguments.  Array order is preserved except for schema fields whose
    semantics are explicitly set-like (currently ``required``).
    """

    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("canonical JSON does not accept NaN or infinite floats")
        return value
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key in sorted(value, key=lambda item: str(item)):
            if not isinstance(key, str):
                raise TypeError("canonical JSON mappings must use string keys")
            result[key] = canonicalize(value[key], schema=schema)
            if schema and key == "required" and isinstance(result[key], list):
                if all(isinstance(item, str) for item in result[key]):
                    result[key] = sorted(result[key])
        return result
    if isinstance(value, (list, tuple)):
        items = [canonicalize(item, schema=schema) for item in value]
        if schema:
            # ``required`` is a set of property names.  Keep all other arrays
            # in their supplied order because allOf/anyOf/enum may be used by
            # providers as ordered prompt material.
            return items
        return items
    if isinstance(value, (set, frozenset)):
        items = [canonicalize(item, schema=schema) for item in value]
        return sorted(items, key=lambda item: canonical_json(item))
    raise TypeError(f"unsupported value for canonical JSON: {type(value).__name__}")


def canonicalize_schema(value: Any) -> Any:
    """Canonicalize JSON Schema, including set-like ``required`` arrays."""

    def walk(item: Any, key: str | None = None) -> Any:
        if isinstance(item, Mapping):
            result: dict[str, Any] = {}
            for child_key in sorted(item, key=lambda candidate: str(candidate)):
                if not isinstance(child_key, str):
                    raise TypeError("JSON Schema mappings must use string keys")
                result[child_key] = walk(item[child_key], child_key)
            return result
        if isinstance(item, (list, tuple)):
            values = [walk(child) for child in item]
            if key == "required" and all(isinstance(child, str) for child in values):
                return sorted(values)
            return values
        if isinstance(item, (set, frozenset)):
            return sorted(
                (walk(child) for child in item), key=lambda child: canonical_json(child)
            )
        if item is None or isinstance(item, (bool, int, str)):
            return item
        if isinstance(item, float):
            if not math.isfinite(item):
                raise ValueError("JSON Schema cannot contain NaN or infinite floats")
            return item
        raise TypeError(f"unsupported JSON Schema value: {type(item).__name__}")

    return walk(value)


def canonical_json(value: Any) -> str:
    return json.dumps(
        canonicalize(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    )


def schema_json(value: Any) -> str:
    return json.dumps(
        canonicalize_schema(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    )


def fingerprint(value: Any, *, schema: bool = False) -> str:
    raw = schema_json(value) if schema else canonical_json(value)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


__all__ = [
    "canonical_json",
    "canonicalize",
    "canonicalize_schema",
    "fingerprint",
    "schema_json",
]
