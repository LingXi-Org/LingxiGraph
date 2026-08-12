"""Stable tool catalog schemas and catalog drift diagnostics."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Literal

from ..tools import ToolSpec, as_tool_spec
from .canonical import canonicalize_schema, fingerprint

ToolCatalogDriftKind = Literal["none", "additive", "breaking", "removed", "schema_changed"]


def tool_schema(value: ToolSpec | Mapping[str, Any] | Any) -> dict[str, Any]:
    raw = value if isinstance(value, Mapping) else as_tool_spec(value).as_function_schema()
    canonical = canonicalize_schema(raw)
    if not isinstance(canonical, dict):
        raise TypeError("tool schema must be a JSON object")
    return canonical


def tool_name(value: Mapping[str, Any]) -> str:
    function = value.get("function")
    if isinstance(function, Mapping) and function.get("name") is not None:
        return str(function["name"])
    if value.get("name") is not None:
        return str(value["name"])
    raise ValueError("tool schema is missing a name")


def canonicalize_tools(
    tools: Sequence[ToolSpec | Mapping[str, Any] | Any],
) -> tuple[dict[str, Any], ...]:
    schemas = [tool_schema(item) for item in tools]
    schemas.sort(key=tool_name)
    names = [tool_name(item) for item in schemas]
    if len(set(names)) != len(names):
        duplicates = sorted({name for name in names if names.count(name) > 1})
        raise ValueError("tool names must be unique: " + ", ".join(duplicates))
    return tuple(schemas)


@dataclass(frozen=True, slots=True)
class ToolCatalogFingerprint:
    fingerprint: str
    tool_count: int
    tool_names: tuple[str, ...]
    tool_hashes: Mapping[str, str]


def build_tool_catalog_fingerprint(
    tools: Sequence[ToolSpec | Mapping[str, Any] | Any],
) -> ToolCatalogFingerprint:
    schemas = canonicalize_tools(tools)
    names = tuple(tool_name(item) for item in schemas)
    hashes = {
        name: fingerprint(schema, schema=True)
        for name, schema in zip(names, schemas, strict=True)
    }
    return ToolCatalogFingerprint(
        fingerprint=fingerprint(list(schemas), schema=True),
        tool_count=len(schemas),
        tool_names=names,
        tool_hashes=MappingProxyType(hashes),
    )


@dataclass(frozen=True, slots=True)
class ToolCatalogDrift:
    kind: ToolCatalogDriftKind
    previous: ToolCatalogFingerprint | None
    current: ToolCatalogFingerprint
    added: tuple[str, ...] = ()
    removed: tuple[str, ...] = ()
    changed: tuple[str, ...] = ()


def compare_tool_catalogs(
    previous: ToolCatalogFingerprint | None,
    current: ToolCatalogFingerprint,
) -> ToolCatalogDrift:
    if previous is None or previous.fingerprint == current.fingerprint:
        return ToolCatalogDrift("none", previous, current)
    previous_names = set(previous.tool_names)
    current_names = set(current.tool_names)
    added = tuple(sorted(current_names - previous_names))
    removed = tuple(sorted(previous_names - current_names))
    changed = tuple(
        sorted(
            name
            for name in previous_names & current_names
            if previous.tool_hashes.get(name) != current.tool_hashes.get(name)
        )
    )
    if changed:
        kind: ToolCatalogDriftKind = "schema_changed"
    elif removed and added:
        kind = "breaking"
    elif removed:
        kind = "removed"
    else:
        kind = "additive"
    return ToolCatalogDrift(kind, previous, current, added, removed, changed)


__all__ = [
    "ToolCatalogDrift",
    "ToolCatalogFingerprint",
    "build_tool_catalog_fingerprint",
    "canonicalize_tools",
    "compare_tool_catalogs",
    "tool_name",
    "tool_schema",
]
