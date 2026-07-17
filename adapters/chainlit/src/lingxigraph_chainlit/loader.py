"""Trusted module-path graph loader used by the standalone host."""

from __future__ import annotations

import importlib

from lingxigraph import CompiledGraph, CompiledStateGraph


def load_graph(spec: str) -> CompiledGraph:
    """Load ``module:attribute`` resolving an object or a zero-argument factory."""

    if not spec or ":" not in spec:
        raise ValueError("graph must use a non-empty 'module:attribute' import path")
    module_name, attribute = spec.split(":", 1)
    if not module_name or not attribute:
        raise ValueError("graph must use a non-empty 'module:attribute' import path")
    module = importlib.import_module(module_name)
    value = getattr(module, attribute)
    if callable(value) and not isinstance(value, CompiledStateGraph):
        value = value()
    if not isinstance(value, CompiledStateGraph):
        raise TypeError(f"{spec!r} did not resolve to a CompiledGraph")
    return value


__all__ = ["load_graph"]
