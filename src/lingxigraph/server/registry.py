"""Trusted graph-package registry loaded from ``lingxigraph.json``."""

from __future__ import annotations

import importlib
import json
from collections.abc import Mapping
from pathlib import Path

from ..graph import CompiledGraph
from ..schema import SchemaAdapter
from .models import GraphInfo


class GraphRegistry:
    def __init__(self, graphs: Mapping[str, CompiledGraph] | None = None) -> None:
        self._graphs: dict[str, CompiledGraph] = dict(graphs or {})

    @classmethod
    def from_manifest(cls, path: str | Path = "lingxigraph.json") -> GraphRegistry:
        manifest_path = Path(path)
        value = json.loads(manifest_path.read_text(encoding="utf-8"))
        definitions = value.get("graphs")
        if not isinstance(definitions, Mapping) or not definitions:
            raise ValueError("lingxigraph.json must define at least one graph")
        registry = cls()
        for graph_id, definition in definitions.items():
            import_path = definition if isinstance(definition, str) else definition.get("path")
            if not import_path or ":" not in import_path:
                raise ValueError(f"graph {graph_id!r} must use 'module:attribute' import path")
            module_name, attribute = import_path.split(":", 1)
            module = importlib.import_module(module_name)
            graph = getattr(module, attribute)
            if callable(graph) and not isinstance(graph, CompiledGraph):
                graph = graph()
            if not isinstance(graph, CompiledGraph):
                raise TypeError(f"graph {graph_id!r} did not resolve to a CompiledGraph")
            registry.register(str(graph_id), graph)
        return registry

    def register(self, graph_id: str, graph: CompiledGraph) -> None:
        if not graph_id or graph_id in self._graphs:
            raise ValueError(f"duplicate or empty graph id {graph_id!r}")
        self._graphs[graph_id] = graph

    def get(self, graph_id: str) -> CompiledGraph:
        try:
            return self._graphs[graph_id]
        except KeyError as exc:
            raise KeyError(f"unknown graph {graph_id!r}") from exc

    def list(self) -> list[GraphInfo]:
        return [self.info(graph_id) for graph_id in sorted(self._graphs)]

    def info(self, graph_id: str) -> GraphInfo:
        graph = self.get(graph_id)
        return GraphInfo(
            id=graph_id,
            version=graph.graph_version,
            schema_hash=graph.schema_hash,
            input_schema=SchemaAdapter(graph.input_schema).json_schema(),
            output_schema=SchemaAdapter(graph.output_schema).json_schema(),
            context_schema=(
                SchemaAdapter(graph.context_schema).json_schema()
                if graph.context_schema is not None
                else None
            ),
        )

    def __contains__(self, graph_id: str) -> bool:
        return graph_id in self._graphs


__all__ = ["GraphRegistry"]
