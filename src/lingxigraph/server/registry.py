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
        self._graphs: dict[tuple[str, str], CompiledGraph] = {}
        self._latest: dict[str, str] = {}
        for graph_id, graph in (graphs or {}).items():
            self.register(graph_id, graph)

    @classmethod
    def from_manifest(cls, path: str | Path = "lingxigraph.json") -> GraphRegistry:
        manifest_path = Path(path)
        value = json.loads(manifest_path.read_text(encoding="utf-8"))
        definitions = value.get("graphs")
        if not isinstance(definitions, Mapping) or not definitions:
            raise ValueError("lingxigraph.json must define at least one graph")
        registry = cls()
        for graph_id, raw_definition in definitions.items():
            version_definitions = (
                raw_definition if isinstance(raw_definition, list) else [raw_definition]
            )
            if not version_definitions:
                raise ValueError(f"graph {graph_id!r} must define at least one version")
            for definition in version_definitions:
                if not isinstance(definition, (str, Mapping)):
                    raise ValueError(
                        f"graph {graph_id!r} version must be an import string or object"
                    )
                import_path = (
                    definition if isinstance(definition, str) else definition.get("path")
                )
                if not isinstance(import_path, str) or ":" not in import_path:
                    raise ValueError(
                        f"graph {graph_id!r} must use 'module:attribute' import path"
                    )
                module_name, attribute = import_path.split(":", 1)
                module = importlib.import_module(module_name)
                graph = getattr(module, attribute)
                if callable(graph) and not isinstance(graph, CompiledGraph):
                    graph = graph()
                if not isinstance(graph, CompiledGraph):
                    raise TypeError(f"graph {graph_id!r} did not resolve to a CompiledGraph")
                declared_version = (
                    definition.get("version") if isinstance(definition, Mapping) else None
                )
                if declared_version is not None and str(declared_version) != graph.graph_version:
                    raise ValueError(
                        f"graph {graph_id!r} manifest version {declared_version!r} does not "
                        f"match compiled version {graph.graph_version!r}"
                    )
                registry.register(str(graph_id), graph)
        return registry

    def register(self, graph_id: str, graph: CompiledGraph) -> None:
        key = (graph_id, graph.graph_version)
        if not graph_id or key in self._graphs:
            raise ValueError(
                f"duplicate or empty graph version {graph_id!r}@{graph.graph_version!r}"
            )
        self._graphs[key] = graph
        self._latest[graph_id] = graph.graph_version

    def get(self, graph_id: str, graph_version: str | None = None) -> CompiledGraph:
        selected = graph_version or self._latest.get(graph_id)
        try:
            if selected is None:
                raise KeyError(graph_id)
            return self._graphs[(graph_id, selected)]
        except KeyError as exc:
            suffix = f"@{graph_version}" if graph_version is not None else ""
            raise KeyError(f"unknown graph {graph_id!r}{suffix}") from exc

    def list(self) -> list[GraphInfo]:
        return [
            self.info(graph_id, version)
            for graph_id, version in sorted(self._graphs)
        ]

    def info(self, graph_id: str, graph_version: str | None = None) -> GraphInfo:
        graph = self.get(graph_id, graph_version)
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
        return graph_id in self._latest


__all__ = ["GraphRegistry"]
