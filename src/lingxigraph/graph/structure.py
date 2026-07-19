"""Serializable graph structure and dependency-free Mermaid rendering."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class NodeInfo:
    id: str
    metadata: dict[str, Any] = field(default_factory=dict)
    is_subgraph: bool = False
    #: Structural kind used by tooling to explain the compiled graph:
    #: "start", "end", "node", or "subgraph".
    kind: str = "node"
    #: Execution semantics surfaced for debugging (retry, timeout, cache, …).
    debug: dict[str, Any] = field(default_factory=dict)
    #: Namespace path when this node is expanded from a nested subgraph.
    namespace: tuple[str, ...] = ()
    #: Recursively expanded subgraph topology (populated only under xray).
    subgraph: "GraphInfo | None" = None


@dataclass(frozen=True, slots=True)
class EdgeInfo:
    source: str
    target: str
    conditional: bool = False
    label: str | None = None
    namespace: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class GraphInfo:
    nodes: tuple[NodeInfo, ...]
    edges: tuple[EdgeInfo, ...]

    def draw_mermaid(self, *, xray: bool = False) -> str:
        lines = ["flowchart TD"]
        self._emit_mermaid(lines, xray=xray)
        return "\n".join(lines)

    def _emit_mermaid(self, lines: list[str], *, xray: bool, prefix: str = "") -> None:
        for node in self.nodes:
            safe = self._safe(prefix + node.id)
            if xray and node.subgraph is not None:
                lines.append(f'    subgraph {safe}["{node.id}"]')
                node.subgraph._emit_mermaid(lines, xray=xray, prefix=f"{prefix}{node.id}.")
                lines.append("    end")
                continue
            shape = f'(["{node.id}"])' if node.id in {"__start__", "__end__"} else f'["{node.id}"]'
            lines.append(f"    {safe}{shape}")
        for edge in self.edges:
            arrow = "-.->" if edge.conditional else "-->"
            label = f'|"{edge.label}"|' if edge.label else ""
            lines.append(
                f"    {self._safe(prefix + edge.source)} {arrow}{label} "
                f"{self._safe(prefix + edge.target)}"
            )

    @staticmethod
    def _safe(value: str) -> str:
        return "n_" + "".join(character if character.isalnum() else "_" for character in value)


__all__ = ["EdgeInfo", "GraphInfo", "NodeInfo"]
