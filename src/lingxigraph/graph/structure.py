"""Serializable graph structure and dependency-free Mermaid rendering."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class NodeInfo:
    id: str
    metadata: dict[str, Any] = field(default_factory=dict)
    is_subgraph: bool = False


@dataclass(frozen=True, slots=True)
class EdgeInfo:
    source: str
    target: str
    conditional: bool = False
    label: str | None = None


@dataclass(frozen=True, slots=True)
class GraphInfo:
    nodes: tuple[NodeInfo, ...]
    edges: tuple[EdgeInfo, ...]

    def draw_mermaid(self) -> str:
        lines = ["flowchart TD"]
        for node in self.nodes:
            shape = f'(["{node.id}"])' if node.id in {"__start__", "__end__"} else f'["{node.id}"]'
            lines.append(f"    {self._safe(node.id)}{shape}")
        for edge in self.edges:
            arrow = "-.->" if edge.conditional else "-->"
            label = f'|"{edge.label}"|' if edge.label else ""
            lines.append(
                f"    {self._safe(edge.source)} {arrow}{label} {self._safe(edge.target)}"
            )
        return "\n".join(lines)

    @staticmethod
    def _safe(value: str) -> str:
        return "n_" + "".join(character if character.isalnum() else "_" for character in value)


__all__ = ["EdgeInfo", "GraphInfo", "NodeInfo"]
