"""Environment configuration for the packaged Chainlit host."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

from .models import ObservabilityOptions


def _boolean(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean")


def _mapping(name: str) -> dict[str, Any]:
    value = os.getenv(name)
    if not value:
        return {}
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise ValueError(f"{name} must contain a JSON object")
    return parsed


@dataclass(frozen=True, slots=True)
class AdapterSettings:
    graph_spec: str
    sqlite_path: str = ".chainlit/lingxigraph.db"
    context: dict[str, Any] | None = None
    observability: ObservabilityOptions = ObservabilityOptions()

    @classmethod
    def from_env(cls) -> AdapterSettings:
        graph_spec = os.getenv("LINGXIGRAPH_CHAINLIT_GRAPH", "").strip()
        if not graph_spec:
            raise RuntimeError("LINGXIGRAPH_CHAINLIT_GRAPH must be set to module:attribute")
        limit_value = os.getenv("LINGXIGRAPH_CHAINLIT_MAX_PAYLOAD_CHARS", "4000")
        try:
            limit = int(limit_value)
        except ValueError as exc:
            raise ValueError("LINGXIGRAPH_CHAINLIT_MAX_PAYLOAD_CHARS must be an integer") from exc
        return cls(
            graph_spec=graph_spec,
            sqlite_path=os.getenv(
                "LINGXIGRAPH_CHAINLIT_SQLITE_PATH", ".chainlit/lingxigraph.db"
            ),
            context=_mapping("LINGXIGRAPH_CHAINLIT_CONTEXT_JSON"),
            observability=ObservabilityOptions(
                show_state_updates=_boolean("LINGXIGRAPH_CHAINLIT_SHOW_STATE_UPDATES"),
                show_custom_payloads=_boolean("LINGXIGRAPH_CHAINLIT_SHOW_CUSTOM_PAYLOADS"),
                show_tool_io=_boolean("LINGXIGRAPH_CHAINLIT_SHOW_TOOL_IO"),
                max_payload_chars=limit,
                default_open=_boolean("LINGXIGRAPH_CHAINLIT_DEFAULT_OPEN"),
            ),
        )


__all__ = ["AdapterSettings"]

