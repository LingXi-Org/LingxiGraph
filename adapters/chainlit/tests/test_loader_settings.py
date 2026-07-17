from __future__ import annotations

import sys
from types import ModuleType

import pytest
from lingxigraph import END, START, MessagesState, StateGraph

from lingxigraph_chainlit.loader import load_graph
from lingxigraph_chainlit.settings import AdapterSettings


def make_graph():
    builder = StateGraph(MessagesState).add_node("done", lambda state: {})
    return builder.add_edge(START, "done").add_edge("done", END).compile()


def test_load_graph_accepts_object_and_factory() -> None:
    module = ModuleType("chainlit_adapter_test_graphs")
    module.graph = make_graph()
    module.factory = make_graph
    module.invalid = object()
    sys.modules[module.__name__] = module
    try:
        assert load_graph(f"{module.__name__}:graph") is module.graph
        assert load_graph(f"{module.__name__}:factory").graph_name == "MessagesState"
        with pytest.raises(TypeError):
            load_graph(f"{module.__name__}:invalid")
        with pytest.raises(ValueError):
            load_graph("missing-separator")
    finally:
        sys.modules.pop(module.__name__, None)


def test_settings_parse_safe_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LINGXIGRAPH_CHAINLIT_GRAPH", "app.graph:graph")
    monkeypatch.setenv("LINGXIGRAPH_CHAINLIT_CONTEXT_JSON", '{"tenant":"acme"}')
    settings = AdapterSettings.from_env()
    assert settings.graph_spec == "app.graph:graph"
    assert settings.context == {"tenant": "acme"}
    assert not settings.observability.show_state_updates
    assert not settings.observability.show_tool_io


def test_settings_reject_invalid_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LINGXIGRAPH_CHAINLIT_GRAPH", raising=False)
    with pytest.raises(RuntimeError):
        AdapterSettings.from_env()
    monkeypatch.setenv("LINGXIGRAPH_CHAINLIT_GRAPH", "app:graph")
    monkeypatch.setenv("LINGXIGRAPH_CHAINLIT_SHOW_TOOL_IO", "sometimes")
    with pytest.raises(ValueError):
        AdapterSettings.from_env()

