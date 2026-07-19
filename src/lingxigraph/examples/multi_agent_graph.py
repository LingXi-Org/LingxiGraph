"""A self-contained multi-agent showcase graph with a nested subgraph.

This graph is model-provider neutral: every "agent" is a plain Python callable,
so it runs with no LLM SDK installed. It exists to exercise the multi-agent
runtime end to end — parallel fan-out, deterministic reduction and a nested
compiled subgraph — and to give the Studio graph explorer real structure to
explain and debug (including X-ray subgraph expansion).

Topology::

    START → intake → research (subgraph) → [analyst ‖ critic] → synthesize → END

``research`` is itself a compiled StateGraph, so ``draw_mermaid(xray=True)`` and
the Studio X-ray toggle expand it in place.
"""

from __future__ import annotations

from typing import Annotated, Any, TypedDict

from lingxigraph import END, START, Runtime, Send, StateGraph


def _merge_findings(left: list[str], right: list[str]) -> list[str]:
    """Reducer that concatenates findings from parallel agents in commit order."""

    return [*left, *right]


class ResearchState(TypedDict):
    request: str
    sources: list[str]
    brief: str


def _gather(state: ResearchState, runtime: Runtime[Any]) -> dict[str, Any]:
    runtime.emit("progress", {"stage": "gather"})
    request = state["request"]
    return {"sources": [f"doc://{request[:12]}/{i}" for i in range(1, 4)]}


def _summarize(state: ResearchState) -> dict[str, Any]:
    count = len(state.get("sources", []))
    return {"brief": f"collected {count} sources for '{state['request']}'"}


def _build_research_subgraph():
    """A small compiled subgraph that plays the role of a research team."""

    graph = StateGraph(ResearchState, name="research", version="1.0.0")
    graph.add_node("gather", _gather, timeout=20, metadata={"role": "retriever"})
    graph.add_node("summarize", _summarize, metadata={"role": "summarizer"})
    graph.add_edge(START, "gather")
    graph.add_edge("gather", "summarize")
    graph.add_edge("summarize", END)
    return graph.compile()


class State(TypedDict):
    request: str
    sources: list[str]
    brief: str
    findings: Annotated[list[str], _merge_findings]
    report: str


def _intake(state: State, runtime: Runtime[Any]) -> dict[str, Any]:
    runtime.emit("progress", {"stage": "intake"})
    return {"request": state["request"].strip(), "findings": []}


def _fan_out(state: State) -> list[Send]:
    """Route the same brief to two independent reviewer agents in parallel."""

    return [
        Send("analyst", dict(state)),
        Send("critic", dict(state)),
    ]


def _analyst(state: State) -> dict[str, Any]:
    return {"findings": [f"analyst: {state.get('brief', 'no brief')} looks actionable"]}


def _critic(state: State) -> dict[str, Any]:
    return {"findings": [f"critic: verify assumptions behind {len(state.get('sources', []))} sources"]}


def _synthesize(state: State) -> dict[str, Any]:
    findings = state.get("findings", [])
    return {"report": f"{state.get('brief', '')} | " + " ; ".join(findings)}


def build() -> StateGraph:
    builder = StateGraph(State, name="multi-agent-research", version="1.0.0")
    builder.add_node("intake", _intake, metadata={"role": "coordinator"})
    builder.add_node(
        "research",
        _build_research_subgraph(),
        metadata={"role": "research-team", "pattern": "subgraph"},
    )
    builder.add_node("analyst", _analyst, metadata={"role": "reviewer"})
    builder.add_node("critic", _critic, metadata={"role": "reviewer"})
    builder.add_node("synthesize", _synthesize, metadata={"role": "editor"})

    builder.add_edge(START, "intake")
    builder.add_edge("intake", "research")
    builder.add_conditional_edges(
        "research", _fan_out, {"analyst": "analyst", "critic": "critic"}
    )
    builder.add_edge(("analyst", "critic"), "synthesize")
    builder.add_edge("synthesize", END)
    return builder


graph = build().compile()

__all__ = ["build", "graph"]
