"""Map-reduce with the Send API: fan out per-item workers, then reduce."""

from __future__ import annotations

import operator
import sys
from pathlib import Path
from typing import Annotated, TypedDict

# Allow running directly from a source checkout without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from lingxigraph import END, START, Send, StateGraph


class MapState(TypedDict, total=False):
    topics: list[str]
    summaries: Annotated[list[str], operator.add]
    report: str


def plan(state: MapState) -> dict:
    print(f"planner: dispatching {len(state['topics'])} workers")
    return {}


def fan_out(state: MapState) -> list[Send]:
    # Each Send gives one worker task a private input instead of the shared state.
    return [Send("summarize", {"topic": topic}) for topic in state["topics"]]


def summarize(payload: dict) -> dict:
    return {"summaries": [f"summary of {payload['topic']}"]}


def reduce_report(state: MapState) -> dict:
    return {"report": " | ".join(state["summaries"])}


builder = StateGraph(MapState)
builder.add_node("plan", plan)
builder.add_node("summarize", summarize)
builder.add_node("reduce", reduce_report)
builder.add_edge(START, "plan")
builder.add_conditional_edges("plan", fan_out)
builder.add_edge("summarize", "reduce")
builder.add_edge("reduce", END)

graph = builder.compile()
result = graph.invoke({"topics": ["pregel", "checkpoints", "interrupts"]})
print("Report:", result["report"])
