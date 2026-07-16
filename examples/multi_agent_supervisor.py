"""Deterministic supervisor, two workers, and durable human approval."""

from __future__ import annotations

import operator
import sys
from pathlib import Path
from typing import Annotated, TypedDict

# Allow running directly from a source checkout without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from lingxigraph import END, START, Command, InMemorySaver, StateGraph, interrupt


class TeamState(TypedDict):
    messages: Annotated[list[str], operator.add]
    approved: bool


def supervisor(state: TeamState) -> dict:
    return {"messages": ["supervisor: dispatch both specialists"]}


def researcher(state: TeamState) -> dict:
    return {"messages": ["researcher: runtime is standard-library only"]}


def writer(state: TeamState) -> dict:
    return {"messages": ["writer: drafted the release summary"]}


def approval(state: TeamState) -> dict:
    accepted = interrupt(
        {"question": "Approve the combined report?", "messages": state["messages"]}
    )
    return {
        "approved": bool(accepted),
        "messages": [f"human: {'approved' if accepted else 'rejected'}"],
    }


builder = StateGraph(TeamState)
builder.add_node("supervisor", supervisor)
builder.add_node("researcher", researcher)
builder.add_node("writer", writer)
builder.add_node("approval", approval)
builder.add_edge(START, "supervisor")
builder.add_edge("supervisor", "researcher")
builder.add_edge("supervisor", "writer")
builder.add_edge(["researcher", "writer"], "approval")
builder.add_edge("approval", END)

graph = builder.compile(checkpointer=InMemorySaver())
config = {"configurable": {"thread_id": "supervisor-example"}}

paused = graph.invoke({"messages": [], "approved": False}, config)
request = paused["__interrupt__"][0]
print("Paused:", request.value["question"])
for message in request.value["messages"]:
    print(" -", message)

completed = graph.invoke(Command(resume=True), config)
print("Completed:", completed["approved"])
print("Final messages:", completed["messages"])
