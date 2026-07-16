"""A compiled subgraph as a parent node, with durable approval inside it."""

from __future__ import annotations

import operator
from pathlib import Path
import sys
from typing import Annotated, TypedDict

# Allow running directly from a source checkout without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from lingxigraph import END, START, Command, InMemorySaver, StateGraph, interrupt


class TeamState(TypedDict, total=False):
    messages: Annotated[list[str], operator.add]
    published: bool


# --- Child graph: draft, then ask a human before handing back to the parent.
def draft(state: TeamState) -> dict:
    return {"messages": ["team: draft ready"]}


def review(state: TeamState) -> dict:
    verdict = interrupt({"question": "Ship the team draft?"})
    return {"messages": [f"team: review={'approved' if verdict else 'rejected'}"]}


team = StateGraph(TeamState)
team.add_node("draft", draft).add_node("review", review)
team.add_edge(START, "draft").add_edge("draft", "review").add_edge("review", END)


# --- Parent graph: the compiled child runs as an ordinary node.
def publish(state: TeamState) -> dict:
    return {"messages": ["parent: published"], "published": True}


parent = StateGraph(TeamState)
parent.add_node("team", team.compile())
parent.add_node("publish", publish)
parent.add_edge(START, "team").add_edge("team", "publish").add_edge("publish", END)

graph = parent.compile(checkpointer=InMemorySaver())
config = {"configurable": {"thread_id": "team-review-example"}}

paused = graph.invoke({"messages": [], "published": False}, config)
print("Paused inside subgraph:", paused["__interrupt__"][0].value["question"])

finished = graph.invoke(Command(resume=True), config)
for message in finished["messages"]:
    print(" -", message)
print("Published:", finished["published"])
