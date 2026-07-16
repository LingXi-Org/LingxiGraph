"""Small trusted graph used by the local production stack."""

from __future__ import annotations

from typing import TypedDict

from lingxigraph import END, START, Runtime, StateGraph


class SupportState(TypedDict):
    request: str
    result: str


class DeploymentContext(TypedDict, total=False):
    department: str


def resolve(state: SupportState, runtime: Runtime[DeploymentContext]):
    department = (runtime.context or {}).get("department", "general")
    runtime.emit("progress", {"stage": "resolved", "department": department})
    return {"result": f"[{department}] accepted: {state['request']}"}


builder = StateGraph(
    SupportState,
    context_schema=DeploymentContext,
    name="production-support",
    version="1.0.0",
)
builder.add_node("resolve", resolve, timeout=30, metadata={"owner": "platform"})
builder.add_edge(START, "resolve")
builder.add_edge("resolve", END)

# Persistence, store and cache are bound by the Worker at deployment time.
graph = builder.compile()
