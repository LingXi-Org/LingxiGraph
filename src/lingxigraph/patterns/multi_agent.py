"""Battle-tested orchestration topologies without model-provider coupling."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from ..constants import END, START
from ..graph import CompiledGraph, StateGraph
from ..runtime import Runtime
from ..types import Command, Send

Agent = Callable[..., Any] | CompiledGraph


@dataclass(frozen=True, slots=True)
class AgentTool:
    """Provider-neutral tool facade around a callable or compiled subgraph."""

    name: str
    action: Agent
    description: str = ""

    async def __call__(
        self, input: Mapping[str, Any], runtime: Runtime[Any] | None = None
    ) -> Any:
        if isinstance(self.action, CompiledGraph):
            config = runtime.config if runtime is not None else None
            return await self.action.ainvoke(
                input,
                config,
                context=runtime.context if runtime is not None else None,
                cancellation=runtime.cancellation if runtime is not None else None,
            )
        parameters = inspect.signature(self.action).parameters
        value = self.action(input, runtime) if len(parameters) >= 2 else self.action(input)
        return await value if inspect.isawaitable(value) else value


def build_manager_as_tools(
    state_schema: type,
    manager: Callable[..., Any],
    agents: Mapping[str, Agent],
    *,
    descriptions: Mapping[str, str] | None = None,
) -> StateGraph:
    """Build a manager that owns the conversation and invokes agents as tools.

    ``manager`` receives ``(state, runtime, tools)`` where ``tools`` maps names
    to :class:`AgentTool`. It may invoke zero or more tools and returns a normal
    state update or ``Command``. This keeps tool selection model-provider neutral.
    """

    if not agents:
        raise ValueError("manager-as-tools requires at least one agent")
    tools = {
        name: AgentTool(name, action, (descriptions or {}).get(name, ""))
        for name, action in agents.items()
    }

    async def manager_node(state: Mapping[str, Any], runtime: Runtime[Any]) -> Any:
        value = manager(state, runtime, tools)
        return await value if inspect.isawaitable(value) else value

    graph = StateGraph(state_schema, name="manager-as-tools")
    graph.add_node(
        "manager",
        manager_node,
        metadata={
            "pattern": "manager_as_tools",
            "tools": tuple(tools),
        },
    )
    graph.add_edge(START, "manager")
    graph.add_edge("manager", END)
    return graph


def build_supervisor(
    state_schema: type,
    supervisor: Callable[..., Any],
    agents: Mapping[str, Agent],
    *,
    name: str = "supervisor",
) -> StateGraph:
    """Build a manager-owned loop.

    The supervisor returns ``Command(goto=<agent-or-END>)`` and each specialist
    returns to the supervisor after its bounded task.
    """

    if not agents:
        raise ValueError("supervisor pattern requires at least one agent")
    graph = StateGraph(state_schema, name=name)
    graph.add_node(
        "supervisor",
        supervisor,
        destinations=(*agents.keys(), END),
        metadata={"pattern": "supervisor", "role": "manager"},
    )
    for agent_name, agent in agents.items():
        graph.add_node(
            agent_name,
            agent,
            metadata={"pattern": "supervisor", "role": "specialist"},
        )
        graph.add_edge(agent_name, "supervisor")
    graph.add_edge(START, "supervisor")
    return graph


def build_handoff(
    state_schema: type,
    agents: Mapping[str, Agent],
    *,
    entry: str,
    name: str = "handoff",
) -> StateGraph:
    """Build peer handoffs where the selected specialist owns the next turn."""

    if entry not in agents:
        raise ValueError("handoff entry must name a registered agent")
    destinations = (*agents.keys(), END)
    graph = StateGraph(state_schema, name=name)
    for agent_name, agent in agents.items():
        graph.add_node(
            agent_name,
            agent,
            destinations=destinations,
            metadata={"pattern": "handoff", "role": "peer"},
        )
    graph.add_edge(START, entry)
    return graph


def build_swarm(
    state_schema: type,
    agents: Mapping[str, Agent],
    *,
    entry: str,
) -> StateGraph:
    """Alias of handoff with explicit decentralized-swarm metadata."""

    graph = build_handoff(state_schema, agents, entry=entry, name="swarm")
    for node_name, spec in list(graph._nodes.items()):
        graph._nodes[node_name] = type(spec)(
            action=spec.action,
            retry=spec.retry,
            cache=spec.cache,
            timeout=spec.timeout,
            max_concurrency=spec.max_concurrency,
            subgraph=spec.subgraph,
            subgraph_persistence=spec.subgraph_persistence,
            destinations=spec.destinations,
            metadata={"pattern": "swarm", "role": "peer"},
            middleware=spec.middleware,
        )
    return graph


def build_group_chat(
    state_schema: type,
    agents: Mapping[str, Agent],
    *,
    strategy: str = "round_robin",
    selector: Callable[[Mapping[str, Any]], str] | None = None,
    termination: Callable[[Mapping[str, Any]], bool] | None = None,
    entry: str | None = None,
) -> StateGraph:
    """Build round-robin or selector-driven shared-conversation orchestration.

    State must expose ``active_agent`` and ``turn`` keys.  Termination is a
    deterministic policy over state and is evaluated between turns.
    """

    names = tuple(agents)
    if not names:
        raise ValueError("group chat requires at least one agent")
    if strategy not in {"round_robin", "selector"}:
        raise ValueError("strategy must be 'round_robin' or 'selector'")
    if strategy == "selector" and selector is None:
        raise ValueError("selector strategy requires a selector callable")
    first = entry or names[0]
    if first not in agents:
        raise ValueError("group chat entry must name a registered agent")

    def route(state: Mapping[str, Any]) -> Command[Any]:
        if termination is not None and termination(state):
            return Command(goto=END)
        current = str(state.get("active_agent") or first)
        turn = int(state.get("turn", 0))
        if strategy == "selector":
            target = selector(state)  # type: ignore[misc]
        else:
            target = names[(names.index(current) + 1) % len(names)]
        if target not in agents:
            raise ValueError(f"group-chat selector returned unknown agent {target!r}")
        return Command(update={"active_agent": target, "turn": turn + 1}, goto=target)

    graph = StateGraph(state_schema, name=f"group-chat-{strategy}")
    graph.add_node("router", route, destinations=(*names, END))
    for agent_name, agent in agents.items():
        graph.add_node(
            agent_name,
            agent,
            metadata={"pattern": "group_chat", "strategy": strategy},
        )
        graph.add_edge(agent_name, "router")
    graph.add_conditional_edges(START, lambda _state: first, {first: first})
    return graph


def build_plan_execute(
    state_schema: type,
    planner: Agent,
    executor: Agent,
    replanner: Callable[..., Any],
) -> StateGraph:
    """Build planner → executor → replanner with dynamic termination."""

    graph = StateGraph(state_schema, name="plan-execute")
    graph.add_node("planner", planner)
    graph.add_node("executor", executor)
    graph.add_node("replanner", replanner, destinations=("executor", END))
    graph.add_edge(START, "planner")
    graph.add_edge("planner", "executor")
    graph.add_edge("executor", "replanner")
    return graph


def build_parallel_review(
    state_schema: type,
    source: Agent,
    reviewers: Mapping[str, Agent],
    judge: Agent,
    *,
    review_input: Callable[[Mapping[str, Any], str], Any] | None = None,
) -> StateGraph:
    """Build source → parallel reviewers → deterministic judge map-reduce."""

    if not reviewers:
        raise ValueError("parallel review requires at least one reviewer")
    graph = StateGraph(state_schema, name="parallel-review")
    graph.add_node("source", source)
    for reviewer_name, reviewer in reviewers.items():
        graph.add_node(
            reviewer_name,
            reviewer,
            metadata={"pattern": "parallel_review", "role": "reviewer"},
        )
    graph.add_node("judge", judge)

    def fan_out(state: Mapping[str, Any]):
        return [
            Send(
                reviewer_name,
                review_input(state, reviewer_name) if review_input else dict(state),
            )
            for reviewer_name in reviewers
        ]

    graph.add_edge(START, "source")
    graph.add_conditional_edges("source", fan_out)
    graph.add_edge(tuple(reviewers), "judge")
    graph.add_edge("judge", END)
    return graph


__all__ = [
    "AgentTool",
    "build_group_chat",
    "build_handoff",
    "build_manager_as_tools",
    "build_parallel_review",
    "build_plan_execute",
    "build_supervisor",
    "build_swarm",
]
