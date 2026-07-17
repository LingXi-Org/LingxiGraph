"""Battle-tested orchestration topologies without model-provider coupling."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from ..constants import END, START
from ..graph import CompiledGraph, StateGraph
from ..messages import HumanMessage, SystemMessage, ToolMessage
from ..prebuilt import create_agent
from ..runtime import Runtime
from ..schema import SchemaAdapter
from ..tools import ToolSpec, tool
from ..types import Command, CommandScope, Send

Agent = Callable[..., Any] | CompiledGraph


def create_handoff_tool(
    agent_name: str,
    *,
    description: str | None = None,
    update: Mapping[str, Any] | None = None,
) -> ToolSpec:
    """Create a model-callable tool that transfers control to a parent agent node."""

    if not agent_name:
        raise ValueError("agent_name must be non-empty")

    def handoff() -> Command[Any]:
        """Transfer the conversation to another specialist."""

        return Command(
            goto=agent_name,
            update={
                "messages": [
                    ToolMessage(
                        f"Transferred to {agent_name}.",
                        tool_call_id=f"handoff:{agent_name}",
                        name=f"transfer_to_{agent_name}",
                    )
                ],
                "active_agent": agent_name,
                **dict(update or {}),
            },
            scope=CommandScope.PARENT,
        )

    spec = tool(
        handoff,
        name=f"transfer_to_{agent_name}",
    )
    assert isinstance(spec, ToolSpec)
    if description is not None:
        return ToolSpec(spec.name, description, spec.parameters, spec.func, spec.return_direct)
    return spec


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
    supervisor: Callable[..., Any] | None = None,
    agents: Mapping[str, Agent] | None = None,
    *,
    name: str = "supervisor",
    model: Any | None = None,
    agent_descriptions: Mapping[str, str] | None = None,
) -> StateGraph:
    """Build a manager-owned loop.

    The supervisor returns ``Command(goto=<agent-or-END>)`` and each specialist
    returns to the supervisor after its bounded task.
    """

    if not agents:
        raise ValueError("supervisor pattern requires at least one agent")
    if supervisor is None and model is None:
        raise ValueError("supervisor callable or model is required")
    if supervisor is not None and model is not None:
        raise ValueError("use either supervisor callable or model, not both")
    supervisor_node: Agent
    if model is not None:
        supervisor_node = create_agent(
            model,
            [
                create_handoff_tool(
                    agent_name,
                    description=(agent_descriptions or {}).get(agent_name),
                )
                for agent_name in agents
            ],
            state_schema=state_schema,
            name=f"{name}-router",
        )
    else:
        assert supervisor is not None
        supervisor_node = supervisor
    graph = StateGraph(state_schema, name=name)
    graph.add_node(
        "supervisor",
        supervisor_node,
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
    """Build a decentralized swarm that resumes at the persisted active agent."""

    if entry not in agents:
        raise ValueError("swarm entry must name a registered agent")
    graph = StateGraph(state_schema, name="swarm")
    destinations = (*agents.keys(), END)
    for agent_name, agent in agents.items():
        graph.add_node(
            agent_name,
            agent,
            destinations=destinations,
            metadata={"pattern": "swarm", "role": "peer"},
        )
    if "active_agent" in SchemaAdapter(state_schema).fields:
        def route(state: Mapping[str, Any]) -> str:
            target = str(state.get("active_agent") or entry)
            if target not in agents:
                raise ValueError(f"state selected unknown active_agent {target!r}")
            return target

        graph.add_conditional_edges(START, route, {name: name for name in agents})
    else:
        # Compatibility for 1.x state schemas; durable swarm handoff requires
        # declaring ``active_agent`` in the state.
        graph.add_edge(START, entry)
    return graph


def build_group_chat(
    state_schema: type,
    agents: Mapping[str, Agent],
    *,
    strategy: str = "round_robin",
    selector: Callable[[Mapping[str, Any]], str] | None = None,
    termination: Callable[[Mapping[str, Any]], bool] | None = None,
    entry: str | None = None,
    model: Any | None = None,
    agent_descriptions: Mapping[str, str] | None = None,
) -> StateGraph:
    """Build round-robin or selector-driven shared-conversation orchestration.

    State must expose ``active_agent`` and ``turn`` keys.  Termination is a
    deterministic policy over state and is evaluated between turns.
    """

    names = tuple(agents)
    if not names:
        raise ValueError("group chat requires at least one agent")
    if strategy not in {"round_robin", "selector", "llm"}:
        raise ValueError("strategy must be 'round_robin', 'selector', or 'llm'")
    if strategy == "selector" and selector is None:
        raise ValueError("selector strategy requires a selector callable")
    if strategy == "llm" and model is None:
        raise ValueError("llm strategy requires a model")
    first = entry or names[0]
    if first not in agents:
        raise ValueError("group chat entry must name a registered agent")

    async def route(state: Mapping[str, Any]) -> Command[Any]:
        if termination is not None and termination(state):
            return Command(goto=END)
        current = str(state.get("active_agent") or first)
        turn = int(state.get("turn", 0))
        if strategy == "selector":
            target = selector(state)  # type: ignore[misc]
        elif strategy == "llm":
            assert model is not None
            descriptions = "\n".join(
                f"- {agent}: {(agent_descriptions or {}).get(agent, '')}"
                for agent in names
            )
            selection_tools = [
                create_handoff_tool(
                    agent,
                    description=(agent_descriptions or {}).get(agent),
                )
                for agent in names
            ]
            response = await model.agenerate(
                [
                    SystemMessage(
                        "Select exactly one next speaker using a transfer tool.\n" + descriptions
                    ),
                    HumanMessage(repr(dict(state))),
                ],
                tools=selection_tools,
            )
            if response.tool_calls:
                tool_name = response.tool_calls[0].name
                target = tool_name.removeprefix("transfer_to_")
            else:
                target = str(response.content).strip()
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
    "create_handoff_tool",
]
