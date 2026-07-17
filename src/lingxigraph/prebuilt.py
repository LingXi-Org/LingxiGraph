"""Production-oriented prebuilt agent graphs."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from typing import Annotated, Any, TypedDict

from .constants import END, START
from .graph import StateGraph
from .messages import (
    AIMessage,
    AnyMessage,
    SystemMessage,
    ToolCall,
    ToolMessage,
    add_messages,
    merge_chunks,
)
from .models import ChatModel
from .runtime import Runtime
from .tools import ToolNode, ToolSpec, as_tool_spec, tools_condition, validate_json_schema
from .types import interrupt


class AgentState(TypedDict, total=False):
    messages: Annotated[list[AnyMessage], add_messages]
    structured_response: Any


def create_agent(
    model: ChatModel,
    tools: Sequence[ToolSpec | Callable[..., Any]] = (),
    *,
    system_prompt: str | SystemMessage | None = None,
    state_schema: type = AgentState,
    response_format: Mapping[str, Any] | type | None = None,
    pre_model_hook: Callable[..., Any] | None = None,
    post_model_hook: Callable[..., Any] | None = None,
    interrupt_on: Sequence[str] | Mapping[str, bool] | None = None,
    structured_retries: int = 2,
    tool_authorize: Callable[..., Any] | None = None,
    secret_resolver: Callable[[str], Any] | None = None,
    name: str | None = None,
    checkpointer: Any | None = None,
    store: Any | None = None,
):
    """Build a durable ReAct-style loop over the neutral ``ChatModel`` protocol."""

    specs = tuple(as_tool_spec(item) for item in tools)
    approvals = set(interrupt_on or ())
    if isinstance(interrupt_on, Mapping):
        approvals = {key for key, enabled in interrupt_on.items() if enabled}
    approvals.update(spec.name for spec in specs if spec.requires_approval)
    if structured_retries < 0:
        raise ValueError("structured_retries must be non-negative")

    async def call_model(state: Mapping[str, Any], runtime: Runtime[Any]) -> Mapping[str, Any]:
        messages = list(state.get("messages", ()))
        if system_prompt is not None:
            prompt = system_prompt if isinstance(system_prompt, SystemMessage) else SystemMessage(system_prompt)
            messages = [prompt, *messages]
        runtime.consume_model_call()
        stream = getattr(model, "astream", None)
        if callable(stream):
            chunks = []
            async for chunk in stream(messages, tools=specs):
                chunks.append(chunk)
                runtime.emit_message(chunk, {"node": "agent"})
            response = merge_chunks(chunks)
        else:
            response = await model.agenerate(messages, tools=specs)
            runtime.emit_message(response, {"node": "agent"})
        runtime.consume_model_usage(response.usage)
        if runtime.remaining_steps is not None and runtime.remaining_steps < 2 and response.tool_calls:
            response = AIMessage(
                "Unable to complete tool calls within the remaining graph steps.",
                response_metadata={"finish_reason": "remaining_steps"},
            )
        return {"messages": [response]}

    async def approve_tools(state: Mapping[str, Any]) -> Mapping[str, Any]:
        messages = state.get("messages", ())
        last = messages[-1] if messages else None
        if not isinstance(last, AIMessage):
            return {}
        selected = [call for call in last.tool_calls if call.name in approvals]
        if not selected:
            return {}
        decision = interrupt(
            {
                "type": "tool_approval",
                "tool_calls": [
                    {"id": call.id, "name": call.name, "args": dict(call.args)}
                    for call in selected
                ],
            }
        )
        action = decision.get("action") if isinstance(decision, Mapping) else decision
        if action == "approve":
            return {}
        if action == "reject":
            return {
                "messages": [
                    ToolMessage(
                        str(decision.get("message", "tool call rejected")),
                        tool_call_id=call.id,
                        name=call.name,
                        status="error",
                    )
                    for call in selected
                ]
            }
        if action == "edit":
            edits = decision.get("tool_calls", ())
            replacement = tuple(
                ToolCall(str(item["name"]), dict(item.get("args", {})), str(item.get("id", "")))
                for item in edits
            )
            return {"messages": [AIMessage(last.content, id=last.id, tool_calls=replacement)]}
        raise ValueError("tool approval action must be approve, reject, or edit")

    graph = StateGraph(state_schema, name=name or "agent", version="2")
    if pre_model_hook is not None:
        graph.add_node("pre_model", pre_model_hook)
    graph.add_node("agent", call_model)
    if post_model_hook is not None:
        graph.add_node("post_model", post_model_hook)
    if approvals:
        graph.add_node("approve_tools", approve_tools)
    if specs:
        graph.add_node(
            "tools",
            ToolNode(specs, authorize=tool_authorize, secret_resolver=secret_resolver),
        )
    if response_format is not None:
        async def structured_response(
            state: Mapping[str, Any], runtime: Runtime[Any]
        ) -> Mapping[str, Any]:
            messages = list(state.get("messages", ()))
            error: Exception | None = None
            for attempt in range(structured_retries + 1):
                runtime.consume_model_call()
                response = await model.agenerate(
                    messages,
                    tools=None,
                    response_format=response_format,
                )
                runtime.consume_model_usage(response.usage)
                value = response.content
                try:
                    if isinstance(value, str):
                        value = json.loads(value)
                    validate = getattr(response_format, "model_validate", None)
                    if callable(validate):
                        value = validate(value)
                    elif isinstance(response_format, Mapping):
                        validate_json_schema(value, response_format, path="structured_response")
                    return {"structured_response": value}
                except (json.JSONDecodeError, TypeError, ValueError) as exc:
                    error = exc
                    if attempt >= structured_retries:
                        break
                    messages = [
                        *messages,
                        response,
                        SystemMessage(
                            "The structured response was invalid. Return only data that satisfies "
                            f"the requested schema. Validation error: {exc}"
                        ),
                    ]
            assert error is not None
            raise ValueError(
                f"structured response remained invalid after {structured_retries + 1} attempt(s): "
                f"{error}"
            ) from error

        graph.add_node("structured_response", structured_response)

    first = "pre_model" if pre_model_hook is not None else "agent"
    graph.add_edge(START, first)
    if pre_model_hook is not None:
        graph.add_edge("pre_model", "agent")
    route_source = "post_model" if post_model_hook is not None else "agent"
    if post_model_hook is not None:
        graph.add_edge("agent", "post_model")
    final_target = "structured_response" if response_format is not None else END
    if not specs:
        graph.add_edge(route_source, final_target)
    else:
        target = "approve_tools" if approvals else "tools"
        graph.add_conditional_edges(
            route_source,
            lambda state: target if tools_condition(state) == "tools" else final_target,
            {target: target, final_target: final_target},
        )
        if approvals:
            graph.add_conditional_edges(
                "approve_tools",
                lambda state: "tools" if tools_condition(state) == "tools" else "agent",
                {"tools": "tools", "agent": "agent"},
            )
        graph.add_edge("tools", "agent")
    if response_format is not None:
        graph.add_edge("structured_response", END)

    compiled = graph.compile(checkpointer=checkpointer, store=store)
    compiled.response_format = response_format
    return compiled


create_react_agent = create_agent

__all__ = ["AgentState", "create_agent", "create_react_agent"]
