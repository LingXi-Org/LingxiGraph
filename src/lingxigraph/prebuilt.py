"""Production-oriented prebuilt agent graphs."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from typing import Annotated, Any, TypedDict

from .cache_first import (
    CacheFirstChatModel,
    CacheFirstConfig,
    ImmutablePrefix,
    PrefixDriftError,
    UsageLedger,
)
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
from .skills import SkillInput, as_skill_registry
from .tools import ToolNode, ToolSpec, as_tool_spec, tools_condition, validate_json_schema
from .types import interrupt


class AgentState(TypedDict, total=False):
    messages: Annotated[list[AnyMessage], add_messages]
    structured_response: Any


def create_agent(
    model: ChatModel,
    tools: Sequence[ToolSpec | Callable[..., Any]] = (),
    *,
    skills: SkillInput = None,
    system_prompt: str | SystemMessage | None = None,
    prefix: ImmutablePrefix | None = None,
    few_shots: Sequence[AnyMessage] = (),
    pinned_constraints: Sequence[str] = (),
    cache_first: CacheFirstConfig | Mapping[str, Any] | bool | None = None,
    usage_ledger: UsageLedger | None = None,
    pricing: Mapping[str, Any] | None = None,
    summarizer: Callable[..., Any] | None = None,
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

    skill_registry = as_skill_registry(skills)
    skill_specs = skill_registry.tool_specs() if skill_registry is not None else ()
    specs = (*tuple(as_tool_spec(item) for item in tools), *skill_specs)
    if len({spec.name for spec in specs}) != len(specs):
        raise ValueError("tool names must be unique, including Agent Skills runtime tools")
    approvals = set(interrupt_on or ())
    if isinstance(interrupt_on, Mapping):
        approvals = {key for key, enabled in interrupt_on.items() if enabled}
    approvals.update(spec.name for spec in specs if spec.requires_approval)
    if structured_retries < 0:
        raise ValueError("structured_retries must be non-negative")
    if cache_first is False:
        cache_config = CacheFirstConfig(enabled=False)
    else:
        cache_config = CacheFirstConfig.from_mapping(cache_first)
    effective_ledger = (
        usage_ledger if usage_ledger is not None else getattr(model, "usage_ledger", None)
    )
    effective_pricing = pricing if pricing is not None else getattr(model, "pricing", None)

    legacy_prompt = ""
    if system_prompt is not None:
        legacy_prompt = str(
            system_prompt.content if isinstance(system_prompt, SystemMessage) else system_prompt
        )
    if skill_registry is not None:
        skill_catalog = skill_registry.catalog_prompt(
            max_skills=cache_config.progressive_tool_limit
        )
        legacy_prompt = "\n\n".join(item for item in (legacy_prompt, skill_catalog) if item)
    if prefix is None:
        prefix = ImmutablePrefix.create(
            system_prompt=legacy_prompt,
            tools=specs,
            few_shots=few_shots,
            pinned_constraints=pinned_constraints,
        )
    else:
        expected_catalog = prefix.tool_catalog
        actual_catalog = ImmutablePrefix.create(tools=specs).tool_catalog
        if expected_catalog.fingerprint != actual_catalog.fingerprint:
            raise ValueError(
                "explicit ImmutablePrefix tool catalog does not match create_agent tools: "
                f"expected={expected_catalog.fingerprint}, actual={actual_catalog.fingerprint}"
            )
    if isinstance(model, CacheFirstChatModel):
        if model.prefix.fingerprint != prefix.fingerprint and cache_config.enabled:
            raise PrefixDriftError(model.prefix.drift_against(prefix))
        cache_model = model
    else:
        underlying_model = getattr(model, "_raw_model", model)
        cache_model = CacheFirstChatModel(
            underlying_model,
            prefix=prefix,
            config=cache_config,
            usage_ledger=effective_ledger,
            pricing=effective_pricing,
            active_skill_ids=tuple(item.name for item in skill_registry.discover())
            if skill_registry is not None
            else (),
            summarizer=summarizer,
        )

    async def call_model(state: Mapping[str, Any], runtime: Runtime[Any]) -> Mapping[str, Any]:
        messages = list(state.get("messages", ()))
        runtime.consume_model_call()
        stream = getattr(cache_model, "astream", None)
        if callable(stream):
            chunks = []
            async for chunk in stream(messages, tools=specs):
                chunks.append(chunk)
                runtime.emit_message(chunk, {"node": "agent"})
            response = merge_chunks(chunks)
        else:
            response = await cache_model.agenerate(messages, tools=specs)
            runtime.emit_message(response, {"node": "agent"})
        runtime.consume_model_usage(response.usage)
        if (
            runtime.remaining_steps is not None
            and runtime.remaining_steps < 2
            and response.tool_calls
        ):
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
            ToolNode(
                specs,
                authorize=tool_authorize,
                secret_resolver=secret_resolver,
                read_only_concurrency=cache_config.read_only_concurrency,
                read_only_batch_size=cache_config.read_only_batch_size,
            ),
        )
    if response_format is not None:

        async def structured_response(
            state: Mapping[str, Any], runtime: Runtime[Any]
        ) -> Mapping[str, Any]:
            messages = list(state.get("messages", ()))
            error: Exception | None = None
            underlying_model = getattr(cache_model.model, "_raw_model", cache_model.model)
            structured_model = CacheFirstChatModel(
                underlying_model,
                prefix=prefix.evolve(tools=()),
                config=cache_config,
                usage_ledger=usage_ledger
                if usage_ledger is not None
                else getattr(cache_model, "usage_ledger", effective_ledger),
                pricing=pricing
                if pricing is not None
                else getattr(cache_model, "pricing", effective_pricing),
                active_skill_ids=tuple(item.name for item in skill_registry.discover())
                if skill_registry is not None
                else (),
                summarizer=summarizer,
            )
            for attempt in range(structured_retries + 1):
                runtime.consume_model_call()
                response = await structured_model.agenerate(
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
