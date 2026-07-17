"""Provider-neutral tool schemas, decorators, execution node, and router."""

from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, get_args, get_origin, get_type_hints

from .constants import END
from .errors import InvalidUpdateError
from .messages import AIMessage, ToolCall, ToolMessage
from .schema import _json_type
from .types import Command


@dataclass(frozen=True, slots=True)
class ToolSpec:
    name: str
    description: str
    parameters: Mapping[str, Any]
    func: Callable[..., Any]
    return_direct: bool = False

    def as_function_schema(self) -> dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": dict(self.parameters),
            },
        }


def _schema(annotation: Any) -> dict[str, Any]:
    if get_origin(annotation) is Literal:
        values = list(get_args(annotation))
        return {"enum": values, "type": _json_type(type(values[0])).get("type")} if values else {}
    return _json_type(annotation)


def _make_spec(func: Callable[..., Any], *, name: str | None, return_direct: bool) -> ToolSpec:
    signature = inspect.signature(func)
    hints = get_type_hints(func)
    properties: dict[str, Any] = {}
    required: list[str] = []
    for parameter_name, parameter in signature.parameters.items():
        if parameter.kind in (parameter.VAR_POSITIONAL, parameter.VAR_KEYWORD):
            continue
        properties[parameter_name] = _schema(hints.get(parameter_name, Any))
        if parameter.default is inspect.Parameter.empty:
            required.append(parameter_name)
    doc = inspect.getdoc(func) or ""
    description = doc.split("\n\n", 1)[0].replace("\n", " ").strip()
    return ToolSpec(
        name=name or func.__name__,
        description=description,
        parameters={
            "type": "object",
            "properties": properties,
            "required": required,
            "additionalProperties": False,
        },
        func=func,
        return_direct=return_direct,
    )


def tool(
    value: Callable[..., Any] | str | None = None,
    *,
    name: str | None = None,
    return_direct: bool = False,
) -> ToolSpec | Callable[[Callable[..., Any]], ToolSpec]:
    """Decorate a typed Python callable as a serializable JSON-schema tool."""

    if isinstance(value, str):
        name = value
        value = None
    if callable(value):
        return _make_spec(value, name=name, return_direct=return_direct)

    def decorate(func: Callable[..., Any]) -> ToolSpec:
        return _make_spec(func, name=name, return_direct=return_direct)

    return decorate


def as_tool_spec(value: ToolSpec | Callable[..., Any]) -> ToolSpec:
    return value if isinstance(value, ToolSpec) else _make_spec(value, name=None, return_direct=False)


class ToolNode:
    def __init__(
        self,
        tools: Sequence[ToolSpec | Callable[..., Any]],
        *,
        messages_key: str = "messages",
        on_error: bool | Callable[[Exception], str] = True,
    ) -> None:
        specs = [as_tool_spec(item) for item in tools]
        if len({item.name for item in specs}) != len(specs):
            raise ValueError("tool names must be unique")
        self.tools = tuple(specs)
        self._by_name = {item.name: item for item in specs}
        self.messages_key = messages_key
        self.on_error = on_error

    async def __call__(self, state: Mapping[str, Any]) -> Mapping[str, Any] | Command[Any]:
        messages = state.get(self.messages_key, ())
        if not messages or not isinstance(messages[-1], AIMessage):
            raise InvalidUpdateError("ToolNode requires the last message to be an AIMessage")
        calls = messages[-1].tool_calls
        results = await asyncio.gather(*(self._execute(call) for call in calls))
        commands = [result for result in results if isinstance(result, Command)]
        if commands:
            if len(results) != 1:
                raise InvalidUpdateError("a Command-returning tool must be the only tool call")
            return commands[0]
        return {self.messages_key: list(results)}

    async def _execute(self, call: ToolCall) -> ToolMessage | Command[Any]:
        spec = self._by_name.get(call.name)
        if spec is None:
            return ToolMessage(
                f"unknown tool {call.name!r}", tool_call_id=call.id, name=call.name, status="error"
            )
        try:
            if inspect.iscoroutinefunction(spec.func):
                result = await spec.func(**dict(call.args))
            else:
                result = await asyncio.to_thread(spec.func, **dict(call.args))
            if inspect.isawaitable(result):
                result = await result
            if isinstance(result, Command):
                return result
            content = result if isinstance(result, str) else json.dumps(result, ensure_ascii=False, default=str)
            return ToolMessage(content, tool_call_id=call.id, name=call.name)
        except Exception as exc:
            if self.on_error is False:
                raise
            content = self.on_error(exc) if callable(self.on_error) else f"{type(exc).__name__}: {exc}"
            return ToolMessage(content, tool_call_id=call.id, name=call.name, status="error")


def tools_condition(state: Mapping[str, Any], *, messages_key: str = "messages") -> str:
    messages = state.get(messages_key, ())
    return "tools" if messages and isinstance(messages[-1], AIMessage) and messages[-1].tool_calls else END


__all__ = ["ToolNode", "ToolSpec", "as_tool_spec", "tool", "tools_condition"]
