"""Provider-neutral tool schemas, decorators, execution node, and router."""

from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, get_args, get_origin, get_type_hints

from .constants import END
from .errors import (
    BudgetExceededError,
    GraphCancelledError,
    GraphTimeoutError,
    InvalidUpdateError,
)
from .messages import AIMessage, ToolCall, ToolMessage
from .runtime import Runtime, get_runtime
from .schema import _json_type
from .types import Command


@dataclass(frozen=True, slots=True)
class ToolSpec:
    name: str
    description: str
    parameters: Mapping[str, Any]
    func: Callable[..., Any]
    return_direct: bool = False
    timeout: float | None = 30.0
    permissions: tuple[str, ...] = ()
    requires_approval: bool = False
    secret_refs: Mapping[str, str] = field(default_factory=dict)

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


def _is_injected(parameter_name: str, annotation: Any) -> bool:
    return parameter_name in {"runtime", "tool_call", "idempotency_key"} or annotation in {
        Runtime,
        ToolCall,
    } or getattr(annotation, "__origin__", None) is Runtime


def _make_spec(
    func: Callable[..., Any],
    *,
    name: str | None,
    return_direct: bool,
    timeout: float | None = 30.0,
    permissions: Sequence[str] = (),
    requires_approval: bool = False,
    secret_refs: Mapping[str, str] | None = None,
) -> ToolSpec:
    if timeout is not None and timeout <= 0:
        raise ValueError("tool timeout must be positive or None")
    if isinstance(permissions, str):
        raise TypeError("tool permissions must be a sequence of permission names, not a string")
    if any(not isinstance(item, str) or not item for item in permissions):
        raise ValueError("tool permissions must contain non-empty strings")
    signature = inspect.signature(func)
    hints = get_type_hints(func)
    unknown_secrets = set(secret_refs or {}) - set(signature.parameters)
    if unknown_secrets:
        raise ValueError(
            "secret_refs contains unknown parameter(s): " + ", ".join(sorted(unknown_secrets))
        )
    if any(not isinstance(reference, str) or not reference for reference in (secret_refs or {}).values()):
        raise ValueError("secret_refs values must be non-empty strings")
    properties: dict[str, Any] = {}
    required: list[str] = []
    for parameter_name, parameter in signature.parameters.items():
        if parameter.kind in (parameter.VAR_POSITIONAL, parameter.VAR_KEYWORD):
            continue
        annotation = hints.get(parameter_name, Any)
        if _is_injected(parameter_name, annotation) or parameter_name in (secret_refs or {}):
            continue
        properties[parameter_name] = _schema(annotation)
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
        timeout=timeout,
        permissions=tuple(permissions),
        requires_approval=requires_approval,
        secret_refs=dict(secret_refs or {}),
    )


def tool(
    value: Callable[..., Any] | str | None = None,
    *,
    name: str | None = None,
    return_direct: bool = False,
    timeout: float | None = 30.0,
    permissions: Sequence[str] = (),
    requires_approval: bool = False,
    secret_refs: Mapping[str, str] | None = None,
) -> ToolSpec | Callable[[Callable[..., Any]], ToolSpec]:
    """Decorate a typed Python callable as a serializable JSON-schema tool."""

    if isinstance(value, str):
        name = value
        value = None
    if callable(value):
        return _make_spec(
            value,
            name=name,
            return_direct=return_direct,
            timeout=timeout,
            permissions=permissions,
            requires_approval=requires_approval,
            secret_refs=secret_refs,
        )

    def decorate(func: Callable[..., Any]) -> ToolSpec:
        return _make_spec(
            func,
            name=name,
            return_direct=return_direct,
            timeout=timeout,
            permissions=permissions,
            requires_approval=requires_approval,
            secret_refs=secret_refs,
        )

    return decorate


def as_tool_spec(value: ToolSpec | Callable[..., Any]) -> ToolSpec:
    return value if isinstance(value, ToolSpec) else _make_spec(value, name=None, return_direct=False)


def _validate_json_value(value: Any, schema: Mapping[str, Any], path: str) -> None:
    if "enum" in schema and value not in schema["enum"]:
        raise ValueError(f"{path} must be one of {schema['enum']!r}")
    expected = schema.get("type")
    valid = {
        "string": isinstance(value, str),
        "boolean": isinstance(value, bool),
        "integer": isinstance(value, int) and not isinstance(value, bool),
        "number": isinstance(value, (int, float)) and not isinstance(value, bool),
        "object": isinstance(value, Mapping),
        "array": isinstance(value, (list, tuple)),
        "null": value is None,
    }
    if expected in valid and not valid[expected]:
        raise ValueError(f"{path} must be {expected}, got {type(value).__name__}")
    if expected == "array" and isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _validate_json_value(item, schema.get("items", {}), f"{path}[{index}]")
    if expected == "object" and isinstance(value, Mapping):
        properties = schema.get("properties", {})
        missing = set(schema.get("required", ())) - set(value)
        if missing:
            raise ValueError(f"{path} is missing required argument(s): {', '.join(sorted(missing))}")
        if schema.get("additionalProperties") is False:
            unknown = set(value) - set(properties)
            if unknown:
                raise ValueError(f"{path} contains unknown argument(s): {', '.join(sorted(unknown))}")
        for key, item in value.items():
            if key in properties:
                _validate_json_value(item, properties[key], f"{path}.{key}")


def validate_json_schema(value: Any, schema: Mapping[str, Any], *, path: str = "value") -> None:
    """Validate the dependency-free JSON Schema subset emitted by LingxiGraph."""

    _validate_json_value(value, schema, path)


class ToolNode:
    def __init__(
        self,
        tools: Sequence[ToolSpec | Callable[..., Any]],
        *,
        messages_key: str = "messages",
        on_error: bool | Callable[[Exception], str] = True,
        authorize: Callable[[ToolSpec, ToolCall, Runtime[Any] | None], bool | Any] | None = None,
        secret_resolver: Callable[[str], Any] | None = None,
    ) -> None:
        specs = [as_tool_spec(item) for item in tools]
        if len({item.name for item in specs}) != len(specs):
            raise ValueError("tool names must be unique")
        self.tools = tuple(specs)
        self._by_name = {item.name: item for item in specs}
        self.messages_key = messages_key
        self.on_error = on_error
        self.authorize = authorize
        self.secret_resolver = secret_resolver

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
            try:
                runtime = get_runtime()
            except RuntimeError:
                runtime = None
            _validate_json_value(dict(call.args), spec.parameters, spec.name)
            if runtime is not None:
                runtime.consume_tool_call(spec.name)
                configured = runtime.config.get("tool_permissions", ())
                if isinstance(configured, str):
                    allowed = {configured}
                elif isinstance(configured, Sequence):
                    allowed = set(configured)
                else:
                    allowed = set()
                missing = set(spec.permissions) - allowed
                if missing:
                    raise PermissionError(
                        f"tool {spec.name!r} requires permission(s): {', '.join(sorted(missing))}"
                    )
            elif spec.permissions:
                raise PermissionError(
                    f"tool {spec.name!r} requires graph runtime permission context"
                )
            if self.authorize is not None:
                decision = self.authorize(spec, call, runtime)
                if inspect.isawaitable(decision):
                    decision = await decision
                if not decision:
                    raise PermissionError(f"tool {spec.name!r} was denied by policy")
            kwargs = dict(call.args)
            for parameter_name, reference in spec.secret_refs.items():
                if self.secret_resolver is None:
                    raise RuntimeError(
                        f"tool {spec.name!r} requires a secret_resolver for {reference!r}"
                    )
                secret = self.secret_resolver(reference)
                if inspect.isawaitable(secret):
                    secret = await secret
                kwargs[parameter_name] = secret
            signature = inspect.signature(spec.func)
            hints = get_type_hints(spec.func)
            for parameter_name in signature.parameters:
                annotation = hints.get(parameter_name, Any)
                if parameter_name == "runtime" or annotation is Runtime or getattr(
                    annotation, "__origin__", None
                ) is Runtime:
                    if runtime is None:
                        raise RuntimeError("runtime injection requires graph execution")
                    kwargs[parameter_name] = runtime
                elif parameter_name == "tool_call" or annotation is ToolCall:
                    kwargs[parameter_name] = call
                elif parameter_name == "idempotency_key":
                    if runtime is None:
                        raise RuntimeError("idempotency_key injection requires graph execution")
                    kwargs[parameter_name] = runtime.idempotency_key
            if inspect.iscoroutinefunction(spec.func):
                invocation = spec.func(**kwargs)
            else:
                invocation = asyncio.to_thread(spec.func, **kwargs)
            result = (
                await asyncio.wait_for(invocation, timeout=spec.timeout)
                if spec.timeout is not None
                else await invocation
            )
            if inspect.isawaitable(result):
                result = await result
            if isinstance(result, Command):
                return result
            content = result if isinstance(result, str) else json.dumps(result, ensure_ascii=False, default=str)
            return ToolMessage(content, tool_call_id=call.id, name=call.name)
        except Exception as exc:
            if isinstance(exc, (BudgetExceededError, GraphCancelledError, GraphTimeoutError)):
                raise
            if self.on_error is False:
                raise
            content = self.on_error(exc) if callable(self.on_error) else f"{type(exc).__name__}: {exc}"
            return ToolMessage(content, tool_call_id=call.id, name=call.name, status="error")


def tools_condition(state: Mapping[str, Any], *, messages_key: str = "messages") -> str:
    messages = state.get(messages_key, ())
    return "tools" if messages and isinstance(messages[-1], AIMessage) and messages[-1].tool_calls else END


__all__ = [
    "ToolNode",
    "ToolSpec",
    "as_tool_spec",
    "tool",
    "tools_condition",
    "validate_json_schema",
]
