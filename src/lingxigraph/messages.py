"""Provider-neutral messages and the canonical message-state reducer."""

from __future__ import annotations

import copy
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Annotated, Any, Literal, TypedDict
from uuid import uuid4


def _id() -> str:
    return str(uuid4())


@dataclass(frozen=True, slots=True)
class ToolCall:
    name: str
    args: Mapping[str, Any] = field(default_factory=dict)
    id: str = field(default_factory=_id)
    type: str = "tool_call"


@dataclass(frozen=True, slots=True)
class ToolCallChunk:
    name: str | None = None
    args: str = ""
    id: str | None = None
    index: int = 0


@dataclass(frozen=True, slots=True)
class SystemMessage:
    content: Any
    id: str = field(default_factory=_id)
    name: str | None = None
    additional_kwargs: Mapping[str, Any] = field(default_factory=dict)
    response_metadata: Mapping[str, Any] = field(default_factory=dict)
    type: Literal["system"] = "system"


@dataclass(frozen=True, slots=True)
class HumanMessage:
    content: Any
    id: str = field(default_factory=_id)
    name: str | None = None
    additional_kwargs: Mapping[str, Any] = field(default_factory=dict)
    response_metadata: Mapping[str, Any] = field(default_factory=dict)
    type: Literal["human"] = "human"


@dataclass(frozen=True, slots=True)
class AIMessage:
    content: Any
    id: str = field(default_factory=_id)
    name: str | None = None
    tool_calls: tuple[ToolCall, ...] = ()
    usage: Mapping[str, Any] = field(default_factory=dict)
    additional_kwargs: Mapping[str, Any] = field(default_factory=dict)
    response_metadata: Mapping[str, Any] = field(default_factory=dict)
    type: Literal["ai"] = "ai"


@dataclass(frozen=True, slots=True)
class ToolMessage:
    content: Any
    tool_call_id: str
    id: str = field(default_factory=_id)
    name: str | None = None
    status: Literal["success", "error"] = "success"
    additional_kwargs: Mapping[str, Any] = field(default_factory=dict)
    response_metadata: Mapping[str, Any] = field(default_factory=dict)
    type: Literal["tool"] = "tool"


@dataclass(frozen=True, slots=True)
class AIMessageChunk:
    content: Any = ""
    id: str | None = None
    name: str | None = None
    tool_call_chunks: tuple[ToolCallChunk, ...] = ()
    usage: Mapping[str, Any] = field(default_factory=dict)
    additional_kwargs: Mapping[str, Any] = field(default_factory=dict)
    response_metadata: Mapping[str, Any] = field(default_factory=dict)
    type: Literal["ai_chunk"] = "ai_chunk"


@dataclass(frozen=True, slots=True)
class RemoveMessage:
    id: str
    type: Literal["remove"] = "remove"


REMOVE_ALL_MESSAGES = "__remove_all__"
AnyMessage = SystemMessage | HumanMessage | AIMessage | ToolMessage | AIMessageChunk


def message_from_dict(value: Mapping[str, Any]) -> AnyMessage | RemoveMessage:
    """Coerce common OpenAI/LangChain-style dictionaries into message objects."""

    kind = str(value.get("type") or value.get("role") or "human").lower()
    data = dict(value)
    data.pop("role", None)
    if kind in {"assistant", "ai"}:
        data["tool_calls"] = tuple(
            item if isinstance(item, ToolCall) else ToolCall(
                name=str(item.get("name") or item.get("function", {}).get("name", "")),
                args=item.get("args", item.get("function", {}).get("arguments", {})),
                id=str(item.get("id") or _id()),
            )
            for item in data.get("tool_calls", ())
        )
        data["type"] = "ai"
        return AIMessage(**data)
    if kind in {"user", "human"}:
        data["type"] = "human"
        return HumanMessage(**data)
    if kind == "system":
        data["type"] = "system"
        return SystemMessage(**data)
    if kind == "tool":
        data["type"] = "tool"
        return ToolMessage(**data)
    if kind in {"remove", "remove_message"}:
        return RemoveMessage(id=str(data["id"]))
    if kind in {"ai_chunk", "assistant_chunk"}:
        data["tool_call_chunks"] = tuple(
            item if isinstance(item, ToolCallChunk) else ToolCallChunk(**item)
            for item in data.get("tool_call_chunks", ())
        )
        data["type"] = "ai_chunk"
        return AIMessageChunk(**data)
    raise ValueError(f"unsupported message type {kind!r}")


def _coerce_many(value: Any) -> list[AnyMessage | RemoveMessage]:
    if value is None:
        return []
    if isinstance(value, str):
        return [HumanMessage(value)]
    if isinstance(value, Mapping):
        return [message_from_dict(value)]
    if isinstance(value, (SystemMessage, HumanMessage, AIMessage, ToolMessage, AIMessageChunk, RemoveMessage)):
        return [value]
    if isinstance(value, Iterable):
        result: list[AnyMessage | RemoveMessage] = []
        for item in value:
            result.extend(_coerce_many(item))
        return result
    raise TypeError(f"cannot coerce {type(value).__name__} to a message")


def add_messages(left: Any, right: Any) -> list[AnyMessage]:
    """Merge messages by stable id, supporting replacement and deletion."""

    existing = [item for item in _coerce_many(left) if not isinstance(item, RemoveMessage)]
    positions = {message.id: index for index, message in enumerate(existing) if message.id}
    for message in _coerce_many(right):
        if isinstance(message, RemoveMessage):
            if message.id == REMOVE_ALL_MESSAGES:
                existing.clear()
                positions.clear()
                continue
            index = positions.pop(message.id, None)
            if index is None:
                continue
            existing.pop(index)
            positions = {item.id: i for i, item in enumerate(existing) if item.id}
            continue
        message_id = message.id or _id()
        if not message.id:
            message = replace(message, id=message_id)
        index = positions.get(message_id)
        if index is None:
            positions[message_id] = len(existing)
            existing.append(copy.deepcopy(message))
        else:
            existing[index] = copy.deepcopy(message)
    return existing


def merge_chunks(chunks: Sequence[AIMessageChunk]) -> AIMessage:
    """Collapse streamed chunks into one AI message."""

    content = "".join(str(chunk.content) for chunk in chunks if chunk.content is not None)
    grouped: dict[int, list[ToolCallChunk]] = {}
    for chunk in chunks:
        for tool_chunk in chunk.tool_call_chunks:
            grouped.setdefault(tool_chunk.index, []).append(tool_chunk)
    tool_calls: list[ToolCall] = []
    if grouped:
        import json

        for parts in grouped.values():
            raw = "".join(part.args for part in parts)
            try:
                args = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                args = {"raw": raw}
            tool_calls.append(
                ToolCall(
                    name=next((part.name for part in parts if part.name), ""),
                    args=args,
                    id=next((part.id for part in parts if part.id), _id()),
                )
            )
    last = chunks[-1] if chunks else AIMessageChunk()
    return AIMessage(
        content=content,
        id=last.id or _id(),
        name=last.name,
        tool_calls=tuple(tool_calls),
        usage=dict(last.usage),
        additional_kwargs=dict(last.additional_kwargs),
        response_metadata=dict(last.response_metadata),
    )


class MessagesState(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]


__all__ = [
    "AIMessage", "AIMessageChunk", "AnyMessage", "HumanMessage", "MessagesState",
    "REMOVE_ALL_MESSAGES", "RemoveMessage", "SystemMessage", "ToolCall", "ToolCallChunk",
    "ToolMessage", "add_messages", "merge_chunks", "message_from_dict",
]
