"""OpenAI-compatible chat-completions model adapter."""

from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator, Mapping, Sequence
from typing import Any

from ..messages import (
    AIMessage,
    AIMessageChunk,
    AnyMessage,
    ToolCall,
    ToolCallChunk,
    ToolMessage,
)
from ..tools import ToolSpec, as_tool_spec

try:
    import httpx
except ImportError as exc:  # pragma: no cover
    raise RuntimeError("install lingxigraph[openai] to use OpenAICompatChatModel") from exc


def _message(value: AnyMessage) -> dict[str, Any]:
    role = {"human": "user", "ai": "assistant"}.get(value.type, value.type)
    result: dict[str, Any] = {"role": role, "content": value.content}
    if isinstance(value, AIMessage) and value.tool_calls:
        result["tool_calls"] = [
            {
                "id": call.id,
                "type": "function",
                "function": {"name": call.name, "arguments": json.dumps(call.args)},
            }
            for call in value.tool_calls
        ]
    if isinstance(value, ToolMessage):
        result["tool_call_id"] = value.tool_call_id
        if value.name:
            result["name"] = value.name
    if value.name and not isinstance(value, ToolMessage):
        result["name"] = value.name
    return result


def _tool_calls(raw_calls: Sequence[Mapping[str, Any]]) -> tuple[ToolCall, ...]:
    calls = []
    for raw in raw_calls:
        function = raw.get("function", {})
        arguments = function.get("arguments", "{}")
        try:
            arguments = json.loads(arguments) if isinstance(arguments, str) else arguments
        except json.JSONDecodeError:
            arguments = {"raw": arguments}
        calls.append(ToolCall(str(function.get("name", "")), dict(arguments), str(raw.get("id", ""))))
    return tuple(calls)


class OpenAICompatChatModel:
    def __init__(
        self,
        model: str,
        *,
        base_url: str = "https://api.openai.com/v1",
        api_key: str | None = None,
        timeout: float = 60.0,
        transport: httpx.AsyncBaseTransport | None = None,
        default_options: Mapping[str, Any] | None = None,
    ) -> None:
        self.model = model
        self._options = dict(default_options or {})
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers={"Authorization": f"Bearer {api_key or os.getenv('OPENAI_API_KEY', '')}"},
            timeout=timeout,
            transport=transport,
        )

    def _payload(self, messages: Sequence[AnyMessage], tools: Sequence[Any] | None, **kwargs: Any) -> dict[str, Any]:
        payload = {"model": self.model, "messages": [_message(item) for item in messages], **self._options, **kwargs}
        if tools:
            payload["tools"] = [
                (item if isinstance(item, Mapping) else as_tool_spec(item).as_function_schema())
                for item in tools
            ]
        return payload

    async def agenerate(
        self,
        messages: Sequence[AnyMessage],
        *,
        tools: Sequence[ToolSpec | Any] | None = None,
        **kwargs: Any,
    ) -> AIMessage:
        response = await self._client.post(
            "/chat/completions", json=self._payload(messages, tools, **kwargs)
        )
        response.raise_for_status()
        payload = response.json()
        choice = payload["choices"][0]
        message = choice["message"]
        return AIMessage(
            message.get("content") or "",
            tool_calls=_tool_calls(message.get("tool_calls", ())),
            usage=dict(payload.get("usage", {})),
            response_metadata={"finish_reason": choice.get("finish_reason"), "model": payload.get("model")},
        )

    async def astream(
        self,
        messages: Sequence[AnyMessage],
        *,
        tools: Sequence[ToolSpec | Any] | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[AIMessageChunk]:
        payload = self._payload(messages, tools, **kwargs)
        payload["stream"] = True
        async with self._client.stream("POST", "/chat/completions", json=payload) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if not line.startswith("data:"):
                    continue
                raw = line[5:].strip()
                if raw == "[DONE]":
                    return
                event = json.loads(raw)
                delta = event["choices"][0].get("delta", {})
                chunks = tuple(
                    ToolCallChunk(
                        name=item.get("function", {}).get("name"),
                        args=item.get("function", {}).get("arguments", ""),
                        id=item.get("id"),
                        index=int(item.get("index", 0)),
                    )
                    for item in delta.get("tool_calls", ())
                )
                yield AIMessageChunk(
                    delta.get("content") or "",
                    id=event.get("id"),
                    tool_call_chunks=chunks,
                    response_metadata={"model": event.get("model")},
                )

    async def aclose(self) -> None:
        await self._client.aclose()


__all__ = ["OpenAICompatChatModel"]
