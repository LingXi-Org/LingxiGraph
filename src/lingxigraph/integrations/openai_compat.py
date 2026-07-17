"""OpenAI-compatible chat-completions model adapter."""

from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator, Mapping, Sequence
from typing import Any
from uuid import uuid4

from ..messages import (
    AIMessage,
    AIMessageChunk,
    AnyMessage,
    ToolCall,
    ToolCallChunk,
    ToolMessage,
)
from ..runtime import get_runtime
from ..tools import ToolSpec, as_tool_spec
from ._http import should_retry_status, sleep_before_retry

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
        max_retries: int = 3,
        retry_base: float = 0.5,
    ) -> None:
        self.model = model
        self._options = dict(default_options or {})
        if max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        self.max_retries = max_retries
        self.retry_base = retry_base
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers={"Authorization": f"Bearer {api_key or os.getenv('OPENAI_API_KEY', '')}"},
            timeout=timeout,
            transport=transport,
        )

    @staticmethod
    def _request_headers(operation_key: str) -> dict[str, str]:
        try:
            runtime = get_runtime()
        except RuntimeError:
            return {"Idempotency-Key": operation_key}
        runtime.raise_if_cancelled()
        return {"Idempotency-Key": runtime.idempotency_key or operation_key}

    async def _post(self, payload: Mapping[str, Any]) -> httpx.Response:
        operation_key = str(uuid4())
        for attempt in range(self.max_retries + 1):
            try:
                response = await self._client.post(
                    "/chat/completions",
                    json=dict(payload),
                    headers=self._request_headers(operation_key),
                )
                if should_retry_status(response.status_code) and attempt < self.max_retries:
                    await sleep_before_retry(attempt + 1, response.headers, base=self.retry_base)
                    continue
                response.raise_for_status()
                return response
            except (httpx.TimeoutException, httpx.NetworkError):
                if attempt >= self.max_retries:
                    raise
                await sleep_before_retry(attempt + 1, base=self.retry_base)
        raise AssertionError("unreachable")

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
        response = await self._post(self._payload(messages, tools, **kwargs))
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
        payload.setdefault("stream_options", {"include_usage": True})
        emitted = False
        operation_key = str(uuid4())
        for attempt in range(self.max_retries + 1):
            try:
                async with self._client.stream(
                    "POST",
                    "/chat/completions",
                    json=payload,
                    headers=self._request_headers(operation_key),
                ) as response:
                    if (
                        should_retry_status(response.status_code)
                        and attempt < self.max_retries
                        and not emitted
                    ):
                        await response.aread()
                        await sleep_before_retry(
                            attempt + 1, response.headers, base=self.retry_base
                        )
                        continue
                    response.raise_for_status()
                    async for line in response.aiter_lines():
                        try:
                            get_runtime().raise_if_cancelled()
                        except RuntimeError:
                            pass
                        if not line.startswith("data:"):
                            continue
                        raw = line[5:].strip()
                        if raw == "[DONE]":
                            return
                        event = json.loads(raw)
                        choices = event.get("choices", ())
                        choice = choices[0] if choices else {}
                        delta = choice.get("delta", {})
                        chunks = tuple(
                            ToolCallChunk(
                                name=item.get("function", {}).get("name"),
                                args=item.get("function", {}).get("arguments", ""),
                                id=item.get("id"),
                                index=int(item.get("index", 0)),
                            )
                            for item in delta.get("tool_calls", ())
                        )
                        usage = dict(event.get("usage") or {})
                        value = AIMessageChunk(
                            delta.get("content") or "",
                            id=event.get("id"),
                            tool_call_chunks=chunks,
                            usage=usage,
                            response_metadata={
                                "model": event.get("model"),
                                "finish_reason": choice.get("finish_reason"),
                            },
                        )
                        if value.content or value.tool_call_chunks or value.usage or choice.get(
                            "finish_reason"
                        ):
                            emitted = True
                            yield value
                    return
            except (httpx.TimeoutException, httpx.NetworkError):
                if emitted or attempt >= self.max_retries:
                    raise
                await sleep_before_retry(attempt + 1, base=self.retry_base)

    async def aclose(self) -> None:
        await self._client.aclose()


__all__ = ["OpenAICompatChatModel"]
