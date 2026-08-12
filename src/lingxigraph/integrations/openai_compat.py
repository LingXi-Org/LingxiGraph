"""OpenAI-compatible chat-completions model adapter."""

from __future__ import annotations

import json
import os
from collections.abc import AsyncIterator, Mapping, Sequence
from typing import Any
from uuid import uuid4

from ..cache_first import (
    CacheFirstChatModel,
    CacheFirstConfig,
    ImmutablePrefix,
    UsageLedger,
    canonicalize_tools,
)
from ..messages import (
    AIMessage,
    AIMessageChunk,
    AnyMessage,
    ToolCall,
    ToolCallChunk,
    ToolMessage,
)
from ..runtime import get_runtime
from ..tools import ToolSpec
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
                "function": {
                    "name": call.name,
                    "arguments": json.dumps(
                        call.args,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                },
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
        calls.append(
            ToolCall(str(function.get("name", "")), dict(arguments), str(raw.get("id", "")))
        )
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
        immutable_prefix: ImmutablePrefix | None = None,
        cache_first: CacheFirstConfig | Mapping[str, Any] | bool | None = None,
        usage_ledger: UsageLedger | None = None,
        pricing: Mapping[str, Any] | None = None,
    ) -> None:
        self.model = model
        self.provider_id = "openai-compatible"
        self.base_url = base_url.rstrip("/")
        self.endpoint_format = f"chat-completions:{self.base_url}/chat/completions"
        self.immutable_prefix = immutable_prefix
        if cache_first is False:
            self.cache_first_config = CacheFirstConfig(enabled=False)
        elif cache_first is None and immutable_prefix is not None:
            self.cache_first_config = CacheFirstConfig(verify_mode="strict")
        elif cache_first is None:
            # No explicit prefix is the backwards-compatible inference mode:
            # warn on unstable/changed prefixes but keep sending the request.
            self.cache_first_config = CacheFirstConfig(verify_mode="warn")
        else:
            self.cache_first_config = CacheFirstConfig.from_mapping(cache_first)
        self.usage_ledger = usage_ledger
        self.pricing = dict(pricing or {})
        self._raw_model = _RawOpenAIModel(self)
        self._compat_wrapper: CacheFirstChatModel | None = None
        self._options = dict(default_options or {})
        if max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        self.max_retries = max_retries
        self.retry_base = retry_base
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
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
                    await sleep_before_retry(
                        attempt + 1, response.headers, base=self.retry_base
                    )
                    continue
                response.raise_for_status()
                return response
            except (httpx.TimeoutException, httpx.NetworkError):
                if attempt >= self.max_retries:
                    raise
                await sleep_before_retry(attempt + 1, base=self.retry_base)
        raise AssertionError("unreachable")

    def _payload(
        self, messages: Sequence[AnyMessage], tools: Sequence[Any] | None, **kwargs: Any
    ) -> dict[str, Any]:
        payload = {
            "model": self.model,
            "messages": [_message(item) for item in messages],
            **self._options,
            **kwargs,
        }
        if tools:
            payload["tools"] = [dict(item) for item in canonicalize_tools(tools)]
        return payload

    def _inferred_prefix(
        self,
        messages: Sequence[AnyMessage],
        tools: Sequence[Any] | None,
    ) -> ImmutablePrefix:
        if self.immutable_prefix is not None:
            return self.immutable_prefix
        leading: list[str] = []
        for message in messages:
            if message.type != "system":
                break
            leading.append(str(message.content))
        return ImmutablePrefix.create(
            system_prompt="\n\n".join(leading),
            tools=tools or (),
        )

    def _cache_wrapper(
        self,
        messages: Sequence[AnyMessage],
        tools: Sequence[Any] | None,
    ) -> CacheFirstChatModel:
        prefix = self._inferred_prefix(messages, tools)
        if self._compat_wrapper is None:
            self._compat_wrapper = CacheFirstChatModel(
                self._raw_model,
                prefix=prefix,
                config=self.cache_first_config,
                usage_ledger=self.usage_ledger,
                pricing=self.pricing,
                provider_id=self.provider_id,
                endpoint_format=self.endpoint_format,
            )
        return self._compat_wrapper

    async def _agenerate_raw(
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
            usage=dict(payload.get("usage") or {}),
            response_metadata={
                "finish_reason": choice.get("finish_reason"),
                "model": payload.get("model"),
            },
        )

    async def agenerate(
        self,
        messages: Sequence[AnyMessage],
        *,
        tools: Sequence[ToolSpec | Any] | None = None,
        **kwargs: Any,
    ) -> AIMessage:
        if not self.cache_first_config.enabled:
            return await self._agenerate_raw(messages, tools=tools, **kwargs)
        effective_tools = tools
        if self.immutable_prefix is None and tools is None:
            effective_tools = ()
        return await self._cache_wrapper(messages, tools).agenerate(
            messages, tools=effective_tools, **kwargs
        )

    async def _astream_raw(
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
                        if (
                            value.content
                            or value.tool_call_chunks
                            or value.usage
                            or choice.get("finish_reason")
                        ):
                            emitted = True
                            yield value
                    return
            except (httpx.TimeoutException, httpx.NetworkError):
                if emitted or attempt >= self.max_retries:
                    raise
                await sleep_before_retry(attempt + 1, base=self.retry_base)

    async def astream(
        self,
        messages: Sequence[AnyMessage],
        *,
        tools: Sequence[ToolSpec | Any] | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[AIMessageChunk]:
        if not self.cache_first_config.enabled:
            async for chunk in self._astream_raw(messages, tools=tools, **kwargs):
                yield chunk
            return
        effective_tools = tools
        if self.immutable_prefix is None and tools is None:
            effective_tools = ()
        async for chunk in self._cache_wrapper(messages, tools).astream(
            messages, tools=effective_tools, **kwargs
        ):
            yield chunk

    async def aclose(self) -> None:
        await self._client.aclose()


__all__ = ["OpenAICompatChatModel"]


class _RawOpenAIModel:
    """Small recursion-free view used by ``CacheFirstChatModel``."""

    def __init__(self, parent: OpenAICompatChatModel) -> None:
        self._parent = parent
        self.model = parent.model
        self.provider_id = parent.provider_id
        self.endpoint_format = parent.endpoint_format

    async def agenerate(self, messages, *, tools=None, **kwargs):
        return await self._parent._agenerate_raw(messages, tools=tools, **kwargs)

    async def astream(self, messages, *, tools=None, **kwargs):
        async for chunk in self._parent._astream_raw(messages, tools=tools, **kwargs):
            yield chunk
