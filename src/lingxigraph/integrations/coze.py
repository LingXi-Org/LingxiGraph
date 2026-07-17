"""Coze OpenAPI integration for durable bot and workflow orchestration."""

from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from ..messages import (
    AIMessage,
    AIMessageChunk,
    AnyMessage,
    ToolCall,
    ToolMessage,
)
from ..runtime import Runtime
from ..tools import ToolSpec, as_tool_spec
from ..types import interrupt

try:
    import httpx
except ImportError as exc:  # pragma: no cover
    raise RuntimeError("install lingxigraph[coze] to use the Coze integration") from exc


_ENDPOINTS = {
    "chat": "/v3/chat",
    "chat_retrieve": "/v3/chat/retrieve",
    "chat_messages": "/v3/chat/message/list",
    "submit_tool_outputs": "/v3/chat/submit_tool_outputs",
    "cancel_chat": "/v3/chat/cancel",
    "conversation": "/v1/conversation/create",
    "workflow": "/v1/workflow/run",
    "workflow_stream": "/v1/workflow/stream_run",
    "workflow_resume": "/v1/workflow/stream_resume",
}


async def _iter_sse(response: httpx.Response) -> AsyncIterator[dict[str, Any]]:
    event = "message"
    data: list[str] = []
    async for line in response.aiter_lines():
        if not line:
            if data:
                raw = "\n".join(data)
                if raw == "[DONE]":
                    return
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    payload = {"data": raw}
                if isinstance(payload, Mapping):
                    yield {"event": event, "data": dict(payload)}
                else:
                    yield {"event": event, "data": payload}
            event, data = "message", []
            continue
        if line.startswith("event:"):
            event = line[6:].strip()
        elif line.startswith("data:"):
            data.append(line[5:].strip())
    if data:
        raw = "\n".join(data)
        if raw != "[DONE]":
            payload = json.loads(raw)
            yield {"event": event, "data": payload}


TokenProvider = Callable[[], str | Awaitable[str]]


class AsyncCozeClient:
    """Small, dependency-controlled client for the Coze v3 chat and v1 workflow APIs."""

    def __init__(
        self,
        api_token: str | None = None,
        *,
        token_provider: TokenProvider | None = None,
        base_url: str = "https://api.coze.cn",
        timeout: float = 60.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if not api_token and token_provider is None:
            raise ValueError("api_token or token_provider is required")
        self._token = api_token
        self._token_provider = token_provider
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"), timeout=timeout, transport=transport
        )

    async def _headers(self) -> dict[str, str]:
        token = self._token
        if token is None and self._token_provider is not None:
            provided = self._token_provider()
            token = await provided if inspect.isawaitable(provided) else provided
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    async def _request(self, method: str, endpoint: str, **kwargs: Any) -> dict[str, Any]:
        response = await self._client.request(
            method, _ENDPOINTS[endpoint], headers=await self._headers(), **kwargs
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, Mapping) and payload.get("code") not in (None, 0):
            raise RuntimeError(f"Coze API error {payload.get('code')}: {payload.get('msg')}")
        data = payload.get("data", payload) if isinstance(payload, Mapping) else payload
        return dict(data) if isinstance(data, Mapping) else {"items": data}

    async def chat(
        self,
        bot_id: str,
        user_id: str,
        *,
        additional_messages: Sequence[Mapping[str, Any]] = (),
        conversation_id: str | None = None,
        stream: bool = False,
        auto_save_history: bool = True,
    ) -> dict[str, Any]:
        params = {"conversation_id": conversation_id} if conversation_id else None
        return await self._request(
            "POST",
            "chat",
            params=params,
            json={
                "bot_id": bot_id,
                "user_id": user_id,
                "additional_messages": list(additional_messages),
                "stream": stream,
                "auto_save_history": auto_save_history,
            },
        )

    async def chat_stream(self, bot_id: str, user_id: str, **kwargs: Any) -> AsyncIterator[dict[str, Any]]:
        conversation_id = kwargs.pop("conversation_id", None)
        payload = {
            "bot_id": bot_id,
            "user_id": user_id,
            "additional_messages": list(kwargs.pop("additional_messages", ())),
            "stream": True,
            "auto_save_history": kwargs.pop("auto_save_history", True),
            **kwargs,
        }
        params = {"conversation_id": conversation_id} if conversation_id else None
        async with self._client.stream(
            "POST",
            _ENDPOINTS["chat"],
            params=params,
            json=payload,
            headers=await self._headers(),
        ) as response:
            response.raise_for_status()
            async for event in _iter_sse(response):
                yield event

    async def chat_retrieve(self, conversation_id: str, chat_id: str) -> dict[str, Any]:
        return await self._request(
            "GET", "chat_retrieve", params={"conversation_id": conversation_id, "chat_id": chat_id}
        )

    async def chat_messages(self, conversation_id: str, chat_id: str) -> list[dict[str, Any]]:
        data = await self._request(
            "GET", "chat_messages", params={"conversation_id": conversation_id, "chat_id": chat_id}
        )
        return list(data.get("items", data.get("data", [])))

    async def submit_tool_outputs(
        self, conversation_id: str, chat_id: str, tool_outputs: Sequence[Mapping[str, Any]]
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            "submit_tool_outputs",
            params={"conversation_id": conversation_id, "chat_id": chat_id},
            json={"tool_outputs": list(tool_outputs)},
        )

    async def cancel_chat(self, conversation_id: str, chat_id: str) -> dict[str, Any]:
        return await self._request(
            "POST", "cancel_chat", params={"conversation_id": conversation_id, "chat_id": chat_id}
        )

    async def create_conversation(self, messages: Sequence[Mapping[str, Any]] = ()) -> dict[str, Any]:
        return await self._request("POST", "conversation", json={"messages": list(messages)})

    async def workflow_run(self, workflow_id: str, parameters: Mapping[str, Any], **kwargs: Any) -> dict[str, Any]:
        return await self._request(
            "POST", "workflow", json={"workflow_id": workflow_id, "parameters": dict(parameters), **kwargs}
        )

    async def workflow_stream(
        self, workflow_id: str, parameters: Mapping[str, Any], **kwargs: Any
    ) -> AsyncIterator[dict[str, Any]]:
        async with self._client.stream(
            "POST",
            _ENDPOINTS["workflow_stream"],
            json={"workflow_id": workflow_id, "parameters": dict(parameters), **kwargs},
            headers=await self._headers(),
        ) as response:
            response.raise_for_status()
            async for event in _iter_sse(response):
                yield event

    async def workflow_stream_resume(
        self, workflow_id: str, event_id: str, interrupt_type: int | str, resume_data: Any
    ) -> AsyncIterator[dict[str, Any]]:
        async with self._client.stream(
            "POST",
            _ENDPOINTS["workflow_resume"],
            json={
                "workflow_id": workflow_id,
                "event_id": event_id,
                "interrupt_type": interrupt_type,
                "resume_data": resume_data,
            },
            headers=await self._headers(),
        ) as response:
            response.raise_for_status()
            async for event in _iter_sse(response):
                yield event

    async def aclose(self) -> None:
        await self._client.aclose()


def _message_to_coze(message: AnyMessage) -> dict[str, Any]:
    role = {"human": "user", "ai": "assistant"}.get(message.type, message.type)
    data: dict[str, Any] = {"role": role, "content": message.content, "content_type": "text"}
    if isinstance(message, ToolMessage):
        data["tool_call_id"] = message.tool_call_id
    return data


def _extract_tool_calls(data: Mapping[str, Any]) -> tuple[ToolCall, ...]:
    action = data.get("required_action", data)
    submit = action.get("submit_tool_outputs", action) if isinstance(action, Mapping) else {}
    raw_calls = submit.get("tool_calls", ()) if isinstance(submit, Mapping) else ()
    calls = []
    for raw in raw_calls:
        function = raw.get("function", raw)
        arguments = function.get("arguments", {})
        if isinstance(arguments, str):
            try:
                arguments = json.loads(arguments)
            except json.JSONDecodeError:
                arguments = {"raw": arguments}
        calls.append(ToolCall(str(function.get("name", "")), dict(arguments), str(raw.get("id", ""))))
    return tuple(calls)


@dataclass(slots=True)
class CozeAgentNode:
    bot_id: str
    client: AsyncCozeClient
    user_id: str
    messages_key: str = "messages"
    conversation_key: str = "coze_conversations"
    stream: bool = True
    tools: Sequence[ToolSpec | Callable[..., Any]] | None = None
    hitl: bool = False
    max_tool_rounds: int = 5

    async def __call__(self, state: Mapping[str, Any], runtime: Runtime[Any]) -> Mapping[str, Any]:
        conversations = dict(state.get(self.conversation_key, {}))
        conversation_id = conversations.get(self.bot_id)
        messages = [_message_to_coze(item) for item in state.get(self.messages_key, ())]
        content = ""
        chat_id: str | None = None
        tool_calls: tuple[ToolCall, ...] = ()
        if self.stream:
            async for event in self.client.chat_stream(
                self.bot_id,
                self.user_id,
                additional_messages=messages,
                conversation_id=conversation_id,
            ):
                runtime.raise_if_cancelled()
                kind, data = event["event"], event.get("data", {})
                if isinstance(data, Mapping):
                    chat_id = str(data.get("id") or data.get("chat_id") or chat_id or "") or None
                    conversation_id = str(data.get("conversation_id") or conversation_id or "") or None
                if kind == "conversation.message.delta":
                    delta = str(data.get("content", ""))
                    content += delta
                    runtime.emit_message(AIMessageChunk(delta, id=chat_id), {"provider": "coze"})
                elif kind == "conversation.chat.requires_action":
                    tool_calls = _extract_tool_calls(data)
                elif kind in {"conversation.chat.failed", "error"}:
                    raise RuntimeError(f"Coze chat failed: {data}")
        else:
            chat = await self.client.chat(
                self.bot_id,
                self.user_id,
                additional_messages=messages,
                conversation_id=conversation_id,
            )
            chat_id = str(chat.get("id") or chat.get("chat_id", ""))
            conversation_id = str(chat.get("conversation_id") or conversation_id or "")
            while chat.get("status") in {"created", "in_progress"}:
                await asyncio.sleep(0.25)
                chat = await self.client.chat_retrieve(conversation_id, chat_id)
            if chat.get("status") == "requires_action":
                tool_calls = _extract_tool_calls(chat)
            else:
                items = await self.client.chat_messages(conversation_id, chat_id)
                content = "".join(
                    str(item.get("content", "")) for item in items if item.get("role") == "assistant"
                )
        if runtime.cancelled and conversation_id and chat_id:
            await self.client.cancel_chat(conversation_id, chat_id)
        if tool_calls:
            if self.hitl:
                decision = interrupt(
                    {
                        "type": "coze_tool_approval",
                        "conversation_id": conversation_id,
                        "chat_id": chat_id,
                        "tool_calls": tool_calls,
                    }
                )
                if not isinstance(decision, Mapping) or decision.get("action") != "approve":
                    return {self.messages_key: [AIMessage("Tool calls rejected.")]}
            if self.tools:
                by_name = {spec.name: spec for spec in (as_tool_spec(item) for item in self.tools)}
                for _round in range(self.max_tool_rounds):
                    outputs = []
                    for call in tool_calls:
                        spec = by_name.get(call.name)
                        if spec is None:
                            output = f"unknown local tool {call.name}"
                        elif inspect.iscoroutinefunction(spec.func):
                            output = await spec.func(**dict(call.args))
                        else:
                            output = await asyncio.to_thread(spec.func, **dict(call.args))
                        if inspect.isawaitable(output):
                            output = await output
                        outputs.append({"tool_call_id": call.id, "output": str(output)})
                    if not conversation_id or not chat_id:
                        break
                    chat = await self.client.submit_tool_outputs(
                        conversation_id, chat_id, outputs
                    )
                    while chat.get("status") in {"created", "in_progress"}:
                        await asyncio.sleep(0.25)
                        chat = await self.client.chat_retrieve(conversation_id, chat_id)
                    if chat.get("status") == "requires_action":
                        tool_calls = _extract_tool_calls(chat)
                        continue
                    items = await self.client.chat_messages(conversation_id, chat_id)
                    content = "".join(
                        str(item.get("content", ""))
                        for item in items
                        if item.get("role") == "assistant"
                    )
                    tool_calls = ()
                    break
                else:
                    raise RuntimeError(
                        f"Coze tool loop exceeded max_tool_rounds={self.max_tool_rounds}"
                    )
        if conversation_id:
            conversations[self.bot_id] = conversation_id
        return {
            self.messages_key: [
                AIMessage(
                    content,
                    tool_calls=tool_calls,
                    response_metadata={"conversation_id": conversation_id, "chat_id": chat_id},
                )
            ],
            self.conversation_key: conversations,
        }


@dataclass(slots=True)
class CozeWorkflowNode:
    workflow_id: str
    client: AsyncCozeClient
    parameters: Mapping[str, Any] | Callable[[Mapping[str, Any]], Mapping[str, Any]]
    output_key: str = "workflow_output"
    stream: bool = True

    async def __call__(self, state: Mapping[str, Any], runtime: Runtime[Any]) -> Mapping[str, Any]:
        parameters = self.parameters(state) if callable(self.parameters) else self.parameters
        if not self.stream:
            return {self.output_key: await self.client.workflow_run(self.workflow_id, parameters)}
        output: Any = None
        iterator = self.client.workflow_stream(self.workflow_id, parameters)
        async for event in iterator:
            runtime.raise_if_cancelled()
            data = event.get("data", {})
            if event["event"] in {"interrupt", "workflow.interrupt"}:
                answer = interrupt(
                    {
                        "type": "coze_workflow_question",
                        "event_id": data.get("event_id"),
                        "interrupt_type": data.get("interrupt_type"),
                        "message": data.get("message"),
                    }
                )
                if not isinstance(answer, Mapping):
                    raise ValueError("workflow resume must echo event_id, interrupt_type, and resume_data")
                iterator = self.client.workflow_stream_resume(
                    self.workflow_id,
                    str(answer["event_id"]),
                    answer["interrupt_type"],
                    answer.get("resume_data"),
                )
                async for resumed in iterator:
                    output = resumed.get("data", output)
                break
            output = data
            runtime.emit("custom", {"provider": "coze", "workflow": data})
        return {self.output_key: output}


class CozeChatModel:
    def __init__(self, bot_id: str, *, client: AsyncCozeClient, user_id: str) -> None:
        self.bot_id, self.client, self.user_id = bot_id, client, user_id

    async def agenerate(self, messages: Sequence[AnyMessage], *, tools=None, **kwargs: Any) -> AIMessage:
        del tools
        last_ai_index = next(
            (index for index in range(len(messages) - 1, -1, -1) if isinstance(messages[index], AIMessage)),
            None,
        )
        if last_ai_index is not None:
            previous = messages[last_ai_index]
            assert isinstance(previous, AIMessage)
            responses = messages[last_ai_index + 1 :]
            conversation_id = previous.response_metadata.get("conversation_id")
            chat_id = previous.response_metadata.get("chat_id")
            if (
                previous.tool_calls
                and responses
                and all(isinstance(item, ToolMessage) for item in responses)
                and conversation_id
                and chat_id
            ):
                chat = await self.client.submit_tool_outputs(
                    str(conversation_id),
                    str(chat_id),
                    [
                        {"tool_call_id": item.tool_call_id, "output": str(item.content)}
                        for item in responses
                        if isinstance(item, ToolMessage)
                    ],
                )
                while chat.get("status") in {"created", "in_progress"}:
                    await asyncio.sleep(0.25)
                    chat = await self.client.chat_retrieve(str(conversation_id), str(chat_id))
                if chat.get("status") == "requires_action":
                    return AIMessage(
                        "",
                        tool_calls=_extract_tool_calls(chat),
                        response_metadata={
                            "conversation_id": conversation_id,
                            "chat_id": chat_id,
                        },
                    )
                items = await self.client.chat_messages(str(conversation_id), str(chat_id))
                return AIMessage(
                    "".join(
                        str(item.get("content", ""))
                        for item in items
                        if item.get("role") == "assistant"
                    ),
                    response_metadata={
                        "conversation_id": conversation_id,
                        "chat_id": chat_id,
                    },
                )
        content = ""
        calls: tuple[ToolCall, ...] = ()
        metadata: dict[str, Any] = {}
        async for event in self.client.chat_stream(
            self.bot_id,
            self.user_id,
            additional_messages=[_message_to_coze(item) for item in messages],
            **kwargs,
        ):
            data = event.get("data", {})
            if event["event"] == "conversation.message.delta":
                content += str(data.get("content", ""))
            elif event["event"] == "conversation.chat.requires_action":
                calls = _extract_tool_calls(data)
            if isinstance(data, Mapping):
                metadata.update(
                    {key: data[key] for key in ("conversation_id", "chat_id") if key in data}
                )
        return AIMessage(content, tool_calls=calls, response_metadata=metadata)

    async def astream(
        self, messages: Sequence[AnyMessage], *, tools=None, **kwargs: Any
    ) -> AsyncIterator[AIMessageChunk]:
        del tools
        async for event in self.client.chat_stream(
            self.bot_id,
            self.user_id,
            additional_messages=[_message_to_coze(item) for item in messages],
            **kwargs,
        ):
            if event["event"] == "conversation.message.delta":
                data = event.get("data", {})
                yield AIMessageChunk(str(data.get("content", "")), id=data.get("id"))


__all__ = [
    "AsyncCozeClient",
    "CozeAgentNode",
    "CozeChatModel",
    "CozeWorkflowNode",
]
