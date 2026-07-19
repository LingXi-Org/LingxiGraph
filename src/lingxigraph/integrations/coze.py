"""Coze OpenAPI integration for durable bot and workflow orchestration."""

from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from ..errors import GraphCancelledError
from ..messages import (
    AIMessage,
    AIMessageChunk,
    AnyMessage,
    ToolCall,
    ToolMessage,
)
from ..runtime import Runtime, get_runtime
from ..tools import ToolNode, ToolSpec
from ..types import interrupt
from ._http import should_retry_status, sleep_before_retry

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
    "conversation_retrieve": "/v1/conversation/retrieve",
    "conversation_message_create": "/v1/conversation/message/create",
    "conversation_message_list": "/v1/conversation/message/list",
    "conversation_message_retrieve": "/v1/conversation/message/retrieve",
    "files_upload": "/v1/files/upload",
    "files_retrieve": "/v1/files/retrieve",
    "bot_retrieve": "/v1/bot/get_online_info",
    "workflow": "/v1/workflow/run",
    "workflow_stream": "/v1/workflow/stream_run",
    "workflow_resume": "/v1/workflow/stream_resume",
}


async def _iter_sse(response: httpx.Response) -> AsyncIterator[dict[str, Any]]:
    event = "message"
    event_id: str | None = None
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
                    yield {"event": event, "data": dict(payload), "id": event_id}
                else:
                    yield {"event": event, "data": payload, "id": event_id}
            event, event_id, data = "message", None, []
            continue
        if line.startswith("event:"):
            event = line[6:].strip()
        elif line.startswith("id:"):
            event_id = line[3:].strip()
        elif line.startswith("data:"):
            data.append(line[5:].strip())
    if data:
        raw = "\n".join(data)
        if raw != "[DONE]":
            payload = json.loads(raw)
            yield {"event": event, "data": payload, "id": event_id}


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
        max_retries: int = 3,
        retry_base: float = 0.5,
    ) -> None:
        if not api_token and token_provider is None:
            raise ValueError("api_token or token_provider is required")
        self._token = api_token
        self._token_provider = token_provider
        if max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        self.max_retries = max_retries
        self.retry_base = retry_base
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"), timeout=timeout, transport=transport
        )

    async def _headers(
        self,
        *,
        operation_key: str,
        last_event_id: str | None = None,
        content_type: str | None = "application/json",
    ) -> dict[str, str]:
        token = self._token
        if token is None and self._token_provider is not None:
            provided = self._token_provider()
            token = await provided if inspect.isawaitable(provided) else provided
        headers = {"Authorization": f"Bearer {token}"}
        if content_type is not None:
            headers["Content-Type"] = content_type
        try:
            runtime = get_runtime()
            runtime.raise_if_cancelled()
            headers["X-Idempotency-Key"] = runtime.idempotency_key or operation_key
        except RuntimeError:
            headers["X-Idempotency-Key"] = operation_key
        if last_event_id:
            headers["Last-Event-ID"] = last_event_id
        return headers

    async def _request(self, method: str, endpoint: str, **kwargs: Any) -> dict[str, Any]:
        response: httpx.Response | None = None
        operation_key = str(uuid4())
        # httpx sets the multipart Content-Type (with boundary) itself, so drop ours.
        content_type = None if "files" in kwargs else "application/json"
        for attempt in range(self.max_retries + 1):
            try:
                response = await self._client.request(
                    method,
                    _ENDPOINTS[endpoint],
                    headers=await self._headers(
                        operation_key=operation_key, content_type=content_type
                    ),
                    **kwargs,
                )
                if should_retry_status(response.status_code) and attempt < self.max_retries:
                    await sleep_before_retry(attempt + 1, response.headers, base=self.retry_base)
                    continue
                response.raise_for_status()
                break
            except (httpx.TimeoutException, httpx.NetworkError):
                if attempt >= self.max_retries:
                    raise
                await sleep_before_retry(attempt + 1, base=self.retry_base)
        assert response is not None
        payload = response.json()
        if isinstance(payload, Mapping) and payload.get("code") not in (None, 0):
            raise RuntimeError(f"Coze API error {payload.get('code')}: {payload.get('msg')}")
        data = payload.get("data", payload) if isinstance(payload, Mapping) else payload
        return dict(data) if isinstance(data, Mapping) else {"items": data}

    async def _stream(
        self,
        endpoint: str,
        *,
        json_body: Mapping[str, Any],
        params: Mapping[str, Any] | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        last_event_id: str | None = None
        seen: set[str] = set()
        operation_key = str(uuid4())
        for attempt in range(self.max_retries + 1):
            try:
                async with self._client.stream(
                    "POST",
                    _ENDPOINTS[endpoint],
                    params=params,
                    json=dict(json_body),
                    headers=await self._headers(
                        operation_key=operation_key,
                        last_event_id=last_event_id,
                    ),
                ) as response:
                    if should_retry_status(response.status_code) and attempt < self.max_retries:
                        await response.aread()
                        await sleep_before_retry(
                            attempt + 1, response.headers, base=self.retry_base
                        )
                        continue
                    response.raise_for_status()
                    async for event in _iter_sse(response):
                        try:
                            get_runtime().raise_if_cancelled()
                        except RuntimeError:
                            pass
                        identifier = str(event.get("id") or "")
                        if identifier:
                            last_event_id = identifier
                            if identifier in seen:
                                continue
                            seen.add(identifier)
                        yield event
                    return
            except (httpx.TimeoutException, httpx.NetworkError):
                if attempt >= self.max_retries:
                    raise
                await sleep_before_retry(attempt + 1, base=self.retry_base)

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
        async for event in self._stream("chat", json_body=payload, params=params):
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

    async def conversation_retrieve(self, conversation_id: str) -> dict[str, Any]:
        return await self._request(
            "GET", "conversation_retrieve", params={"conversation_id": conversation_id}
        )

    async def conversation_message_create(
        self, conversation_id: str, *, role: str, content: str, content_type: str = "text"
    ) -> dict[str, Any]:
        """Append a message to a conversation without triggering a chat run."""
        return await self._request(
            "POST",
            "conversation_message_create",
            params={"conversation_id": conversation_id},
            json={"role": role, "content": content, "content_type": content_type},
        )

    async def conversation_message_list(
        self,
        conversation_id: str,
        *,
        order: str = "desc",
        chat_id: str | None = None,
        before_id: str | None = None,
        after_id: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        """List messages in a conversation with cursor pagination.

        Returns a mapping with ``items``, ``first_id``, ``last_id`` and ``has_more``.
        """
        body: dict[str, Any] = {"order": order, "limit": limit}
        if chat_id:
            body["chat_id"] = chat_id
        if before_id:
            body["before_id"] = before_id
        if after_id:
            body["after_id"] = after_id
        return await self._request(
            "POST",
            "conversation_message_list",
            params={"conversation_id": conversation_id},
            json=body,
        )

    async def conversation_message_retrieve(
        self, conversation_id: str, message_id: str
    ) -> dict[str, Any]:
        return await self._request(
            "GET",
            "conversation_message_retrieve",
            params={"conversation_id": conversation_id, "message_id": message_id},
        )

    async def file_retrieve(self, file_id: str) -> dict[str, Any]:
        return await self._request("GET", "files_retrieve", params={"file_id": file_id})

    async def bot_retrieve(self, bot_id: str) -> dict[str, Any]:
        return await self._request("GET", "bot_retrieve", params={"bot_id": bot_id})

    async def upload_file(
        self,
        content: bytes,
        *,
        filename: str,
        content_type: str = "application/octet-stream",
    ) -> dict[str, Any]:
        """Upload a file to Coze and return its metadata (including ``id``).

        The returned ``id`` is referenced from ``additional_messages`` via an
        ``object_string`` content item (see :func:`file_object` /
        :func:`image_object`).
        """

        return await self._request(
            "POST",
            "files_upload",
            files={"file": (filename, content, content_type)},
        )

    async def workflow_run(self, workflow_id: str, parameters: Mapping[str, Any], **kwargs: Any) -> dict[str, Any]:
        return await self._request(
            "POST", "workflow", json={"workflow_id": workflow_id, "parameters": dict(parameters), **kwargs}
        )

    async def workflow_stream(
        self, workflow_id: str, parameters: Mapping[str, Any], **kwargs: Any
    ) -> AsyncIterator[dict[str, Any]]:
        async for event in self._stream(
            "workflow_stream",
            json_body={"workflow_id": workflow_id, "parameters": dict(parameters), **kwargs},
        ):
            yield event

    async def workflow_stream_resume(
        self, workflow_id: str, event_id: str, interrupt_type: int | str, resume_data: Any
    ) -> AsyncIterator[dict[str, Any]]:
        async for event in self._stream(
            "workflow_resume",
            json_body={
                "workflow_id": workflow_id,
                "event_id": event_id,
                "interrupt_type": interrupt_type,
                "resume_data": resume_data,
            },
        ):
            yield event

    async def aclose(self) -> None:
        await self._client.aclose()


def file_object(file_id: str) -> dict[str, str]:
    """Build a Coze ``object_string`` item that references an uploaded file."""

    return {"type": "file", "file_id": str(file_id)}


def image_object(file_id: str) -> dict[str, str]:
    """Build a Coze ``object_string`` item that references an uploaded image."""

    return {"type": "image", "file_id": str(file_id)}


def text_object(text: str) -> dict[str, str]:
    """Build a Coze ``object_string`` text item."""

    return {"type": "text", "text": str(text)}


def _message_to_coze(message: AnyMessage) -> dict[str, Any]:
    role = {"human": "user", "ai": "assistant"}.get(message.type, message.type)
    # Multimodal messages carry object_string items in additional_kwargs["objects"];
    # when present we emit a Coze object_string message (text + file/image refs).
    objects = message.additional_kwargs.get("objects")
    if objects:
        items = list(objects)
        text = str(message.content or "")
        if text and not any(item.get("type") == "text" for item in items):
            items = [text_object(text), *items]
        data: dict[str, Any] = {
            "role": role,
            "content": json.dumps(items, ensure_ascii=False),
            "content_type": "object_string",
        }
    else:
        data = {"role": role, "content": message.content, "content_type": "text"}
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


def _is_answer_delta(data: Mapping[str, Any]) -> bool:
    """True if a message.delta/completed event carries the visible answer text.

    Coze multiplexes ``function_call``, ``tool_output``, ``verbose`` (e.g. multi-agent
    jump info) and ``knowledge_recall`` payloads onto the same event names as the
    real answer; only ``type in (None, "answer")`` should be shown to the user.
    """

    return data.get("type") in (None, "answer")


def _extract_follow_up(data: Mapping[str, Any]) -> str | None:
    """Return the follow-up question text from a completed message event, if any."""

    if data.get("type") != "follow_up":
        return None
    content = str(data.get("content", "")).strip()
    return content or None


def _split_answer_and_follow_ups(
    items: Sequence[Mapping[str, Any]],
) -> tuple[str, list[str]]:
    """Split polled chat messages into the assistant answer and follow-up prompts."""

    answer_parts: list[str] = []
    follow_ups: list[str] = []
    for item in items:
        if item.get("role") != "assistant":
            continue
        if item.get("type") == "follow_up":
            text = str(item.get("content", "")).strip()
            if text:
                follow_ups.append(text)
        elif item.get("type") in (None, "answer"):
            answer_parts.append(str(item.get("content", "")))
    return "".join(answer_parts), follow_ups


@dataclass(slots=True)
class CozeAgentNode:
    bot_id: str
    client: AsyncCozeClient
    user_id: str
    messages_key: str = "messages"
    conversation_key: str = "coze_conversations"
    # State key for follow-up suggestions. Only written when the graph's state
    # schema declares it (opt-in), so strict schemas are never broken.
    suggestions_key: str | None = None
    stream: bool = True
    tools: Sequence[ToolSpec | Callable[..., Any]] | None = None
    hitl: bool = False
    max_tool_rounds: int = 5
    tool_authorize: Callable[..., Any] | None = None
    secret_resolver: Callable[[str], Any] | None = None

    async def __call__(self, state: Mapping[str, Any], runtime: Runtime[Any]) -> Mapping[str, Any]:
        conversations = dict(state.get(self.conversation_key, {}))
        conversation_id = conversations.get(self.bot_id)
        messages = [_message_to_coze(item) for item in state.get(self.messages_key, ())]
        content = ""
        reasoning = ""
        suggestions: list[str] = []
        chat_id: str | None = None
        tool_calls: tuple[ToolCall, ...] = ()
        usage: dict[str, Any] = {}
        if self.stream:
            try:
                async for event in self.client.chat_stream(
                    self.bot_id,
                    self.user_id,
                    additional_messages=messages,
                    conversation_id=conversation_id,
                ):
                    kind, data = event["event"], event.get("data", {})
                    if isinstance(data, Mapping):
                        chat_id = str(data.get("id") or data.get("chat_id") or chat_id or "") or None
                        conversation_id = str(
                            data.get("conversation_id") or conversation_id or ""
                        ) or None
                    runtime.raise_if_cancelled()
                    if kind == "conversation.message.delta":
                        reasoning_delta = str(data.get("reasoning_content", "") or "")
                        if reasoning_delta:
                            reasoning += reasoning_delta
                            runtime.emit_message(
                                AIMessageChunk(
                                    reasoning_delta,
                                    id=chat_id,
                                    additional_kwargs={"reasoning": True},
                                ),
                                {"provider": "coze", "channel": "reasoning"},
                            )
                        delta = str(data.get("content", "")) if _is_answer_delta(data) else ""
                        if delta:
                            content += delta
                            runtime.emit_message(
                                AIMessageChunk(delta, id=chat_id), {"provider": "coze"}
                            )
                    elif kind == "conversation.message.completed":
                        suggestion = _extract_follow_up(data)
                        if suggestion is not None:
                            suggestions.append(suggestion)
                    elif kind == "conversation.chat.completed":
                        raw_usage = data.get("usage") if isinstance(data, Mapping) else None
                        if isinstance(raw_usage, Mapping):
                            usage = dict(raw_usage)
                    elif kind == "conversation.chat.requires_action":
                        tool_calls = _extract_tool_calls(data)
                    elif kind in {"conversation.chat.failed", "error"}:
                        raise RuntimeError(f"Coze chat failed: {data}")
            except GraphCancelledError:
                if conversation_id and chat_id:
                    await asyncio.shield(self.client.cancel_chat(conversation_id, chat_id))
                raise
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
                if runtime.cancelled:
                    await asyncio.shield(self.client.cancel_chat(conversation_id, chat_id))
                runtime.raise_if_cancelled()
                chat = await self.client.chat_retrieve(conversation_id, chat_id)
            if chat.get("status") == "requires_action":
                tool_calls = _extract_tool_calls(chat)
            else:
                if isinstance(chat.get("usage"), Mapping):
                    usage = dict(chat["usage"])
                items = await self.client.chat_messages(conversation_id, chat_id)
                content, suggestions = _split_answer_and_follow_ups(items)
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
                tool_node = ToolNode(
                    self.tools,
                    authorize=self.tool_authorize,
                    secret_resolver=self.secret_resolver,
                )
                for _round in range(self.max_tool_rounds):
                    executed = await tool_node(
                        {"messages": [AIMessage("", tool_calls=tool_calls)]}
                    )
                    if not isinstance(executed, Mapping):
                        raise RuntimeError("Coze local tools may not return graph Commands")
                    outputs = [
                        {"tool_call_id": item.tool_call_id, "output": str(item.content)}
                        for item in executed.get("messages", ())
                        if isinstance(item, ToolMessage)
                    ]
                    if not conversation_id or not chat_id:
                        break
                    chat = await self.client.submit_tool_outputs(
                        conversation_id, chat_id, outputs
                    )
                    while chat.get("status") in {"created", "in_progress"}:
                        await asyncio.sleep(0.25)
                        if runtime.cancelled:
                            await asyncio.shield(
                                self.client.cancel_chat(conversation_id, chat_id)
                            )
                        runtime.raise_if_cancelled()
                        chat = await self.client.chat_retrieve(conversation_id, chat_id)
                    if chat.get("status") == "requires_action":
                        tool_calls = _extract_tool_calls(chat)
                        continue
                    if isinstance(chat.get("usage"), Mapping):
                        usage = dict(chat["usage"])
                    items = await self.client.chat_messages(conversation_id, chat_id)
                    content, suggestions = _split_answer_and_follow_ups(items)
                    tool_calls = ()
                    break
                else:
                    raise RuntimeError(
                        f"Coze tool loop exceeded max_tool_rounds={self.max_tool_rounds}"
                    )
        if conversation_id:
            conversations[self.bot_id] = conversation_id
        additional_kwargs: dict[str, Any] = {}
        if reasoning:
            additional_kwargs["reasoning_content"] = reasoning
        if suggestions:
            additional_kwargs["follow_ups"] = tuple(suggestions)
        result: dict[str, Any] = {
            self.messages_key: [
                AIMessage(
                    content,
                    tool_calls=tool_calls,
                    usage=usage,
                    additional_kwargs=additional_kwargs,
                    response_metadata={
                        "conversation_id": conversation_id,
                        "chat_id": chat_id,
                        "follow_ups": tuple(suggestions),
                    },
                )
            ],
            self.conversation_key: conversations,
        }
        if self.suggestions_key is not None:
            result[self.suggestions_key] = tuple(suggestions)
        return result


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
                answer, follow_ups = _split_answer_and_follow_ups(items)
                return AIMessage(
                    answer,
                    usage=dict(chat["usage"]) if isinstance(chat.get("usage"), Mapping) else {},
                    additional_kwargs={"follow_ups": tuple(follow_ups)} if follow_ups else {},
                    response_metadata={
                        "conversation_id": conversation_id,
                        "chat_id": chat_id,
                        "follow_ups": tuple(follow_ups),
                    },
                )
        content = ""
        reasoning = ""
        follow_ups: list[str] = []
        calls: tuple[ToolCall, ...] = ()
        metadata: dict[str, Any] = {}
        usage: dict[str, Any] = {}
        async for event in self.client.chat_stream(
            self.bot_id,
            self.user_id,
            additional_messages=[_message_to_coze(item) for item in messages],
            **kwargs,
        ):
            data = event.get("data", {})
            if event["event"] == "conversation.message.delta":
                if _is_answer_delta(data):
                    content += str(data.get("content", ""))
                reasoning += str(data.get("reasoning_content", "") or "")
            elif event["event"] == "conversation.message.completed":
                suggestion = _extract_follow_up(data)
                if suggestion is not None:
                    follow_ups.append(suggestion)
            elif event["event"] == "conversation.chat.completed":
                raw_usage = data.get("usage") if isinstance(data, Mapping) else None
                if isinstance(raw_usage, Mapping):
                    usage = dict(raw_usage)
            elif event["event"] == "conversation.chat.requires_action":
                calls = _extract_tool_calls(data)
            if isinstance(data, Mapping):
                metadata.update(
                    {key: data[key] for key in ("conversation_id", "chat_id") if key in data}
                )
        additional_kwargs: dict[str, Any] = {}
        if reasoning:
            additional_kwargs["reasoning_content"] = reasoning
        if follow_ups:
            additional_kwargs["follow_ups"] = tuple(follow_ups)
        metadata["follow_ups"] = tuple(follow_ups)
        return AIMessage(
            content,
            tool_calls=calls,
            usage=usage,
            additional_kwargs=additional_kwargs,
            response_metadata=metadata,
        )

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
                reasoning_delta = str(data.get("reasoning_content", "") or "")
                if reasoning_delta:
                    yield AIMessageChunk(
                        reasoning_delta,
                        id=data.get("id"),
                        additional_kwargs={"reasoning": True},
                    )
                content_delta = str(data.get("content", "")) if _is_answer_delta(data) else ""
                if content_delta:
                    yield AIMessageChunk(content_delta, id=data.get("id"))


__all__ = [
    "AsyncCozeClient",
    "CozeAgentNode",
    "CozeChatModel",
    "CozeWorkflowNode",
    "file_object",
    "image_object",
    "text_object",
]
