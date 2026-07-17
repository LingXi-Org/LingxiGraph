"""Stateful conversion from LingxiGraph events to UI-neutral projections."""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Iterable, Mapping
from enum import Enum
from typing import Any

from lingxigraph import AIMessage, AIMessageChunk, ToolMessage
from lingxigraph.events import Event, EventKind

from .models import ObservabilityOptions, Projection


def _plain(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Enum):
        return value.value
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {field.name: _plain(getattr(value, field.name)) for field in dataclasses.fields(value)}
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_plain(item) for item in value]
    return repr(value)


def format_payload(value: Any, limit: int) -> str:
    """Render arbitrary safe runtime values as bounded JSON text."""

    rendered = json.dumps(_plain(value), ensure_ascii=False, indent=2, sort_keys=True)
    if len(rendered) <= limit:
        return rendered
    omitted = len(rendered) - limit
    return f"{rendered[:limit]}\n… ({omitted} characters omitted)"


def _kind(value: Any) -> str:
    if isinstance(value, Mapping):
        return str(value.get("type") or value.get("role") or "")
    return str(getattr(value, "type", ""))


def _content(value: Any) -> Any:
    if isinstance(value, Mapping):
        return value.get("content", "")
    return getattr(value, "content", "")


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _messages(update: Any) -> Iterable[Any]:
    if not isinstance(update, Mapping):
        return ()
    values = update.get("messages", ())
    if isinstance(values, (list, tuple)):
        return values
    return (values,)


class EventProjector:
    """Project one embedded graph invocation while suppressing duplicate UI output."""

    def __init__(self, options: ObservabilityOptions | None = None) -> None:
        self.options = options or ObservabilityOptions()
        self._seen: set[tuple[str, int] | tuple[str, str]] = set()
        self._assistant_key: str | None = None
        self._assistant_streamed: set[str] = set()
        self._tools: dict[str, str] = {}
        self._tool_args: dict[str, str] = {}
        self._message_keys: dict[str, str] = {}
        self._tool_indices: dict[tuple[str, int], str] = {}

    def project(self, event: Event) -> list[Projection]:
        identity: tuple[str, int] | tuple[str, str]
        identity = (event.run_id, event.sequence) if event.sequence else (event.run_id, event.event_id)
        if identity in self._seen:
            return []
        self._seen.add(identity)

        metadata = self._metadata(event)
        kind = event.kind
        if kind is EventKind.RUN_STARTED:
            return [
                Projection(
                    "run_start",
                    event.run_id,
                    name="LingxiGraph run",
                    metadata=metadata,
                )
            ]
        if kind in {
            EventKind.RUN_COMPLETED,
            EventKind.RUN_FAILED,
            EventKind.RUN_CANCELLED,
            EventKind.RUN_TIMED_OUT,
            EventKind.RUN_BUDGET_EXCEEDED,
            EventKind.RUN_PAUSED,
        }:
            status = kind.value.removeprefix("run_")
            result = self._finish_assistant()
            result.append(
                Projection(
                    "run_end",
                    event.run_id,
                    content=(
                        format_payload(event.data.get("state"), self.options.max_payload_chars)
                        if self.options.show_state_updates and "state" in event.data
                        else status
                    ),
                    metadata=metadata,
                    status=status,
                    is_error=kind
                    in {
                        EventKind.RUN_FAILED,
                        EventKind.RUN_TIMED_OUT,
                        EventKind.RUN_BUDGET_EXCEEDED,
                    },
                )
            )
            return result
        if kind is EventKind.NODE_STARTED:
            return [
                Projection(
                    "node_start",
                    self._node_key(event),
                    name=event.node or event.task_id or "node",
                    metadata=metadata,
                    status="running",
                )
            ]
        if kind in {
            EventKind.NODE_COMPLETED,
            EventKind.NODE_FAILED,
            EventKind.NODE_RETRYING,
            EventKind.NODE_CACHED,
        }:
            status = {
                EventKind.NODE_COMPLETED: "completed",
                EventKind.NODE_FAILED: "failed",
                EventKind.NODE_RETRYING: "retrying",
                EventKind.NODE_CACHED: "cached",
            }[kind]
            update = event.data.get("update")
            content = None
            if self.options.show_state_updates and update is not None:
                content = format_payload(update, self.options.max_payload_chars)
            elif kind is EventKind.NODE_FAILED:
                content = str(event.data.get("error") or "node failed")
            result = [
                Projection(
                    "node_update",
                    self._node_key(event),
                    name=event.node or event.task_id or "node",
                    content=content,
                    metadata=metadata,
                    status=status,
                    is_error=kind is EventKind.NODE_FAILED,
                )
            ]
            if kind in {EventKind.NODE_COMPLETED, EventKind.NODE_CACHED}:
                result.extend(self._tool_results(update, metadata))
            return result
        if kind is EventKind.MESSAGE:
            return self._message(event, metadata)
        if kind is EventKind.CUSTOM:
            channel = str(event.data.get("channel") or "custom")
            value = event.data.get("value")
            content = (
                format_payload(value, self.options.max_payload_chars)
                if self.options.show_custom_payloads
                else f"{type(value).__name__} payload hidden"
            )
            return [
                Projection(
                    "custom",
                    f"{event.run_id}:{event.sequence}:{channel}",
                    name=channel,
                    content=content,
                    metadata=metadata,
                )
            ]
        if kind is EventKind.INTERRUPT_RAISED:
            result = []
            for index, marker in enumerate(event.data.get("interrupts", ())):
                interrupt_id = str(_field(marker, "id") or f"{event.run_id}:{index}")
                value = _field(marker, "value")
                result.append(
                    Projection(
                        "interrupt",
                        interrupt_id,
                        name="Input required",
                        content=self._interrupt_prompt(value),
                        metadata={
                            **metadata,
                            "interrupt_id": interrupt_id,
                            "resumable": bool(_field(marker, "resumable", True)),
                            "value": _plain(value),
                        },
                    )
                )
            return result
        return []

    def finish(self) -> list[Projection]:
        return self._finish_assistant()

    def _message(self, event: Event, metadata: dict[str, Any]) -> list[Projection]:
        envelope = event.data.get("value")
        if isinstance(envelope, (list, tuple)) and envelope:
            message = envelope[0]
            if len(envelope) > 1 and isinstance(envelope[1], Mapping):
                metadata = {**metadata, **dict(envelope[1])}
        else:
            message = envelope
        message_kind = _kind(message)
        is_chunk = isinstance(message, AIMessageChunk) or message_kind == "ai_chunk"
        is_ai = isinstance(message, AIMessage) or message_kind in {"ai", "assistant"}
        if not (is_chunk or is_ai):
            return []

        stream_base = f"{event.run_id}:{event.step}:{event.task_id or 'assistant'}"
        raw_message_id = _field(message, "id")
        if raw_message_id:
            key = f"{event.run_id}:{event.step}:{raw_message_id}"
            self._message_keys[stream_base] = key
        else:
            key = self._message_keys.get(stream_base, stream_base)
        result: list[Projection] = []
        if self._assistant_key is not None and self._assistant_key != key:
            result.extend(self._finish_assistant())
        content = _content(message)
        if is_chunk:
            self._assistant_key = key
            if content:
                self._assistant_streamed.add(key)
                result.append(
                    Projection(
                        "assistant_token",
                        key,
                        content=str(content),
                        metadata=metadata,
                    )
                )
        elif content and key not in self._assistant_streamed:
            result.append(
                Projection(
                    "assistant_message",
                    key,
                    content=str(content),
                    metadata=metadata,
                )
            )
        calls = _field(message, "tool_call_chunks", ()) if is_chunk else _field(message, "tool_calls", ())
        result.extend(self._tool_calls(calls, key, metadata, chunked=is_chunk))
        return result

    def _tool_calls(
        self,
        calls: Iterable[Any],
        message_key: str,
        metadata: dict[str, Any],
        *,
        chunked: bool,
    ) -> list[Projection]:
        result = []
        for fallback_index, call in enumerate(calls or ()):
            index = int(_field(call, "index", fallback_index))
            index_key = (message_key, index)
            raw_call_id = _field(call, "id")
            if raw_call_id:
                call_id = str(raw_call_id)
                self._tool_indices[index_key] = call_id
            else:
                call_id = self._tool_indices.get(index_key, f"{message_key}:{index}")
            name = str(_field(call, "name") or "tool")
            args = _field(call, "args", "" if chunked else {})
            if chunked:
                self._tool_args[call_id] = self._tool_args.get(call_id, "") + str(args or "")
                raw_input: Any = self._tool_args[call_id]
            else:
                raw_input = args
            tool_key = self._tools.setdefault(call_id, f"tool:{call_id}")
            result.append(
                Projection(
                    "tool_start",
                    tool_key,
                    name=name,
                    content=(
                        format_payload(raw_input, self.options.max_payload_chars)
                        if self.options.show_tool_io
                        else "input hidden"
                    ),
                    metadata={**metadata, "tool_call_id": call_id},
                )
            )
        return result

    def _tool_results(self, update: Any, metadata: dict[str, Any]) -> list[Projection]:
        result = []
        for message in _messages(update):
            if not (isinstance(message, ToolMessage) or _kind(message) == "tool"):
                continue
            call_id = str(_field(message, "tool_call_id") or "")
            if not call_id:
                continue
            tool_key = self._tools.setdefault(call_id, f"tool:{call_id}")
            status = str(_field(message, "status", "success"))
            result.append(
                Projection(
                    "tool_end",
                    tool_key,
                    name=str(_field(message, "name") or "tool"),
                    content=(
                        format_payload(_content(message), self.options.max_payload_chars)
                        if self.options.show_tool_io
                        else "output hidden"
                    ),
                    metadata={**metadata, "tool_call_id": call_id},
                    status=status,
                    is_error=status == "error",
                )
            )
        return result

    def _finish_assistant(self) -> list[Projection]:
        if self._assistant_key is None:
            return []
        key = self._assistant_key
        self._assistant_key = None
        return [Projection("assistant_end", key)]

    @staticmethod
    def _interrupt_prompt(value: Any) -> str:
        if isinstance(value, str):
            return value
        if isinstance(value, Mapping):
            prompt = value.get("question") or value.get("message") or value.get("prompt")
            if prompt:
                return str(prompt)
        return f"The graph requires input:\n```json\n{format_payload(value, 4_000)}\n```"

    @staticmethod
    def _node_key(event: Event) -> str:
        namespace = "/".join(event.namespace)
        return f"{event.run_id}:{event.step}:{namespace}:{event.task_id or event.node or 'node'}"

    @staticmethod
    def _metadata(event: Event) -> dict[str, Any]:
        return {
            "run_id": event.run_id,
            "sequence": event.sequence,
            "step": event.step,
            "node": event.node,
            "namespace": list(event.namespace),
            "task_id": event.task_id,
            "checkpoint_id": event.checkpoint_id,
            "graph_id": event.graph_id,
            "thread_id": event.thread_id,
        }


__all__ = ["EventProjector", "format_payload"]
