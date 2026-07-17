"""Dependency-free model protocols used by prebuilt agents and integrations."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any, Protocol, runtime_checkable

from .messages import AIMessage, AIMessageChunk, AnyMessage


@runtime_checkable
class ChatModel(Protocol):
    async def agenerate(
        self,
        messages: Sequence[AnyMessage],
        *,
        tools: Sequence[Any] | None = None,
        **kwargs: Any,
    ) -> AIMessage: ...


@runtime_checkable
class StreamingChatModel(ChatModel, Protocol):
    def astream(
        self,
        messages: Sequence[AnyMessage],
        *,
        tools: Sequence[Any] | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[AIMessageChunk]: ...


__all__ = ["ChatModel", "StreamingChatModel"]
