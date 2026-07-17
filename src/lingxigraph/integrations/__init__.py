"""Optional provider integrations, imported lazily to keep core dependency-free."""

from __future__ import annotations

from typing import Any


def __getattr__(name: str) -> Any:
    if name in {"AsyncCozeClient", "CozeAgentNode", "CozeChatModel", "CozeWorkflowNode"}:
        from . import coze

        return getattr(coze, name)
    if name == "OpenAICompatChatModel":
        from .openai_compat import OpenAICompatChatModel

        return OpenAICompatChatModel
    raise AttributeError(name)


__all__ = [
    "AsyncCozeClient",
    "CozeAgentNode",
    "CozeChatModel",
    "CozeWorkflowNode",
    "OpenAICompatChatModel",
]
