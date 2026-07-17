"""Public configuration and internal projection values."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass(frozen=True, slots=True)
class ObservabilityOptions:
    """Control which potentially sensitive graph payloads reach the UI."""

    show_state_updates: bool = False
    show_custom_payloads: bool = False
    show_tool_io: bool = False
    max_payload_chars: int = 4_000
    default_open: bool = False

    def __post_init__(self) -> None:
        if self.max_payload_chars < 64:
            raise ValueError("max_payload_chars must be at least 64")


@dataclass(frozen=True, slots=True)
class SessionInfo:
    """Stable Chainlit session fields exposed to context factories."""

    thread_id: str
    session_id: str
    user_identifier: str | None = None
    chat_profile: str | None = None


ProjectionKind = Literal[
    "run_start",
    "run_end",
    "node_start",
    "node_update",
    "assistant_token",
    "assistant_message",
    "assistant_end",
    "tool_start",
    "tool_end",
    "custom",
    "interrupt",
]


@dataclass(frozen=True, slots=True)
class Projection:
    kind: ProjectionKind
    key: str
    name: str = ""
    content: Any = None
    metadata: dict[str, Any] = field(default_factory=dict)
    status: str | None = None
    is_error: bool = False

