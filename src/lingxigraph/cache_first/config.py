"""Configuration for LingxiGraph's provider-neutral cache-first request path."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Literal, Mapping

VerifyMode = Literal["strict", "warn", "off"]
SummaryMode = Literal["heuristic", "callback"]


@dataclass(frozen=True, slots=True)
class CacheFirstConfig:
    """Bounded, provider-neutral controls for cache-first model requests.

    The defaults are deliberately conservative.  Request projections are
    compacted/hygienized before transport, while the state stored by the graph
    remains untouched.
    """

    enabled: bool = True
    verify_mode: VerifyMode = "strict"
    max_tool_result_lines: int = 320
    max_tool_result_bytes: int = 32 * 1024
    max_tool_result_tokens: int = 8_000
    max_tool_argument_string_bytes: int = 8 * 1024
    max_tool_argument_string_tokens: int = 2_000
    max_array_items: int = 80
    max_cumulative_tool_result_tokens: int = 0
    keep_recent_tool_results: int = 4
    read_only_concurrency: int = 4
    read_only_batch_size: int = 4
    context_window_tokens: int | None = None
    soft_threshold_tokens: int | None = None
    hard_threshold_tokens: int | None = None
    max_output_tokens: int = 4_096
    keep_recent_messages: int = 8
    summary_max_tokens: int = 1_200
    summary_mode: SummaryMode = "heuristic"
    progressive_tool_limit: int = 64

    def __post_init__(self) -> None:
        if self.verify_mode not in {"strict", "warn", "off"}:
            raise ValueError("verify_mode must be 'strict', 'warn', or 'off'")
        if self.summary_mode not in {"heuristic", "callback"}:
            raise ValueError("summary_mode must be 'heuristic' or 'callback'")
        positive = (
            "max_tool_result_lines",
            "max_tool_result_bytes",
            "max_tool_result_tokens",
            "max_tool_argument_string_bytes",
            "max_tool_argument_string_tokens",
            "max_array_items",
            "read_only_concurrency",
            "read_only_batch_size",
            "max_output_tokens",
            "keep_recent_messages",
            "summary_max_tokens",
            "progressive_tool_limit",
        )
        for name in positive:
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.max_cumulative_tool_result_tokens < 0:
            raise ValueError("max_cumulative_tool_result_tokens must be non-negative")
        if self.keep_recent_tool_results < 0:
            raise ValueError("keep_recent_tool_results must be non-negative")
        for name in ("context_window_tokens", "soft_threshold_tokens", "hard_threshold_tokens"):
            value = getattr(self, name)
            if value is not None and value <= 0:
                raise ValueError(f"{name} must be positive when configured")
        if (
            self.soft_threshold_tokens is not None
            and self.hard_threshold_tokens is not None
            and self.soft_threshold_tokens > self.hard_threshold_tokens
        ):
            raise ValueError("soft_threshold_tokens cannot exceed hard_threshold_tokens")

    @classmethod
    def from_mapping(
        cls, value: Mapping[str, Any] | "CacheFirstConfig" | bool | None
    ) -> "CacheFirstConfig":
        if value is None:
            return cls()
        if isinstance(value, cls):
            return value
        if value is True:
            return cls()
        if value is False:
            return cls(enabled=False)
        if not isinstance(value, Mapping):
            raise TypeError("cache_first configuration must be a mapping")
        fields = {field for field in cls.__dataclass_fields__}
        selected = {key: item for key, item in value.items() if key in fields}
        return cls(**selected)

    def with_overrides(self, **changes: Any) -> "CacheFirstConfig":
        return replace(self, **changes)


ContextCompactionConfig = CacheFirstConfig

__all__ = ["CacheFirstConfig", "ContextCompactionConfig", "SummaryMode", "VerifyMode"]
