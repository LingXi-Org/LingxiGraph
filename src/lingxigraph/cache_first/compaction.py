"""Deterministic context compaction that leaves the immutable prefix untouched."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any

from ..messages import AIMessage, AnyMessage, HumanMessage, ToolMessage
from .config import CacheFirstConfig
from .history import _ERROR_RE, _PATH_RE, _approx_tokens, _json_text


def estimate_message_tokens(messages: Sequence[AnyMessage]) -> int:
    return sum(_approx_tokens(_json_text(message.content)) + 4 for message in messages)


def estimate_tokens(value: Any) -> int:
    """Conservative dependency-free token estimate for fixed request material."""

    return _approx_tokens(_json_text(value))


@dataclass(frozen=True, slots=True)
class CompactionResult:
    messages: tuple[AnyMessage, ...]
    compacted: bool
    estimated_input_tokens: int
    estimated_total_tokens: int
    summary: str | None = None
    diagnostics: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "compacted": self.compacted,
            "estimated_input_tokens": self.estimated_input_tokens,
            "estimated_total_tokens": self.estimated_total_tokens,
            "summary_present": self.summary is not None,
            "diagnostics": list(self.diagnostics),
        }


Summarizer = Callable[[Sequence[AnyMessage]], str | Awaitable[str]]


def _is_block_start(message: AnyMessage) -> bool:
    return isinstance(message, AIMessage) and bool(message.tool_calls)


def _history_blocks(messages: Sequence[AnyMessage]) -> list[tuple[AnyMessage, ...]]:
    blocks: list[tuple[AnyMessage, ...]] = []
    index = 0
    while index < len(messages):
        message = messages[index]
        if _is_block_start(message):
            end = index + 1
            while end < len(messages) and isinstance(messages[end], ToolMessage):
                end += 1
            blocks.append(tuple(messages[index:end]))
            index = end
        else:
            blocks.append((message,))
            index += 1
    return blocks


def _summary_for(messages: Sequence[AnyMessage], limit: int) -> str:
    goals = [str(item.content) for item in messages if isinstance(item, HumanMessage)]
    conclusions = [
        str(item.content)
        for item in messages
        if isinstance(item, AIMessage) and not item.tool_calls
    ]
    tools = [
        f"{item.name or 'tool'}: {str(item.content)}"
        for item in messages
        if isinstance(item, ToolMessage)
    ]
    lines = ["[deterministic context summary]"]
    if goals:
        lines.append("Goals/latest requests: " + " | ".join(goals[-3:]))
    if conclusions:
        lines.append("Key conclusions: " + " | ".join(conclusions[-3:]))
    if tools:
        lines.append("Completed tool observations: " + " | ".join(tools[-3:]))
    signals = [
        line.strip()
        for message in messages
        for line in str(message.content).splitlines()
        if line.strip()
        and (
            _ERROR_RE.search(line)
            or _PATH_RE.search(line)
            or "TODO" in line.upper()
            or "BLOCKED" in line.upper()
            or "COMMAND" in line.upper()
        )
    ]
    if signals:
        lines.append("Errors/TODOs/paths/commands: " + " | ".join(signals[-8:]))
    text = "\n".join(lines)
    if _approx_tokens(text) > limit:
        text = text[: max(128, limit * 4)] + "\n[summary truncated]"
    return text


def _truncate_text(value: Any, token_budget: int) -> str:
    text = str(value)
    if token_budget <= 0:
        return "[content omitted]"
    if _approx_tokens(text) <= token_budget:
        return text
    width = max(24, token_budget * 4)
    head = max(8, width // 2)
    tail = max(8, width - head - 32)
    return text[:head] + "\n[content compacted]\n" + text[-tail:]


class ContextCompactor:
    """Compact only dynamic history, retaining prefix messages byte-for-byte."""

    def __init__(
        self,
        config: CacheFirstConfig | Mapping[str, Any] | None = None,
        *,
        summarizer: Summarizer | None = None,
    ) -> None:
        self.config = CacheFirstConfig.from_mapping(config)
        self.summarizer = summarizer

    def _limit(self) -> int | None:
        return self.config.hard_threshold_tokens or self.config.context_window_tokens

    def compact(
        self,
        prefix_messages: Sequence[AnyMessage],
        dynamic_messages: Sequence[AnyMessage],
        *,
        stable_extra_tokens: int = 0,
    ) -> CompactionResult:
        """Synchronous deterministic compaction path.

        Async callbacks are intentionally rejected here; the request wrapper
        uses :meth:`acompact` when a callback is configured.
        """

        if not self._needs_compaction(prefix_messages, dynamic_messages, stable_extra_tokens):
            input_tokens = (
                estimate_message_tokens(prefix_messages)
                + estimate_message_tokens(dynamic_messages)
                + stable_extra_tokens
            )
            return CompactionResult(
                tuple(dynamic_messages),
                False,
                input_tokens,
                input_tokens + self.config.max_output_tokens,
            )
        if self.summarizer is not None:
            value = self.summarizer(tuple(dynamic_messages))
            if inspect.isawaitable(value):
                raise TypeError("async summarizer requires ContextCompactor.acompact()")
            summary = str(value)
        else:
            summary = _summary_for(dynamic_messages, self.config.summary_max_tokens)
        return self._compact_with_summary(
            prefix_messages, dynamic_messages, summary, stable_extra_tokens=stable_extra_tokens
        )

    async def acompact(
        self,
        prefix_messages: Sequence[AnyMessage],
        dynamic_messages: Sequence[AnyMessage],
        *,
        stable_extra_tokens: int = 0,
    ) -> CompactionResult:
        try:
            if not self._needs_compaction(
                prefix_messages, dynamic_messages, stable_extra_tokens
            ):
                input_tokens = (
                    estimate_message_tokens(prefix_messages)
                    + estimate_message_tokens(dynamic_messages)
                    + stable_extra_tokens
                )
                return CompactionResult(
                    tuple(dynamic_messages),
                    False,
                    input_tokens,
                    input_tokens + self.config.max_output_tokens,
                )
            if self.summarizer is not None:
                value = self.summarizer(tuple(dynamic_messages))
                if inspect.isawaitable(value):
                    value = await value
                summary = str(value)
            else:
                summary = _summary_for(dynamic_messages, self.config.summary_max_tokens)
            return self._compact_with_summary(
                prefix_messages,
                dynamic_messages,
                summary,
                stable_extra_tokens=stable_extra_tokens,
            )
        except Exception:
            # A summarizer is an optimization.  A failed callback must not
            # make the primary model request fail.
            summary = _summary_for(dynamic_messages, self.config.summary_max_tokens)
            result = self._compact_with_summary(
                prefix_messages,
                dynamic_messages,
                summary,
                stable_extra_tokens=stable_extra_tokens,
            )
            return CompactionResult(
                result.messages,
                result.compacted,
                result.estimated_input_tokens,
                result.estimated_total_tokens,
                result.summary,
                (*result.diagnostics, "summarizer_failed_local_fallback"),
            )

    def _compact_with_summary(
        self,
        prefix_messages: Sequence[AnyMessage],
        dynamic_messages: Sequence[AnyMessage],
        summary: str,
        *,
        stable_extra_tokens: int = 0,
    ) -> CompactionResult:
        prefix = tuple(prefix_messages)
        dynamic = tuple(dynamic_messages)
        prefix_tokens = estimate_message_tokens(prefix)
        input_tokens = prefix_tokens + estimate_message_tokens(dynamic) + stable_extra_tokens
        total_tokens = input_tokens + self.config.max_output_tokens
        limit = self._limit()
        soft = self.config.soft_threshold_tokens
        should_compact = limit is not None and (
            total_tokens > limit or (soft is not None and input_tokens > soft)
        )
        if not should_compact:
            return CompactionResult(dynamic, False, input_tokens, total_tokens)

        if limit is None:
            return CompactionResult(
                dynamic,
                False,
                input_tokens,
                total_tokens,
                diagnostics=("no_context_window_configured",),
            )
        dynamic_budget = max(
            1, limit - self.config.max_output_tokens - prefix_tokens - stable_extra_tokens
        )
        blocks = _history_blocks(dynamic)
        if (
            blocks
            and _is_block_start(blocks[-1][0])
            and estimate_message_tokens(blocks[-1]) > dynamic_budget
        ):
            # Splitting a tool call/result block changes protocol semantics. The
            # hygiene pass remains useful, but this compaction is skipped.
            return CompactionResult(
                dynamic,
                False,
                input_tokens,
                total_tokens,
                diagnostics=("latest_tool_block_exceeds_budget_compaction_skipped",),
            )

        keep: list[tuple[AnyMessage, ...]] = []
        used = 0
        for block in reversed(blocks):
            block_tokens = estimate_message_tokens(block)
            if used + block_tokens > dynamic_budget:
                break
            keep.append(block)
            used += block_tokens
            if len(keep) >= self.config.keep_recent_messages:
                break
        keep.reverse()
        latest_human = next(
            (message for message in reversed(dynamic) if isinstance(message, HumanMessage)),
            None,
        )
        latest_id = id(latest_human) if latest_human is not None else None
        if latest_id is not None and not any(
            latest_id in {id(message) for message in block} for block in keep
        ):
            assert latest_human is not None
            keep.insert(0, (latest_human,))

        def flatten(
            blocks_to_flatten: Sequence[tuple[AnyMessage, ...]],
        ) -> tuple[AnyMessage, ...]:
            return tuple(message for block in blocks_to_flatten for message in block)

        summary_message = HumanMessage(summary)

        def candidate_for(
            blocks_to_use: Sequence[tuple[AnyMessage, ...]],
            summary_message_to_use: HumanMessage,
        ) -> tuple[AnyMessage, ...]:
            return (summary_message_to_use, *flatten(blocks_to_use))

        def fits(candidate_to_check: Sequence[AnyMessage]) -> bool:
            return (
                estimate_message_tokens(prefix)
                + estimate_message_tokens(candidate_to_check)
                + stable_extra_tokens
                + self.config.max_output_tokens
                <= limit
            )

        candidate = candidate_for(keep, summary_message)
        # Remove whole history blocks, never individual tool-call/result
        # messages. The block containing the latest user request is retained.
        while not fits(candidate) and keep:
            removable = next(
                (
                    index
                    for index, block in enumerate(keep)
                    if latest_id is None or latest_id not in {id(message) for message in block}
                ),
                None,
            )
            if removable is None:
                break
            keep.pop(removable)
            candidate = candidate_for(keep, summary_message)

        if not fits(candidate):
            available = max(
                1,
                limit - self.config.max_output_tokens - prefix_tokens - stable_extra_tokens,
            )
            summary_budget = max(1, min(self.config.summary_max_tokens, available // 3))
            summary_message = replace(
                summary_message,
                content=_truncate_text(summary_message.content, summary_budget),
            )
            candidate = candidate_for(keep, summary_message)
            if not fits(candidate) and latest_human is not None:
                latest_budget = max(1, available - summary_budget - 8)
                keep = [
                    tuple(
                        replace(message, content=_truncate_text(message.content, latest_budget))
                        if id(message) == latest_id
                        else message
                        for message in block
                    )
                    for block in keep
                ]
                candidate = candidate_for(keep, summary_message)
            if not fits(candidate):
                # A complete tool block can itself consume the entire dynamic
                # budget. Preserve it and omit the summary rather than emit a
                # request that violates the hard input+output cap.
                short_summary = HumanMessage("[older context compacted]")
                candidate = candidate_for(keep, short_summary)
                if not fits(candidate):
                    candidate = flatten(keep)
                if not fits(candidate):
                    return CompactionResult(
                        dynamic,
                        False,
                        input_tokens,
                        total_tokens,
                        diagnostics=("compaction_budget_cannot_fit_complete_block",),
                    )
        kept_ids = {id(message) for block in keep for message in block}
        older = [message for message in dynamic if id(message) not in kept_ids]
        summary_value = (
            str(candidate[0].content)
            if candidate and isinstance(candidate[0], HumanMessage)
            else None
        )
        final_input = prefix_tokens + estimate_message_tokens(candidate) + stable_extra_tokens
        return CompactionResult(
            candidate,
            True,
            final_input,
            final_input + self.config.max_output_tokens,
            summary=summary_value,
            diagnostics=("history_compacted", f"messages_summarized={len(older)}"),
        )

    def _needs_compaction(
        self,
        prefix_messages: Sequence[AnyMessage],
        dynamic_messages: Sequence[AnyMessage],
        stable_extra_tokens: int,
    ) -> bool:
        limit = self._limit()
        if limit is None:
            return False
        input_tokens = (
            estimate_message_tokens(prefix_messages)
            + estimate_message_tokens(dynamic_messages)
            + stable_extra_tokens
        )
        return input_tokens + self.config.max_output_tokens > limit or (
            self.config.soft_threshold_tokens is not None
            and input_tokens > self.config.soft_threshold_tokens
        )


__all__ = ["CompactionResult", "ContextCompactor", "estimate_message_tokens", "estimate_tokens"]
