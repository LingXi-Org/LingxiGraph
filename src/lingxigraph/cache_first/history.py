"""Pure outgoing-message history repair and bounded hygiene."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any

from ..messages import AIMessage, AnyMessage, ToolCall, ToolMessage
from .canonical import canonicalize
from .config import CacheFirstConfig

_ANSI_RE = re.compile(r"\x1b(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
_DATA_URL_RE = re.compile(r"data:[^;\s]+;base64,[A-Za-z0-9+/=]+", re.IGNORECASE)
_BASE64_RE = re.compile(r"(?<![A-Za-z0-9+/=])[A-Za-z0-9+/]{160,}={0,2}(?![A-Za-z0-9+/=])")
_ERROR_RE = re.compile(r"\b(error|exception|traceback|failed|failure|denied|timeout)\b", re.I)
_PATH_RE = re.compile(r"(?:[A-Za-z]:[\\/]|/)[^\s]+")


@dataclass(frozen=True, slots=True)
class HistoryRepairResult:
    messages: tuple[AnyMessage, ...]
    orphan_tool_results: int = 0
    missing_tool_results: int = 0
    duplicate_tool_results: int = 0
    malformed_tool_calls: int = 0
    reordered_tool_results: int = 0
    dropped_message_count: int = 0
    diagnostics: tuple[str, ...] = ()

    @property
    def changed(self) -> bool:
        return (
            self.dropped_message_count > 0
            or self.reordered_tool_results > 0
            or bool(self.diagnostics)
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "orphan_tool_results": self.orphan_tool_results,
            "missing_tool_results": self.missing_tool_results,
            "duplicate_tool_results": self.duplicate_tool_results,
            "malformed_tool_calls": self.malformed_tool_calls,
            "reordered_tool_results": self.reordered_tool_results,
            "dropped_message_count": self.dropped_message_count,
            "diagnostics": list(self.diagnostics),
        }


def _valid_calls(message: AIMessage) -> bool:
    return bool(message.tool_calls) and all(
        isinstance(call, ToolCall) and bool(call.id) and bool(call.name)
        for call in message.tool_calls
    )


def repair_model_history(messages: Sequence[AnyMessage]) -> HistoryRepairResult:
    """Repair only a request projection; never mutate or rewrite ``messages``.

    A tool-call block is an AI message followed immediately by its tool
    results.  Blocks with a missing result are removed as a unit.  Unknown,
    cross-turn, or duplicate results are removed individually, and remaining
    results are emitted in the original AI call order.
    """

    source = tuple(messages)
    output: list[AnyMessage] = []
    orphan = missing = duplicate = malformed = reordered = dropped = 0
    diagnostics: list[str] = []
    index = 0
    while index < len(source):
        current = source[index]
        if not isinstance(current, AIMessage) or not current.tool_calls:
            if isinstance(current, ToolMessage):
                orphan += 1
                dropped += 1
                diagnostics.append("orphan_tool_result")
            else:
                output.append(current)
            index += 1
            continue

        ai = current
        if not _valid_calls(ai) or len({call.id for call in ai.tool_calls}) != len(
            ai.tool_calls
        ):
            malformed += 1
            dropped += 1
            diagnostics.append("malformed_tool_call_block")
            index += 1
            while index < len(source) and isinstance(source[index], ToolMessage):
                dropped += 1
                index += 1
            continue

        expected = tuple(call.id for call in ai.tool_calls)
        expected_set = set(expected)
        result_by_id: dict[str, ToolMessage] = {}
        scan = index + 1
        while scan < len(source):
            candidate = source[scan]
            if not isinstance(candidate, ToolMessage):
                break
            result = candidate
            if result.tool_call_id not in expected_set:
                orphan += 1
                dropped += 1
                diagnostics.append("cross_turn_or_unknown_tool_result")
            elif result.tool_call_id in result_by_id:
                duplicate += 1
                dropped += 1
                diagnostics.append("duplicate_tool_result")
            else:
                result_by_id[result.tool_call_id] = result
            scan += 1

        if set(result_by_id) != expected_set:
            missing_ids = expected_set - set(result_by_id)
            missing += len(missing_ids)
            dropped += 1  # the AI message itself
            dropped += len(result_by_id)
            diagnostics.append("missing_tool_result_block")
            index = scan
            continue

        output.append(ai)
        ordered = tuple(result_by_id[call_id] for call_id in expected)
        observed = tuple(result.tool_call_id for result in result_by_id.values())
        if observed != expected:
            reordered += 1
            diagnostics.append("tool_results_reordered")
        output.extend(ordered)
        index = scan

    return HistoryRepairResult(
        messages=tuple(output),
        orphan_tool_results=orphan,
        missing_tool_results=missing,
        duplicate_tool_results=duplicate,
        malformed_tool_calls=malformed,
        reordered_tool_results=reordered,
        dropped_message_count=dropped,
        diagnostics=tuple(dict.fromkeys(diagnostics)),
    )


def _json_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    except (TypeError, ValueError):
        return str(value)


def _approx_tokens(value: Any) -> int:
    text = _json_text(value)
    # CJK and other non-ASCII scripts are intentionally estimated more
    # conservatively than ASCII; this is a budget guard, not tokenizer parity.
    non_ascii = sum(1 for char in text if ord(char) > 127)
    ascii_count = len(text) - non_ascii
    return max(1, (ascii_count + 3) // 4 + non_ascii)


def _short_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", errors="replace")).hexdigest()[:16]


def _replace_encoded(text: str) -> str:
    def replacement(match: re.Match[str]) -> str:
        value = match.group(0)
        try:
            decoded = base64.b64decode(value + "===", validate=False)
            size = len(decoded)
        except (ValueError, binascii.Error):
            size = len(value)
        return f"[base64 omitted sha256={_short_hash(value)} bytes={size}]"

    text = _DATA_URL_RE.sub(
        lambda match: (
            f"[data-url omitted sha256={_short_hash(match.group(0))} bytes={len(match.group(0))}]"
        ),
        text,
    )
    return _BASE64_RE.sub(replacement, text)


def _collapse_repeated_lines(lines: Sequence[str]) -> list[str]:
    output: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        end = index + 1
        while end < len(lines) and lines[end] == line:
            end += 1
        count = end - index
        output.append(line)
        if count > 3:
            output.append(f"[repeated line omitted x{count - 1}]")
        elif count > 1:
            output.extend([line] * (count - 1))
        index = end
    return output


def _bounded_text(value: Any, config: CacheFirstConfig) -> str:
    bounded_value = value if isinstance(value, str) else _bounded_value(value, config)
    text = _replace_encoded(_ANSI_RE.sub("", _json_text(bounded_value)))
    lines = _collapse_repeated_lines(text.splitlines() or [text])
    if len(lines) > config.max_tool_result_lines:
        keep_head = max(1, config.max_tool_result_lines // 2)
        keep_tail = max(1, config.max_tool_result_lines - keep_head - 1)
        lines = [
            *lines[:keep_head],
            "[tool result lines omitted]",
            *lines[-keep_tail:],
        ]
    # Error, path, command, and diagnostic lines survive a byte cap when
    # possible.  The head/tail still make the result deterministic.
    text = "\n".join(lines)
    if len(text.encode("utf-8")) > config.max_tool_result_bytes:
        selected = [line for line in lines if _ERROR_RE.search(line) or _PATH_RE.search(line)]
        head = "\n".join(lines[: max(1, config.max_tool_result_lines // 4)])
        tail = "\n".join(lines[-max(1, config.max_tool_result_lines // 4) :])
        diagnostic = "\n".join(selected[:20])
        text = "\n".join(
            part for part in (head, diagnostic, "[tool result bytes omitted]", tail) if part
        )
        encoded = text.encode("utf-8")
        if len(encoded) > config.max_tool_result_bytes:
            text = encoded[: config.max_tool_result_bytes].decode("utf-8", errors="ignore")
    token_limit = config.max_tool_result_tokens
    if _approx_tokens(text) > token_limit:
        chars = max(64, token_limit * 4)
        text = text[:chars]
        text += "\n[tool result token budget omitted]"
    return text


def _bounded_value(value: Any, config: CacheFirstConfig, *, depth: int = 0) -> Any:
    if depth > 6:
        return "[nested value omitted]"
    if isinstance(value, Mapping):
        items = list(value.items())
        if len(items) > config.max_array_items:
            items = items[: config.max_array_items]
            items.append(("__truncated__", True))
        return {str(key): _bounded_value(item, config, depth=depth + 1) for key, item in items}
    if isinstance(value, (list, tuple)):
        values = list(value[: config.max_array_items])
        if len(value) > config.max_array_items:
            values.append("[array items omitted]")
        return [_bounded_value(item, config, depth=depth + 1) for item in values]
    if isinstance(value, str):
        return _replace_encoded(_ANSI_RE.sub("", value))
    return value


def _bounded_args(call: ToolCall, config: CacheFirstConfig, *, cap: bool) -> ToolCall:
    canonical_args = canonicalize(dict(call.args))
    if not cap:
        return replace(call, args=canonical_args)
    args = _bounded_value(canonical_args, config)
    encoded = _json_text(args)
    if (
        len(encoded.encode("utf-8")) <= config.max_tool_argument_string_bytes
        and _approx_tokens(encoded) <= config.max_tool_argument_string_tokens
    ):
        return replace(call, args=args)
    limit = max(
        64,
        min(
            config.max_tool_argument_string_bytes // 2,
            config.max_tool_argument_string_tokens * 4,
        ),
    )
    compact = {"_truncated": True, "preview": encoded[:limit]}
    return replace(call, args=compact)


def apply_history_hygiene(
    messages: Sequence[AnyMessage],
    config: CacheFirstConfig | Mapping[str, Any] | None = None,
) -> tuple[AnyMessage, ...]:
    """Bound only the model-facing projection of a repaired history."""

    settings = CacheFirstConfig.from_mapping(config)
    repaired = repair_model_history(messages).messages
    result_ids: set[str] = set()
    for item in repaired:
        if isinstance(item, ToolMessage):
            result_ids.add(item.tool_call_id)
    output: list[AnyMessage] = []
    tool_result_indexes: list[int] = []
    for message in repaired:
        if isinstance(message, AIMessage) and message.tool_calls:
            calls = tuple(
                _bounded_args(call, settings, cap=call.id in result_ids)
                for call in message.tool_calls
            )
            message = replace(message, tool_calls=calls)
        if isinstance(message, ToolMessage):
            message = replace(message, content=_bounded_text(message.content, settings))
            tool_result_indexes.append(len(output))
        output.append(message)

    cumulative = settings.max_cumulative_tool_result_tokens
    if cumulative:
        used = 0
        recent = set(tool_result_indexes[-settings.keep_recent_tool_results :])
        for index in reversed(tool_result_indexes):
            message = output[index]
            assert isinstance(message, ToolMessage)
            tokens = _approx_tokens(message.content)
            if index in recent or used + tokens <= cumulative:
                used += tokens
                continue
            output[index] = replace(
                message,
                content=f"[older tool result omitted; original_sha256={_short_hash(_json_text(message.content))}]",
            )
    return tuple(output)


__all__ = ["HistoryRepairResult", "apply_history_hygiene", "repair_model_history"]
