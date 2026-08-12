"""Provider-neutral usage normalization and restartable cache accounting.

The transport adapters keep the provider response intact.  This module adds a
small, deliberately boring normalized projection on top of that response so a
DeepSeek response, an OpenAI-compatible response, and a fixture containing
Anthropic-style fields can be measured by the same code.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass, field
from threading import Lock
from typing import Any, Protocol


def _number(value: Any, *, integer: bool = True) -> int | float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        converted = int(value) if integer else float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if isinstance(converted, float) and not math.isfinite(converted):
        return None
    return converted


def _first_number(source: Mapping[str, Any], *keys: str) -> int | float | None:
    for key in keys:
        if key in source:
            value = _number(source.get(key))
            if value is not None:
                return value
    return None


def _non_negative(value: int | float | None) -> int | float | None:
    return value if value is None or value >= 0 else None


def _pricing_for(
    pricing: Mapping[str, Any] | None,
    model: str | None,
) -> Mapping[str, Any] | None:
    if not pricing:
        return None
    if model and isinstance(pricing.get(model), Mapping):
        return pricing[model]
    if isinstance(pricing.get("pricing"), Mapping):
        nested = pricing["pricing"]
        if model and isinstance(nested.get(model), Mapping):
            return nested[model]
        if all(key in nested for key in ("input_miss_per_1m", "output_per_1m")):
            return nested
    if any(
        key in pricing for key in ("input_miss_per_1m", "input_hit_per_1m", "output_per_1m")
    ):
        return pricing
    return None


@dataclass(frozen=True, slots=True)
class NormalizedUsage:
    """A normalized usage projection.

    ``None`` is intentional: a provider that reports only one side of a cache
    pair must not be presented as a fabricated 100% hit or 0% miss.
    """

    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None
    cache_hit_tokens: int | None = None
    cache_miss_tokens: int | None = None
    cache_write_tokens: int | None = None
    cache_hit_rate: float | None = None
    cacheable_token_hit_rate: float | None = None
    total_input_token_hit_rate: float | None = None
    token_savings: int | None = None
    estimated_cost: float | None = None
    estimated_cost_savings: float | None = None
    provider_metrics_available: bool = False
    diagnostics: tuple[str, ...] = ()
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False)

    def as_dict(self, *, include_raw: bool = False) -> dict[str, Any]:
        result: dict[str, Any] = {
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
            "cache_hit_tokens": self.cache_hit_tokens,
            "cache_miss_tokens": self.cache_miss_tokens,
            "cache_write_tokens": self.cache_write_tokens,
            "cache_hit_rate": self.cache_hit_rate,
            "cacheable_token_hit_rate": self.cacheable_token_hit_rate,
            "total_input_token_hit_rate": self.total_input_token_hit_rate,
            "token_savings": self.token_savings,
            "estimated_cost": self.estimated_cost,
            "estimated_cost_savings": self.estimated_cost_savings,
            "provider_metrics_available": self.provider_metrics_available,
            "cache_diagnostics": list(self.diagnostics),
        }
        if include_raw:
            result["raw"] = dict(self.raw)
        return result


def normalize_usage(
    usage: Mapping[str, Any] | None,
    *,
    provider: str | None = None,
    model: str | None = None,
    pricing: Mapping[str, Any] | None = None,
) -> NormalizedUsage:
    """Normalize DeepSeek/OpenAI/Anthropic-compatible usage fields.

    DeepSeek's explicit hit/miss pair wins over all compatibility fallbacks.
    OpenAI's nested ``prompt_tokens_details.cached_tokens`` is interpreted as
    cached input and its miss side is derived only when total prompt tokens are
    known.  Anthropic names are recognized for normalizer fixtures; this core
    does not add an Anthropic transport.
    """

    raw = dict(usage or {})
    prompt_details = raw.get("prompt_tokens_details")
    if not isinstance(prompt_details, Mapping):
        prompt_details = {}
    input_details = raw.get("input_tokens_details")
    if not isinstance(input_details, Mapping):
        input_details = {}

    prompt = _non_negative(_first_number(raw, "prompt_tokens", "input_tokens"))
    completion = _non_negative(_first_number(raw, "completion_tokens", "output_tokens"))
    total = _non_negative(_first_number(raw, "total_tokens", "total_token_count"))
    diagnostics: list[str] = []

    # Priority 1: DeepSeek native fields.  Do not infer the missing half of a
    # pair: a partial native report is useful telemetry, but not a hit rate.
    native_hit = _first_number(raw, "prompt_cache_hit_tokens")
    native_miss = _first_number(raw, "prompt_cache_miss_tokens")
    if native_hit is not None or native_miss is not None:
        hit = _non_negative(native_hit)
        miss = _non_negative(native_miss)
        if hit is None or miss is None:
            diagnostics.append("incomplete_deepseek_cache_pair")
    else:
        # Priority 2: OpenAI-compatible prompt cache details.
        read = _first_number(raw, "cache_read_input_tokens")
        write = _first_number(raw, "cache_creation_input_tokens")
        anthropic_cache_fields = "input_tokens" in raw and (
            read is not None or write is not None
        )
        if anthropic_cache_fields:
            uncached = _non_negative(_first_number(raw, "input_tokens"))
            prompt = (
                int(uncached or 0) + int(read or 0) + int(write or 0)
                if uncached is not None
                else prompt
            )
            hit = _non_negative(read)
            miss = _non_negative(uncached)
        else:
            hit = None
            miss = None
        cached = _first_number(prompt_details, "cached_tokens")
        if not anthropic_cache_fields and cached is None:
            cached = _first_number(raw, "cached_tokens")
        if not anthropic_cache_fields and cached is None:
            cached = _first_number(input_details, "cache_read_input_tokens")
        if not anthropic_cache_fields and read is not None:
            cached = read
        if not anthropic_cache_fields:
            hit = _non_negative(cached)
        if not anthropic_cache_fields and hit is not None and prompt is not None:
            candidate = prompt - hit
            if candidate >= 0:
                miss = candidate
        if hit is None and miss is None and (read is not None or write is not None):
            diagnostics.append("incomplete_compatible_cache_fields")

    cache_write = _first_number(raw, "cache_write_tokens", "cache_creation_input_tokens")
    if cache_write is None:
        cache_write = _first_number(input_details, "cache_creation_input_tokens")
    cache_write = _non_negative(cache_write)

    # A canonical field is also accepted for callers that already normalized a
    # provider response before passing it to the ledger.
    if native_hit is None and native_miss is None:
        if hit is None:
            hit = _non_negative(_first_number(raw, "cache_hit_tokens"))
        if miss is None:
            miss = _non_negative(_first_number(raw, "cache_miss_tokens"))

    if total is None and prompt is not None and completion is not None:
        total = prompt + completion
    if prompt is None and hit is not None and miss is not None:
        prompt = hit + miss
    if total is None and prompt is not None and completion is not None:
        total = prompt + completion
    if prompt is not None and hit is not None and miss is not None and prompt != hit + miss:
        diagnostics.append("cache_pair_does_not_match_prompt_tokens")

    if hit is not None and miss is not None:
        cacheable = hit + miss
        cache_rate = hit / cacheable if cacheable else None
        total_rate = hit / prompt if prompt else None
    else:
        cache_rate = None
        total_rate = None
        if hit is not None or miss is not None:
            diagnostics.append("cache_hit_rate_unavailable_without_hit_and_miss")
    metrics_available = hit is not None or miss is not None or cache_write is not None
    if metrics_available and (hit is None or miss is None):
        diagnostics.append("cache_hit_rate_unavailable_without_hit_and_miss")

    price = _pricing_for(pricing, model)
    estimated_cost: float | None = None
    estimated_savings: float | None = None
    if price is not None:

        def rate(name: str) -> float | None:
            value = _number(price.get(name), integer=False)
            return float(value) if value is not None and value >= 0 else None

        miss_rate = rate("input_miss_per_1m")
        hit_rate = rate("input_hit_per_1m")
        write_rate = rate("cache_write_per_1m")
        output_rate = rate("output_per_1m")
        components: list[float] = []
        if miss is not None and miss_rate is not None:
            components.append(miss * miss_rate / 1_000_000)
        if hit is not None and hit_rate is not None:
            components.append(hit * hit_rate / 1_000_000)
        if cache_write is not None and write_rate is not None:
            components.append(cache_write * write_rate / 1_000_000)
        if completion is not None and output_rate is not None:
            components.append(completion * output_rate / 1_000_000)
        if components:
            estimated_cost = sum(components)
        if (
            prompt is not None
            and miss_rate is not None
            and hit is not None
            and miss is not None
        ):
            baseline = prompt * miss_rate / 1_000_000
            actual_input = 0.0
            if miss_rate is not None:
                actual_input += miss * miss_rate / 1_000_000
            if hit_rate is not None:
                actual_input += hit * hit_rate / 1_000_000
            if write_rate is not None and cache_write is not None:
                actual_input += cache_write * write_rate / 1_000_000
            estimated_savings = max(0.0, baseline - actual_input)
    elif pricing:
        diagnostics.append("pricing_unavailable_for_model")

    return NormalizedUsage(
        prompt_tokens=int(prompt) if prompt is not None else None,
        completion_tokens=int(completion) if completion is not None else None,
        total_tokens=int(total) if total is not None else None,
        cache_hit_tokens=int(hit) if hit is not None else None,
        cache_miss_tokens=int(miss) if miss is not None else None,
        cache_write_tokens=int(cache_write) if cache_write is not None else None,
        cache_hit_rate=cache_rate,
        cacheable_token_hit_rate=cache_rate,
        total_input_token_hit_rate=total_rate,
        token_savings=int(hit) if hit is not None else None,
        estimated_cost=estimated_cost,
        estimated_cost_savings=estimated_savings,
        provider_metrics_available=metrics_available,
        diagnostics=tuple(dict.fromkeys(diagnostics)),
        raw=raw,
    )


class UsageLedger(Protocol):
    """Pluggable durable aggregate for cache and token telemetry."""

    def record(self, scope: str, usage: Mapping[str, Any]) -> None: ...

    def snapshot(self, scope: str) -> Mapping[str, Any]: ...

    def restore(self, scope: str, snapshot: Mapping[str, Any]) -> None: ...


@dataclass(slots=True)
class _UsageTotals:
    requests: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    cache_hit_tokens: int = 0
    cache_miss_tokens: int = 0
    cache_write_tokens: int = 0
    token_savings: int = 0
    estimated_cost: float = 0.0
    estimated_cost_savings: float = 0.0
    provider_metrics_requests: int = 0

    def add(self, usage: Mapping[str, Any] | NormalizedUsage) -> None:
        normalized = usage if isinstance(usage, NormalizedUsage) else normalize_usage(usage)
        values = (
            normalized.as_dict() if isinstance(normalized, NormalizedUsage) else dict(usage)
        )
        self.requests += 1
        self.prompt_tokens += int(values.get("prompt_tokens") or 0)
        self.completion_tokens += int(values.get("completion_tokens") or 0)
        self.total_tokens += int(values.get("total_tokens") or 0)
        self.cache_hit_tokens += int(values.get("cache_hit_tokens") or 0)
        self.cache_miss_tokens += int(values.get("cache_miss_tokens") or 0)
        self.cache_write_tokens += int(values.get("cache_write_tokens") or 0)
        self.token_savings += int(values.get("token_savings") or 0)
        self.estimated_cost += float(values.get("estimated_cost") or 0.0)
        self.estimated_cost_savings += float(values.get("estimated_cost_savings") or 0.0)
        if values.get("provider_metrics_available"):
            self.provider_metrics_requests += 1

    def as_dict(self) -> dict[str, Any]:
        denominator = self.cache_hit_tokens + self.cache_miss_tokens
        return {
            "version": 1,
            "requests": self.requests,
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
            "cache_hit_tokens": self.cache_hit_tokens,
            "cache_miss_tokens": self.cache_miss_tokens,
            "cache_write_tokens": self.cache_write_tokens,
            "cache_hit_rate": (self.cache_hit_tokens / denominator if denominator else None),
            "cacheable_token_hit_rate": (
                self.cache_hit_tokens / denominator if denominator else None
            ),
            "total_input_token_hit_rate": (
                self.cache_hit_tokens / self.prompt_tokens if self.prompt_tokens else None
            ),
            "token_savings": self.token_savings,
            "estimated_cost": self.estimated_cost,
            "estimated_cost_savings": self.estimated_cost_savings,
            "provider_metrics_requests": self.provider_metrics_requests,
        }


class InMemoryUsageLedger:
    """Thread-safe process ledger; applications may replace it with Redis/SQL."""

    def __init__(self) -> None:
        self._values: dict[str, _UsageTotals] = {}
        self._lock = Lock()

    def record(self, scope: str, usage: Mapping[str, Any]) -> None:
        normalized = usage if isinstance(usage, NormalizedUsage) else normalize_usage(usage)
        with self._lock:
            totals = self._values.setdefault(str(scope), _UsageTotals())
            totals.add(normalized)

    def snapshot(self, scope: str) -> Mapping[str, Any]:
        with self._lock:
            return dict(self._values.get(str(scope), _UsageTotals()).as_dict())

    def restore(self, scope: str, snapshot: Mapping[str, Any]) -> None:
        values = _UsageTotals(
            requests=int(snapshot.get("requests", 0) or 0),
            prompt_tokens=int(snapshot.get("prompt_tokens", 0) or 0),
            completion_tokens=int(snapshot.get("completion_tokens", 0) or 0),
            total_tokens=int(snapshot.get("total_tokens", 0) or 0),
            cache_hit_tokens=int(snapshot.get("cache_hit_tokens", 0) or 0),
            cache_miss_tokens=int(snapshot.get("cache_miss_tokens", 0) or 0),
            cache_write_tokens=int(snapshot.get("cache_write_tokens", 0) or 0),
            token_savings=int(snapshot.get("token_savings", 0) or 0),
            estimated_cost=float(snapshot.get("estimated_cost", 0.0) or 0.0),
            estimated_cost_savings=float(snapshot.get("estimated_cost_savings", 0.0) or 0.0),
            provider_metrics_requests=int(snapshot.get("provider_metrics_requests", 0) or 0),
        )
        with self._lock:
            self._values[str(scope)] = values


def merge_normalized_usage(
    raw: Mapping[str, Any] | None,
    normalized: NormalizedUsage,
) -> dict[str, Any]:
    """Retain all provider fields while adding canonical fields."""

    result: MutableMapping[str, Any] = dict(raw or normalized.raw)
    result.update(normalized.as_dict())
    return dict(result)


__all__ = [
    "InMemoryUsageLedger",
    "NormalizedUsage",
    "UsageLedger",
    "merge_normalized_usage",
    "normalize_usage",
]
