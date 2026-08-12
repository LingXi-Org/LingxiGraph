"""Cache request signatures and actionable drift/miss diagnostics."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class CacheRequestSignature:
    model: str
    provider_id: str
    endpoint_format: str
    prefix_fingerprint: str
    tool_catalog_fingerprint: str
    active_skill_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CacheDiagnostic:
    cacheable_token_hit_rate: float | None
    total_input_token_hit_rate: float | None
    reasons: tuple[str, ...] = ()
    suggestions: tuple[str, ...] = ()
    prefix_fingerprint: str | None = None
    tool_catalog_fingerprint: str | None = None

    @property
    def cache_hit_rate(self) -> float | None:
        return self.cacheable_token_hit_rate

    def as_dict(self) -> dict[str, Any]:
        return {
            "cacheable_token_hit_rate": self.cacheable_token_hit_rate,
            "cache_hit_rate": self.cache_hit_rate,
            "total_input_token_hit_rate": self.total_input_token_hit_rate,
            "reasons": list(self.reasons),
            "suggestions": list(self.suggestions),
            "prefix_fingerprint": self.prefix_fingerprint,
            "tool_catalog_fingerprint": self.tool_catalog_fingerprint,
        }


def diagnose_cache_usage(
    usage: dict[str, Any],
    current: CacheRequestSignature,
    previous: CacheRequestSignature | None = None,
) -> CacheDiagnostic:
    raw_hit = usage.get("cache_hit_tokens")
    raw_miss = usage.get("cache_miss_tokens")
    raw_prompt = usage.get("prompt_tokens")
    hit = (
        int(raw_hit)
        if isinstance(raw_hit, (int, float)) and not isinstance(raw_hit, bool)
        else None
    )
    miss = (
        int(raw_miss)
        if isinstance(raw_miss, (int, float)) and not isinstance(raw_miss, bool)
        else None
    )
    prompt = (
        int(raw_prompt)
        if isinstance(raw_prompt, (int, float)) and not isinstance(raw_prompt, bool)
        else None
    )
    has_metrics = hit is not None and miss is not None
    denominator = (hit or 0) + (miss or 0)
    reasons: list[str] = []
    suggestions: list[str] = []
    if previous is None:
        reasons.append("cold_request")
    else:
        if previous.model != current.model:
            reasons.append("model_changed")
        if previous.provider_id != current.provider_id:
            reasons.append("provider_changed")
        if previous.endpoint_format != current.endpoint_format:
            reasons.append("endpoint_changed")
        if previous.prefix_fingerprint != current.prefix_fingerprint:
            reasons.append("stable_prefix_changed")
            suggestions.append(
                "Move timestamps, workspace snippets, and retrieval data after the prefix."
            )
        if previous.tool_catalog_fingerprint != current.tool_catalog_fingerprint:
            reasons.append("tool_catalog_changed")
            suggestions.append(
                "Keep MCP and Skill tool catalogs stable within a cache-sensitive thread."
            )
        if tuple(sorted(previous.active_skill_ids)) != tuple(sorted(current.active_skill_ids)):
            reasons.append("skills_changed")
            suggestions.append("Reuse a stable active Skill set for hot turns.")
    if not has_metrics:
        reasons.append("provider_metrics_unavailable")
        suggestions.append("The provider did not report a complete cache hit/miss pair.")
    elif miss and not hit:
        reasons.extend(("provider_cache_miss", "cache_ttl_unknown"))
        suggestions.append(
            "The provider cache may be cold, expired, or routed to another cache shard."
        )
    if previous is not None and any(
        item in reasons for item in ("model_changed", "provider_changed", "endpoint_changed")
    ):
        suggestions.append(
            "Keep model, provider, and endpoint unchanged while warming a thread."
        )
    cacheable_rate = (
        hit / denominator if hit is not None and miss is not None and denominator else None
    )
    total_rate = hit / prompt if hit is not None and prompt else None
    return CacheDiagnostic(
        cacheable_token_hit_rate=cacheable_rate,
        total_input_token_hit_rate=total_rate,
        reasons=tuple(dict.fromkeys(reasons)),
        suggestions=tuple(dict.fromkeys(suggestions)),
        prefix_fingerprint=current.prefix_fingerprint,
        tool_catalog_fingerprint=current.tool_catalog_fingerprint,
    )


__all__ = ["CacheDiagnostic", "CacheRequestSignature", "diagnose_cache_usage"]
