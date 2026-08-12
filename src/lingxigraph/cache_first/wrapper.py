"""The provider-neutral cache-first model request projection."""

from __future__ import annotations

import inspect
import json
import time
import warnings
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import replace
from typing import Any

from ..messages import AIMessage, AIMessageChunk, AnyMessage, ToolCallChunk
from .canonical import canonicalize
from .catalog import (
    ToolCatalogFingerprint,
    build_tool_catalog_fingerprint,
    canonicalize_tools,
    compare_tool_catalogs,
    tool_name,
    tool_schema,
)
from .compaction import CompactionResult, ContextCompactor, estimate_tokens
from .config import CacheFirstConfig
from .diagnostics import CacheRequestSignature, diagnose_cache_usage
from .history import HistoryRepairResult, apply_history_hygiene, repair_model_history
from .prefix import ImmutablePrefix, PrefixDriftDiagnostic, PrefixDriftError
from .usage import (
    InMemoryUsageLedger,
    NormalizedUsage,
    UsageLedger,
    merge_normalized_usage,
    normalize_usage,
)


def _model_name(model: Any) -> str:
    return str(getattr(model, "model", getattr(model, "model_name", type(model).__name__)))


def _provider_name(model: Any, explicit: str | None) -> str:
    if explicit:
        return explicit
    return str(getattr(model, "provider_id", type(model).__module__.split(".")[0]))


def _endpoint_format(model: Any, explicit: str | None) -> str:
    if explicit:
        return explicit
    return str(
        getattr(model, "endpoint_format", getattr(model, "base_url", "chat-completions"))
    )


def _scope_for_runtime(
    model: Any,
    *,
    provider_id: str | None = None,
    model_name: str | None = None,
) -> str:
    provider = provider_id or _provider_name(model, None)
    model_value = model_name or _model_name(model)
    try:
        from ..runtime import get_runtime

        runtime = get_runtime()
    except RuntimeError:
        return f"process|{provider}|{model_value}"
    namespace = "/".join(runtime.namespace) or "default"
    configurable = runtime.config.get("configurable", {})
    configurable_thread = (
        configurable.get("thread_id", "default")
        if isinstance(configurable, Mapping)
        else "default"
    )
    thread = str(runtime.config.get("thread_id", configurable_thread))
    return f"{thread}|{namespace}|{provider}|{model_value}"


def _message_equivalent(left: AnyMessage, right: AnyMessage) -> bool:
    """Compare prompt representation while ignoring runtime-generated ids."""

    if left.type != right.type or getattr(left, "name", None) != getattr(right, "name", None):
        return False
    try:
        if canonicalize(getattr(left, "content", None)) != canonicalize(
            getattr(right, "content", None)
        ):
            return False
    except (TypeError, ValueError):
        if getattr(left, "content", None) != getattr(right, "content", None):
            return False
    left_calls = getattr(left, "tool_calls", ())
    right_calls = getattr(right, "tool_calls", ())
    if len(left_calls) != len(right_calls):
        return False
    return all(
        left_call.name == right_call.name
        and left_call.id == right_call.id
        and canonicalize(dict(left_call.args)) == canonicalize(dict(right_call.args))
        for left_call, right_call in zip(left_calls, right_calls, strict=True)
    )


class CacheFirstChatModel:
    """Wrap any existing ``ChatModel`` with stable-prefix request hygiene.

    The wrapper never writes its repaired, compacted, or redacted projection
    back into graph state.  It is therefore safe to put around an existing
    model in LingxiLearn without changing that application's message protocol.
    """

    def __init__(
        self,
        model: Any,
        *,
        prefix: ImmutablePrefix,
        config: CacheFirstConfig | Mapping[str, Any] | None = None,
        usage_ledger: UsageLedger | None = None,
        pricing: Mapping[str, Any] | None = None,
        provider_id: str | None = None,
        endpoint_format: str | None = None,
        active_skill_ids: Sequence[str] = (),
        summarizer: Any | None = None,
    ) -> None:
        if not isinstance(prefix, ImmutablePrefix):
            raise TypeError("prefix must be an ImmutablePrefix")
        self.model = model
        self.prefix = prefix
        self.config = CacheFirstConfig.from_mapping(config)
        self.pricing = dict(pricing or {})
        self.provider_id = _provider_name(model, provider_id)
        self.endpoint_format = _endpoint_format(model, endpoint_format)
        self.active_skill_ids = tuple(sorted(str(item) for item in active_skill_ids))
        self.usage_ledger = usage_ledger if usage_ledger is not None else InMemoryUsageLedger()
        self.compactor = ContextCompactor(self.config, summarizer=summarizer)
        self._expected_prefix_fingerprint = prefix.fingerprint
        self._expected_prefix_revision = prefix.revision
        self._expected_catalog = prefix.tool_catalog
        self._expected_model_name = _model_name(model)
        self._expected_provider_id = self.provider_id
        self._expected_endpoint_format = self.endpoint_format
        self._expected_active_skill_ids = self.active_skill_ids
        self._previous_signature: CacheRequestSignature | None = None
        self.last_cache_diagnostic: Mapping[str, Any] | None = None

    @property
    def model_name(self) -> str:
        return _model_name(self.model)

    @property
    def tool_catalog_fingerprint(self) -> ToolCatalogFingerprint:
        return self._expected_catalog

    def _signature(self, catalog: ToolCatalogFingerprint) -> CacheRequestSignature:
        return CacheRequestSignature(
            model=self.model_name,
            provider_id=self.provider_id,
            endpoint_format=self.endpoint_format,
            prefix_fingerprint=self.prefix.fingerprint,
            tool_catalog_fingerprint=catalog.fingerprint,
            active_skill_ids=self.active_skill_ids,
        )

    def _drift_payload(self, actual_catalog: ToolCatalogFingerprint) -> dict[str, Any] | None:
        prefix_actual = self.prefix._compute_fingerprint()
        changed: list[str] = []
        if prefix_actual != self._expected_prefix_fingerprint:
            changed.append("prefix")
        if self.prefix.revision != self._expected_prefix_revision:
            changed.append("revision")
        catalog_diff = compare_tool_catalogs(self._expected_catalog, actual_catalog)
        if catalog_diff.kind != "none":
            changed.append("tool_catalog")
        actual_signature = self._signature(actual_catalog)
        expected_signature = CacheRequestSignature(
            model=self._expected_model_name,
            provider_id=self._expected_provider_id,
            endpoint_format=self._expected_endpoint_format,
            prefix_fingerprint=self._expected_prefix_fingerprint,
            tool_catalog_fingerprint=self._expected_catalog.fingerprint,
            active_skill_ids=self._expected_active_skill_ids,
        )
        for field in ("model", "provider_id", "endpoint_format", "active_skill_ids"):
            if getattr(expected_signature, field) != getattr(actual_signature, field):
                changed.append(field)
        if not changed:
            return None
        first = changed[0]
        return {
            "expected_fingerprint": self._expected_prefix_fingerprint,
            "actual_fingerprint": prefix_actual,
            "changed_sections": tuple(dict.fromkeys(changed)),
            "previous_revision": self._expected_prefix_revision,
            "current_revision": self.prefix.revision,
            "tool_catalog_diff": {
                "kind": catalog_diff.kind,
                "added": list(catalog_diff.added),
                "removed": list(catalog_diff.removed),
                "changed": list(catalog_diff.changed),
                "expected_fingerprint": self._expected_catalog.fingerprint,
                "actual_fingerprint": actual_catalog.fingerprint,
            },
            "first_changed_component": first,
            "suggestion": (
                "Keep system prompt, tool schemas, few-shots, constraints, model, endpoint, "
                "and active Skills stable; move runtime data to the request suffix."
            ),
        }

    def _verify(self, catalog: ToolCatalogFingerprint) -> dict[str, Any] | None:
        drift = self._drift_payload(catalog)
        if drift is None or self.config.verify_mode == "off":
            return drift
        diagnostic = PrefixDriftDiagnostic(
            expected_fingerprint=str(drift["expected_fingerprint"]),
            actual_fingerprint=str(drift["actual_fingerprint"]),
            changed_sections=tuple(drift["changed_sections"]),
            previous_revision=int(drift["previous_revision"]),
            current_revision=int(drift["current_revision"]),
            first_changed_component=drift.get("first_changed_component"),
            suggestion=str(drift["suggestion"]),
            tool_catalog_diff=drift.get("tool_catalog_diff"),
        )
        drift["message"] = diagnostic.message
        if self.config.verify_mode == "strict":
            raise PrefixDriftError(diagnostic)
        warnings.warn(diagnostic.message, RuntimeWarning, stacklevel=3)
        return drift

    @staticmethod
    def _without_duplicate_prefix(
        messages: Sequence[AnyMessage],
        prefix_messages: Sequence[AnyMessage],
    ) -> tuple[AnyMessage, ...]:
        prefix = tuple(prefix_messages)
        source = tuple(messages)
        if len(source) >= len(prefix) and all(
            _message_equivalent(source[index], prefix[index]) for index in range(len(prefix))
        ):
            return source[len(prefix) :]
        return source

    async def _prepare(
        self,
        messages: Sequence[AnyMessage],
        tools: Sequence[Any] | None,
    ) -> tuple[
        tuple[AnyMessage, ...],
        tuple[dict[str, Any], ...] | None,
        HistoryRepairResult,
        CompactionResult,
        dict[str, Any] | None,
        ToolCatalogFingerprint,
    ]:
        if tools is None:
            request_tools: tuple[Any, ...] = self.prefix.tools
        else:
            # Keep provider-neutral ToolSpec objects intact for custom models;
            # OpenAI-compatible transports canonicalize the same objects at
            # their boundary.  The ordering is nevertheless derived from the
            # canonical schema, never from registration order.
            source_tools = tuple(tools)
            canonical_schemas = canonicalize_tools(source_tools)
            by_name = {tool_name(tool_schema(item)): item for item in source_tools}
            request_tools = tuple(by_name[tool_name(schema)] for schema in canonical_schemas)
        catalog = build_tool_catalog_fingerprint(request_tools)
        drift = self._verify(catalog)
        prefix_messages = self.prefix.render_messages()
        dynamic = self._without_duplicate_prefix(messages, prefix_messages)
        repaired = repair_model_history(dynamic)
        hygienic = apply_history_hygiene(repaired.messages, self.config)
        compaction = await self.compactor.acompact(
            prefix_messages,
            hygienic,
            stable_extra_tokens=estimate_tokens([tool_schema(item) for item in request_tools])
            if request_tools
            else 0,
        )
        request_messages = (*prefix_messages, *compaction.messages)
        return (
            request_messages,
            request_tools if request_tools else None,
            repaired,
            compaction,
            drift,
            catalog,
        )

    def _record(
        self,
        response_usage: Mapping[str, Any],
        *,
        latency_ms: float,
        ttft_ms: float | None = None,
        repair: HistoryRepairResult | None = None,
        compaction: CompactionResult | None = None,
        drift: Mapping[str, Any] | None = None,
        catalog: ToolCatalogFingerprint | None = None,
    ) -> tuple[dict[str, Any], NormalizedUsage, Mapping[str, Any]]:
        normalized = normalize_usage(
            response_usage,
            provider=self.provider_id,
            model=self.model_name,
            pricing=self.pricing,
        )
        usage = merge_normalized_usage(response_usage, normalized)
        signature = self._signature(catalog or self._expected_catalog)
        diagnostic = diagnose_cache_usage(usage, signature, self._previous_signature)
        self._previous_signature = signature
        cache_meta = diagnostic.as_dict()
        cache_meta.update(
            {
                "hit_tokens": normalized.cache_hit_tokens,
                "miss_tokens": normalized.cache_miss_tokens,
                "write_tokens": normalized.cache_write_tokens,
                "latency_ms": latency_ms,
                "ttft_ms": ttft_ms,
                "provider_metrics_available": normalized.provider_metrics_available,
                "prefix_revision": self.prefix.revision,
                "prefix_fingerprint": self.prefix.fingerprint,
                "tool_catalog_fingerprint": signature.tool_catalog_fingerprint,
                "cache_miss_reasons": list(normalized.diagnostics),
            }
        )
        if repair is not None:
            cache_meta["history_repair"] = repair.as_dict()
        if compaction is not None:
            cache_meta["context_compaction"] = compaction.as_dict()
        if drift is not None:
            cache_meta["prefix_drift"] = dict(drift)
        catalog_diff = compare_tool_catalogs(
            self._expected_catalog, catalog or self._expected_catalog
        )
        cache_meta["tool_catalog_drift"] = {
            "kind": catalog_diff.kind,
            "added": list(catalog_diff.added),
            "removed": list(catalog_diff.removed),
            "changed": list(catalog_diff.changed),
        }
        self.last_cache_diagnostic = cache_meta
        scope = _scope_for_runtime(
            self.model,
            provider_id=self.provider_id,
            model_name=self.model_name,
        )
        self.usage_ledger.record(scope, usage)
        try:
            cache_meta["cumulative"] = dict(self.usage_ledger.snapshot(scope))
        except (AttributeError, TypeError):
            # A minimal custom ledger may only implement record; the per-turn
            # projection remains available in that case.
            pass
        try:
            from ..runtime import get_runtime

            get_runtime().emit("cache_telemetry", dict(cache_meta))
        except RuntimeError:
            pass
        return usage, normalized, cache_meta

    async def agenerate(
        self,
        messages: Sequence[AnyMessage],
        *,
        tools: Sequence[Any] | None = None,
        **kwargs: Any,
    ) -> AIMessage:
        if not self.config.enabled:
            return await self.model.agenerate(messages, tools=tools, **kwargs)
        start = time.perf_counter()
        (
            request_messages,
            request_tools,
            repair,
            compaction,
            drift,
            catalog,
        ) = await self._prepare(messages, tools)
        response = await self.model.agenerate(request_messages, tools=request_tools, **kwargs)
        latency_ms = (time.perf_counter() - start) * 1000
        usage, _, cache_meta = self._record(
            response.usage,
            latency_ms=latency_ms,
            repair=repair,
            compaction=compaction,
            drift=drift,
            catalog=catalog,
        )
        metadata = dict(response.response_metadata)
        metadata.update(
            {
                "cache": cache_meta,
                "cache_request": {
                    "prefix_fingerprint": self.prefix.fingerprint,
                    "prefix_revision": self.prefix.revision,
                    "tool_catalog_fingerprint": catalog.fingerprint,
                    "model": self.model_name,
                    "provider": self.provider_id,
                    "endpoint": self.endpoint_format,
                },
            }
        )
        return replace(response, usage=usage, response_metadata=metadata)

    @staticmethod
    def _response_chunk(response: AIMessage) -> AIMessageChunk:
        return AIMessageChunk(
            response.content,
            id=response.id,
            tool_call_chunks=tuple(
                ToolCallChunk(
                    name=call.name,
                    args=json.dumps(dict(call.args), ensure_ascii=False),
                    id=call.id,
                    index=index,
                )
                for index, call in enumerate(response.tool_calls)
            ),
            usage=response.usage,
            response_metadata=response.response_metadata,
        )

    async def astream(
        self,
        messages: Sequence[AnyMessage],
        *,
        tools: Sequence[Any] | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[AIMessageChunk]:
        if not self.config.enabled:
            stream = getattr(self.model, "astream", None)
            if not callable(stream):
                response = await self.model.agenerate(messages, tools=tools, **kwargs)
                yield self._response_chunk(response)
                return
            async for chunk in stream(messages, tools=tools, **kwargs):
                yield chunk
            return

        start = time.perf_counter()
        first_at: float | None = None
        (
            request_messages,
            request_tools,
            repair,
            compaction,
            drift,
            catalog,
        ) = await self._prepare(messages, tools)
        stream = getattr(self.model, "astream", None)
        if not callable(stream):
            response = await self.model.agenerate(
                request_messages, tools=request_tools, **kwargs
            )
            latency_ms = (time.perf_counter() - start) * 1000
            usage, _, cache_meta = self._record(
                response.usage,
                latency_ms=latency_ms,
                repair=repair,
                compaction=compaction,
                drift=drift,
                catalog=catalog,
            )
            metadata = dict(response.response_metadata)
            metadata.update(
                {
                    "cache": cache_meta,
                    "cache_request": {
                        "prefix_fingerprint": self.prefix.fingerprint,
                        "prefix_revision": self.prefix.revision,
                        "tool_catalog_fingerprint": catalog.fingerprint,
                        "model": self.model_name,
                        "provider": self.provider_id,
                        "endpoint": self.endpoint_format,
                    },
                }
            )
            yield self._response_chunk(
                replace(response, usage=usage, response_metadata=metadata)
            )
            return

        last_usage: Mapping[str, Any] = {}
        last_id: str | None = None
        emitted = False
        async for chunk in stream(request_messages, tools=request_tools, **kwargs):
            if first_at is None:
                first_at = time.perf_counter()
            emitted = True
            last_id = chunk.id
            if chunk.usage:
                last_usage = dict(chunk.usage)
            yield chunk
        latency_ms = (time.perf_counter() - start) * 1000
        ttft_ms = ((first_at - start) * 1000) if first_at is not None else None
        usage, _, cache_meta = self._record(
            last_usage,
            latency_ms=latency_ms,
            ttft_ms=ttft_ms,
            repair=repair,
            compaction=compaction,
            drift=drift,
            catalog=catalog,
        )
        # A terminal metadata-only chunk preserves the existing merge_chunks
        # contract while making final usage/TTFT visible to stream consumers.
        metadata = {
            "cache": cache_meta,
            "cache_request": {
                "prefix_fingerprint": self.prefix.fingerprint,
                "prefix_revision": self.prefix.revision,
                "tool_catalog_fingerprint": catalog.fingerprint,
                "model": self.model_name,
                "provider": self.provider_id,
                "endpoint": self.endpoint_format,
            },
        }
        if emitted or usage:
            yield AIMessageChunk(id=last_id, usage=usage, response_metadata=metadata)

    async def aclose(self) -> None:
        close = getattr(self.model, "aclose", None)
        if callable(close):
            value = close()
            if inspect.isawaitable(value):
                await value


__all__ = ["CacheFirstChatModel"]
