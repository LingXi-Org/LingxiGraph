"""OpenTelemetry setup, spans, and sensitive-field redaction."""

from __future__ import annotations

import os
from collections.abc import Mapping
from contextlib import nullcontext
from typing import Any

SENSITIVE_PARTS = ("authorization", "token", "secret", "password", "api_key", "cookie")


def configure_telemetry(
    *,
    service_name: str = "lingxigraph",
    endpoint: str | None = None,
) -> None:
    """Configure OTLP tracing when the optional SDK is installed."""

    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("install lingxigraph[otel] for OpenTelemetry export") from exc
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    exporter = OTLPSpanExporter(
        endpoint=endpoint or os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    )
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)


def start_span(name: str, attributes: Mapping[str, Any] | None = None):
    try:
        from opentelemetry import trace

        tracer = trace.get_tracer("lingxigraph", "1.0.0")
        return tracer.start_as_current_span(name, attributes=dict(attributes or {}))
    except ImportError:  # pragma: no cover
        return nullcontext()


def redact(value: Any) -> Any:
    """Recursively redact common credential fields before logging/export."""

    if isinstance(value, Mapping):
        return {
            str(key): (
                "[REDACTED]"
                if any(part in str(key).lower() for part in SENSITIVE_PARTS)
                else redact(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [redact(item) for item in value]
    return value


__all__ = ["configure_telemetry", "redact", "start_span"]
