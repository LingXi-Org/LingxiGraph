"""OpenTelemetry setup, spans, and sensitive-field redaction."""

from __future__ import annotations

import json
import logging
import os
import sys
from collections.abc import Mapping
from contextlib import nullcontext
from datetime import UTC, datetime
from typing import Any

from .version import __version__

SENSITIVE_PARTS = ("authorization", "token", "secret", "password", "api_key", "cookie")
_telemetry_configured = False


def configure_telemetry(
    *,
    service_name: str = "lingxigraph",
    endpoint: str | None = None,
) -> None:
    """Configure OTLP tracing when the optional SDK is installed."""

    global _telemetry_configured
    if _telemetry_configured:
        return
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
    _telemetry_configured = True


class JsonFormatter(logging.Formatter):
    """Small structured formatter with recursive credential redaction."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
        }
        for name in (
            "request_id",
            "tenant_id",
            "run_id",
            "task_id",
            "graph_id",
            "graph_version",
            "status",
            "duration_ms",
            "error_type",
        ):
            if hasattr(record, name):
                payload[name] = getattr(record, name)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(redact(payload), ensure_ascii=False, separators=(",", ":"))


def configure_logging(*, level: str | None = None, json_output: bool | None = None) -> None:
    """Configure process logging once for CLI server and worker processes."""

    selected = (level or os.getenv("LINGXIGRAPH_LOG_LEVEL") or "INFO").upper()
    use_json = (
        os.getenv("LINGXIGRAPH_LOG_FORMAT", "json").lower() == "json"
        if json_output is None
        else json_output
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter() if use_json else logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s"
    ))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(selected)


def start_span(name: str, attributes: Mapping[str, Any] | None = None):
    try:
        from opentelemetry import trace

        tracer = trace.get_tracer("lingxigraph", __version__)
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


__all__ = [
    "JsonFormatter",
    "configure_logging",
    "configure_telemetry",
    "redact",
    "start_span",
]
