"""Cache-first prompt-prefix and model-request utilities."""

from .canonical import (
    canonical_json,
    canonicalize,
    canonicalize_schema,
    fingerprint,
    schema_json,
)
from .catalog import (
    ToolCatalogDrift,
    ToolCatalogFingerprint,
    build_tool_catalog_fingerprint,
    canonicalize_tools,
    compare_tool_catalogs,
    tool_name,
    tool_schema,
)
from .compaction import (
    CompactionResult,
    ContextCompactor,
    estimate_message_tokens,
    estimate_tokens,
)
from .config import CacheFirstConfig, ContextCompactionConfig
from .diagnostics import CacheDiagnostic, CacheRequestSignature, diagnose_cache_usage
from .history import HistoryRepairResult, apply_history_hygiene, repair_model_history
from .prefix import ImmutablePrefix, PrefixDriftDiagnostic, PrefixDriftError
from .usage import (
    InMemoryUsageLedger,
    NormalizedUsage,
    UsageLedger,
    merge_normalized_usage,
    normalize_usage,
)
from .wrapper import CacheFirstChatModel

__all__ = [
    "CacheDiagnostic",
    "CacheFirstChatModel",
    "CacheFirstConfig",
    "CacheRequestSignature",
    "CompactionResult",
    "ContextCompactor",
    "ContextCompactionConfig",
    "HistoryRepairResult",
    "ImmutablePrefix",
    "InMemoryUsageLedger",
    "NormalizedUsage",
    "PrefixDriftDiagnostic",
    "PrefixDriftError",
    "ToolCatalogDrift",
    "ToolCatalogFingerprint",
    "UsageLedger",
    "apply_history_hygiene",
    "build_tool_catalog_fingerprint",
    "canonical_json",
    "canonicalize",
    "canonicalize_schema",
    "canonicalize_tools",
    "compare_tool_catalogs",
    "diagnose_cache_usage",
    "estimate_message_tokens",
    "estimate_tokens",
    "fingerprint",
    "merge_normalized_usage",
    "normalize_usage",
    "repair_model_history",
    "schema_json",
    "tool_name",
    "tool_schema",
]
