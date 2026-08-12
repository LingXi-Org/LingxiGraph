# Cache-first prompt prefixes

LingxiGraph keeps hot prompt material in an `ImmutablePrefix`: system
instructions, pinned constraints, few-shot examples, and canonical tool
schemas. Per-turn history, goals, workspace state, timestamps, retrievals,
and tool results are request-suffix projections. The projection is repaired,
hygienized, and compacted without modifying checkpointed raw messages.

```python
from lingxigraph import CacheFirstConfig, ImmutablePrefix, create_agent
from lingxigraph.integrations import OpenAICompatChatModel

prefix = ImmutablePrefix.create(
    system_prompt="You are a precise engineering assistant.",
    pinned_constraints=("Use supplied evidence only.",),
    tools=tools,
)
model = OpenAICompatChatModel(
    "deepseek-chat",
    base_url="https://api.deepseek.com/v1",
    immutable_prefix=prefix,
    cache_first=CacheFirstConfig(
        verify_mode="strict",
        context_window_tokens=128_000,
        max_output_tokens=1_024,
    ),
)
graph = create_agent(model, tools, prefix=prefix)
```

The legacy `create_agent(model, tools, system_prompt=...)` form automatically
creates a prefix. `OpenAICompatChatModel` also works unchanged for existing
callers: it canonicalizes tool order, normalizes usage, and infers a leading
system prefix in warning mode. Explicit prefixes use strict drift checks.
Drift diagnostics include expected/actual fingerprints, changed sections,
revision, tool catalog diff, and a remediation suggestion.

## Usage and operations

`AIMessage.usage` retains provider fields and adds normalized
`cache_hit_tokens`, `cache_miss_tokens`, `cache_write_tokens`, cache rates,
token savings, and optional cost estimates. DeepSeek's native hit/miss pair
takes precedence, followed by OpenAI cached-token details and compatible
Anthropic field fixtures. A partial pair produces `None` for the rate rather
than a fabricated 100%.

Pricing is application configuration, not provider-neutral hardcoding. Use
`InMemoryUsageLedger` for process-local counters or implement the
`UsageLedger` protocol for SQLite, Redis, or a database. Checkpoint metadata
stores only counters, rates, fingerprints, and diagnostics; it never stores
complete prompts or tool results. `cache_telemetry` is available as a runtime
custom stream channel.

For large catalogs, Skill metadata is progressively disclosed and
`MCPToolset.progressive_tools()` exposes fixed `mcp_search`, `mcp_describe`,
`mcp_call`, and `mcp_refresh_catalog` schemas. Remote MCP schemas and results
remain dynamic suffix data.

## Live DeepSeek benchmark

```powershell
$env:DEEPSEEK_API_KEY = "..."
python scripts/benchmark_deepseek_cache.py `
  --pricing-file pricing/deepseek-2026-08.json `
  --output artifacts/deepseek-cache.json
```

The benchmark fixes endpoint, model, temperature, and output budget, then
compares an intentionally unstable baseline with cold, warm-up, and ten
steady-state optimized turns. It records cold-inclusive and steady-state hit
rates, hit/miss/input/output/total tokens, savings, cost, TTFT, full latency,
fingerprints, and provider diagnostics. Missing `DEEPSEEK_API_KEY` stops the
run; no live result is synthesized. Provider TTL, routing, and model-version
variance are reported as provider-side variance when they prevent the 90%+
engineering target.

Custom provider-neutral models can opt in without changing the `ChatModel`
protocol:

```python
from lingxigraph import CacheFirstChatModel

model = CacheFirstChatModel(custom_model, prefix=prefix, config=CacheFirstConfig())
```
