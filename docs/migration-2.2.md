# Migrating to LingxiGraph 2.2.0 / 迁移到 LingxiGraph 2.2.0

## English

LingxiGraph 2.2.0 is backward compatible with existing graph and agent code. Existing
`create_agent(model, tools)` and `create_agent(model, tools, system_prompt=...)` calls continue to
work. The default agent path now builds a stable `ImmutablePrefix` and applies the provider-neutral
cache-first request projection.

To configure it explicitly:

```python
from lingxigraph import CacheFirstConfig, ImmutablePrefix, create_agent

prefix = ImmutablePrefix.create(
    system_prompt="You are a precise support agent.",
    pinned_constraints=("Use supplied evidence only.",),
    tools=tools,
)
agent = create_agent(
    model,
    tools,
    prefix=prefix,
    cache_first=CacheFirstConfig(
        verify_mode="strict",
        context_window_tokens=128_000,
        max_output_tokens=1_024,
    ),
)
```

Keep system instructions, fixed constraints, few-shot examples, and tool schemas in the prefix.
Keep timestamps, retrievals, workspace data, user messages, and tool results in the request suffix.
Strict verification raises `PrefixDriftError` when the prefix, tool catalog, model, provider, endpoint,
or active Skill set changes. Use `verify_mode="warn"` for a gradual rollout or `verify_mode="off"`
when drift verification is not wanted.

To disable the complete cache-first path for a legacy integration, pass `cache_first=False` to
`create_agent` or `OpenAICompatChatModel`. This does not change the `ChatModel` protocol or the
checkpoint format.

`AIMessage.usage` retains raw provider fields and adds normalized cache hit/miss, rate, savings, and
optional cost fields. A provider that reports only one side of a cache pair produces an unknown rate,
not a fabricated hit rate. Prices are application configuration. Use `InMemoryUsageLedger` for local
development and implement `UsageLedger` for durable counters in production.

The request projection may repair history and compact dynamic context, but it never writes the repaired
projection back to graph state. Review `Wiki/en/guides/cache-first.mdx` for the full configuration,
progressive Skills/MCP discovery, telemetry, and benchmark guidance.

## 中文

LingxiGraph 2.2.0 与现有图和 Agent 代码向后兼容。原有的 `create_agent(model, tools)` 与
`create_agent(model, tools, system_prompt=...)` 调用继续有效。默认 Agent 路径现在会构造稳定的
`ImmutablePrefix`，并应用 provider-neutral 的 cache-first 请求投影。

显式配置示例：

```python
from lingxigraph import CacheFirstConfig, ImmutablePrefix, create_agent

prefix = ImmutablePrefix.create(
    system_prompt="You are a precise support agent.",
    pinned_constraints=("Use supplied evidence only.",),
    tools=tools,
)
agent = create_agent(
    model,
    tools,
    prefix=prefix,
    cache_first=CacheFirstConfig(
        verify_mode="strict",
        context_window_tokens=128_000,
        max_output_tokens=1_024,
    ),
)
```

请把 system instructions、固定约束、few-shot 示例和 tool schema 放入 prefix；时间戳、检索结果、
workspace 数据、用户消息和 tool result 放入请求后缀。严格校验会在 prefix、tool catalog、model、
provider、endpoint 或 active Skill 集合改变时抛出 `PrefixDriftError`。渐进迁移可使用
`verify_mode="warn"`；不需要漂移校验时使用 `verify_mode="off"`。

对于旧集成，可向 `create_agent` 或 `OpenAICompatChatModel` 传入 `cache_first=False`，关闭完整
cache-first 路径。该选项不会改变 `ChatModel` 协议或 checkpoint 格式。

`AIMessage.usage` 保留 provider 原始字段，并追加统一的 cache hit/miss、命中率、节省 token 和
可选成本字段。provider 只报告 cache pair 的一侧时，命中率保持未知，不会伪造命中。价格由应用
配置；开发环境可使用 `InMemoryUsageLedger`，生产环境可实现 `UsageLedger` 接入持久化计数。

请求投影可能修复历史并压缩动态上下文，但不会把修复后的投影写回 graph state。完整配置、Skills/
MCP progressive discovery、telemetry 与 benchmark 说明见
[`Wiki/zh/guides/cache-first.mdx`](../Wiki/zh/guides/cache-first.mdx)。
