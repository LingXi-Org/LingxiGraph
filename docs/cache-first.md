# LingxiGraph cache-first / 稳定 Prompt Prefix

LingxiGraph 的 cache-first 路径把模型请求分成两部分：不可变的热前缀和每轮动态后缀。
`ImmutablePrefix` 包含 system prompt、pinned constraints、few-shot 和 canonical tools；用户
消息、workspace/state、时间、检索内容、tool result 只进入后缀。前缀的完整 SHA-256
fingerprint、revision 和 tool catalog fingerprint 会写入 `AIMessage.response_metadata`，而不把
完整 prompt 或敏感 tool result 写入 telemetry。

```python
from lingxigraph import (
    CacheFirstConfig,
    ImmutablePrefix,
    create_agent,
)
from lingxigraph.integrations import OpenAICompatChatModel

prefix = ImmutablePrefix.create(
    system_prompt="You are a precise repository assistant.",
    pinned_constraints=("Never invent file contents.", "Keep tool calls minimal."),
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
    pricing={
        "deepseek-chat": {
            "currency": "USD",
            "input_miss_per_1m": 0.27,
            "input_hit_per_1m": 0.07,
            "cache_write_per_1m": 0.0,
            "output_per_1m": 1.10,
        }
    },
)
graph = create_agent(model, tools, prefix=prefix)
```

如果不传 `prefix`，`create_agent` 会从现有 `system_prompt`、Skills metadata 和 tools 自动构造
prefix。`OpenAICompatChatModel` 的旧构造方式也继续有效：它会稳定排序 tools、解析 provider
usage，并在没有显式 prefix 时以 warning 模式推断连续 system prompt。显式 prefix 默认严格
校验；若 tool catalog、model、endpoint 或 active Skill 集合漂移，会抛出
`PrefixDriftError`，诊断中包含 expected/actual fingerprint、changed sections 和建议。

## Usage 与累计统计

`AIMessage.usage` 保留 provider 原始字段，并追加统一字段：

- `cache_hit_tokens` / `cache_miss_tokens` / `cache_write_tokens`
- `cache_hit_rate`、`cacheable_token_hit_rate`、`total_input_token_hit_rate`
- `token_savings`、`estimated_cost`、`estimated_cost_savings`

DeepSeek 原生 `prompt_cache_hit_tokens` / `prompt_cache_miss_tokens` 优先；其次识别
OpenAI `prompt_tokens_details.cached_tokens` 和兼容的 Anthropic cache 字段。只收到 hit 或
miss 一侧时，命中率是 `None`，不会伪造 100%。价格始终由应用传入，未配置价格时成本为
`None`。`InMemoryUsageLedger` 可替换成 SQLite/Redis/数据库实现；checkpoint metadata 只保留
版本化计数投影，runtime 重启后从最新 checkpoint hydration。

每轮 `response_metadata["cache"]` 包含 prefix/tool fingerprint、history repair、compaction、
latency/TTFT、miss reasons 和 cumulative snapshot。运行时还会通过 `cache_telemetry` custom
channel 发出相同的非敏感投影。

## History hygiene 与 compaction

模型发送前会复制并修复历史：孤儿 tool result、重复 result、缺失 result 的整组 multi-tool
block 和跨 turn 配对会被从 projection 删除；tool result 按原 tool-call 顺序输出。ANSI、
base64/data URL、重复噪声、超长 JSON、tool args 和累计 tool-result token 会按
`CacheFirstConfig` 上限压缩。checkpoint 中的原始 `messages` 不会被修改。

当 `input_tokens + max_output_tokens` 超过 hard/context cap 时，只压缩动态历史，保留 immutable
prefix、最新目标、关键结论、错误/TODO 和最近完整 tool block。默认摘要是确定性的本地摘要；
可传 summarizer callback，但失败会回退本地摘要，且摘要请求不会混入主请求 prefix。

## Skills 与 MCP progressive discovery

Skills 的固定 `read_skill`/`read_skill_resource` schema 不随技能正文膨胀；当技能数量超过
`progressive_tool_limit`，catalog prompt 只携带有限 metadata 和稳定的 additional names，正文
仍按需读取。对于大型 MCP catalog：

```python
from lingxigraph.protocols import MCPToolset

mcp_tools = MCPToolset("https://mcp.example.test/rpc").progressive_tools()
# 固定 schema：mcp_search、mcp_describe、mcp_call、mcp_refresh_catalog
graph = create_agent(model, mcp_tools)
```

真实 MCP schema 和搜索结果作为动态 tool result；`MCPToolset.catalog_fingerprint` 用于观测
catalog 更新，不要求把所有远程 schema 携带到每一轮。

## DeepSeek live benchmark

需要安装 `lingxigraph[openai]` 并设置 `DEEPSEEK_API_KEY`：

```powershell
$env:DEEPSEEK_API_KEY = "..."
python scripts/benchmark_deepseek_cache.py --output artifacts/deepseek-cache.json
```

benchmark 固定 endpoint/model/temperature/max output，运行 baseline、cold turn、warm-up turn
和 10 个 steady-state turns，保存运行时间、请求数、价格配置、prefix/tool fingerprint、
hit/miss/input/output/total tokens、token savings、cost、TTFT、full latency 和 provider
diagnostic。报告同时给出 cold-inclusive 与 steady-state 命中率。DeepSeek context cache 是
best-effort；TTL、路由、模型版本或 provider-side cache variance 可能使结果低于 90%，报告会
明确标记，而不会把缺失 usage 当成成功命中。

## 接入 LingxiLearn

现有 `LlmBrain`/`OpenAICompatBrain` 只要继续把 `OpenAICompatChatModel` 作为统一模型调用链的
model，即可自动受益，无需复制 DeepSeek 专用字段处理。若 LingxiLearn 使用自定义
`ChatModel`，在边界包一层：

```python
model = CacheFirstChatModel(custom_model, prefix=prefix, config=cache_first_config)
```

它仍实现 `agenerate` 和可选 `astream`，不会要求修改既有模型协议。
