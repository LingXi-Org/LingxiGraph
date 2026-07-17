# LingxiGraph 2.0

LingxiGraph 是模型供应商中立的企业级多智能体图运行平台。它同时提供可嵌入 Python
应用的 SDK，以及由 Agent Server、PostgreSQL 队列、分布式 Worker、Redis 加速层和
REST/SSE 协议组成的生产运行面。

核心语义采用 Pregel `plan → execute → commit` 超步模型：并行节点可并发完成，但状态
更新始终按编译计划顺序归并。成功任务会先写入 pending writes；兄弟任务失败、进程退出
或动态中断后恢复时，不会再次执行已经持久化的成功任务。

## 能力概览

- `StateGraph`、不可变 `CompiledGraph`、`Command`、`Send`、动态 interrupt/resume。
- 独立 state/input/output/context schema，支持 TypedDict、dataclass 与 Pydantic。
- 同步/异步 invoke 与 stream，`values`、`updates`、`events`、`custom/messages` 流模式。
- 节点重试、超时、并发上限、中间件、Redis TTL 缓存、稳定幂等键和协作式取消。
- JSON typed checkpoint、pending writes、历史、replay、fork 与任意深度子图 namespace。
- PostgreSQL 事务队列、租约/心跳/回收、同线程单 active run 与 Redis 故障降级。
- FastAPI `/v1`、SSE `Last-Event-ID` 续传、Python SDK、A2A 与 MCP 双向适配。
- OIDC/JWT、固定 RBAC、tenant claim、PostgreSQL RLS、审计、配额和 OpenTelemetry。
- supervisor、manager-as-tools、handoff、swarm、group chat、plan-execute、parallel review。
- 中立消息/工具/ChatModel 协议、`create_agent` ReAct 预制件和 HITL 工具审批。
- Coze Bot/工作流一等图节点、Coze ChatModel，以及 OpenAI 兼容模型适配器。
- 实时 token/custom 流、组合流模式、图结构/Mermaid、TTL 与可插拔语义记忆。

## 嵌入式 SDK

基础安装不强制引入任何 LLM/provider 或服务端依赖：

```bash
pip install lingxigraph
```

```python
from typing import TypedDict
from lingxigraph import END, START, Runtime, StateGraph

class State(TypedDict):
    request: str
    result: str

class Context(TypedDict):
    tenant: str

def resolve(state: State, runtime: Runtime[Context]):
    runtime.emit("progress", {"stage": "resolve"})
    return {
        "result": f"{runtime.context['tenant']}: {state['request']}",
    }

builder = StateGraph(State, context_schema=Context, name="support", version="2.0.0")
builder.add_node("resolve", resolve, timeout=30)
builder.add_edge(START, "resolve").add_edge("resolve", END)
graph = builder.compile()

print(graph.invoke(
    {"request": "reset access", "result": ""},
    context={"tenant": "acme"},
))
```

生产副作用应使用 `runtime.idempotency_key` 向下游服务去重。LingxiGraph 保证状态提交
幂等；外部网络调用采用至少一次语义。

## Agent 与工具

核心包不依赖任何模型 SDK。模型只需实现 `ChatModel.agenerate()`；支持流式时再实现
`astream()`。工具由类型注解生成 JSON Schema：

```python
from lingxigraph import AIMessage, HumanMessage, ToolCall, create_agent, tool

@tool
def search(query: str) -> str:
    """Search the internal knowledge base."""
    return f"result for {query}"

# model 可以是 CozeChatModel、OpenAICompatChatModel 或自定义实现。
agent = create_agent(model, [search], system_prompt="You are a support agent.")
result = agent.invoke({"messages": [HumanMessage("查找退款规则")]})
```

`messages` 使用稳定 ID upsert reducer，支持删除、checkpoint 无损往返、并行工具调用和
`Command(scope=PARENT)` 跨子图 handoff。

## Coze（扣子）

```bash
pip install "lingxigraph[coze]"
```

```python
import os
from lingxigraph import create_agent
from lingxigraph.integrations import AsyncCozeClient, CozeChatModel

client = AsyncCozeClient(os.environ["COZE_API_TOKEN"])
model = CozeChatModel("your_bot_id", client=client, user_id="user-001")
agent = create_agent(model)
```

`CozeAgentNode` 可续接 conversation、转发 SSE token、处理 requires_action、本地工具与
HITL；`CozeWorkflowNode` 支持工作流流式输出和中断恢复。中国站默认
`https://api.coze.cn`，国际站或兼容网关通过 `base_url` 配置。详见
[Coze 集成](docs/integrations-coze.md)。

## 本地生产栈

安装全部平台组件：

```bash
pip install "lingxigraph[all]"
docker compose up --build
```

Compose 会启动 PostgreSQL 16、Redis 7.2、迁移任务、一个 Agent Server 和两个 Worker。
示例可信图由 [lingxigraph.json](lingxigraph.json) 注册，服务位于
`http://localhost:8124`。本地栈启用显式的不安全开发认证；不要在生产环境使用该设置。

生产环境使用：

```bash
lingxigraph doctor
lingxigraph migrate
lingxigraph server
lingxigraph worker
```

Kubernetes Chart 位于 [deploy/helm/lingxigraph](deploy/helm/lingxigraph)。镜像以 UID 10001
非 root 运行，启用只读根文件系统、默认 seccomp、健康检查、优雅终止、HPA 与 PDB。
生产镜像使用带哈希的 `requirements.lock`，CI 使用 `requirements-dev.lock`；`uv.lock`
是依赖解析真相，修改 `pyproject.toml` 后必须以 `uv lock --check` 校验同步。

## 可信图清单

Worker 只导入随镜像或签名制品发布的 Python 图，不接受在线上传代码：

```json
{
  "graphs": {
    "support": {"path": "myapp.graphs:graph", "version": "2.0.0"}
  }
}
```

部署前 `lingxigraph doctor` 会导入清单、编译图并校验可生成 JSON Schema。运行状态和事件
只允许安全 JSON typed serializer 支持的类型；pickle 不再用于生产状态。

## 文档

- [v2 架构与执行语义](docs/architecture.md)
- [Agent、工具与多智能体模式](docs/agents.md)
- [Coze 集成](docs/integrations-coze.md)
- [独立 Chainlit 适配层](adapters/chainlit/README.md)
- [REST、SSE 与 Python SDK](docs/api.md)
- [生产运维手册](docs/operations.md)
- [安全与多租户](docs/security.md)
- [ADR：耐久执行与平台边界](docs/adr/0001-durable-agent-platform.md)
- [ADR：中立 Agent 层与 Coze 集成](docs/adr/0002-agent-layer-and-coze.md)

## 2.1 路线图

2.0 聚焦核心运行时与智能体层。cron 执行器、webhook、assistant 版本化、分页/线程
搜索复制、DLQ redrive、OTel 启动激活、MCP 同步结果与 A2A message/stream 计划在 2.1
补齐。

## 验证

```powershell
$env:PYTHONPATH = (Resolve-Path "src").Path
python -m unittest discover -s tests -v
python -m compileall -q src tests
python -m build
```

PostgreSQL、Redis、RLS 与故障注入集成场景在 CI 的 Testcontainers 作业执行。发布流水线还
运行 Ruff、mypy、分支覆盖率、依赖审计、镜像扫描与 CycloneDX SBOM 生成。
