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
- run 级模型调用、工具调用、token 与成本预算；state/event/request 尺寸和 tenant 速率限制。
- JSON typed checkpoint、pending writes、历史、replay、fork 与任意深度子图 namespace。
- PostgreSQL 事务队列、租约/心跳/回收、同线程单 active run 与 Redis 故障降级。
- transient delivery 自动重试、dead-letter、人工 redrive、Worker drain 与独立存活/就绪探针。
- FastAPI `/v1`、SSE `Last-Event-ID` 续传、Python SDK、A2A 与 MCP 双向适配。
- OIDC/JWT、固定 RBAC、tenant claim、PostgreSQL RLS、审计、配额和 OpenTelemetry。
- supervisor、manager-as-tools、handoff、swarm、group chat、plan-execute、parallel review。
- 中立消息/工具/ChatModel 协议、`create_agent` ReAct 预制件和 HITL 工具审批。
- 强类型工具参数、权限策略、secret resolver、单工具 timeout 和结构化输出校验/修复。
- Coze Bot/工作流一等图节点、Coze ChatModel，以及 OpenAI 兼容模型适配器。
- 实时 token/custom 流、组合流模式、图结构/Mermaid、TTL 与可插拔语义记忆。
- `lingxigraph new/dev/build/up` 项目脚手架与开发者工作流；Docker Compose 单服务器交付。
- 内嵌 Studio 1.0：真实 API 图浏览器、实时 SSE 运行轨迹、状态/检查点检查器、
  节点解释与调试、X-ray 子图逐层展开。

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

def resolve_secret(reference: str) -> str:
    return secret_manager.read(reference)

@tool(
    permissions=("knowledge:read",),
    secret_refs={"token": "knowledge/api-token"},
    timeout=10,
)
def search(query: str, token: str) -> str:
    """Search the internal knowledge base."""
    return f"result for {query}"

# model 可以是 CozeChatModel、OpenAICompatChatModel 或自定义实现。
agent = create_agent(model, [search], system_prompt="You are a support agent.",
                     secret_resolver=resolve_secret)
result = agent.invoke(
    {"messages": [HumanMessage("查找退款规则")]},
    {"tool_permissions": ["knowledge:read"], "max_tool_calls": 4},
)
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

## 快速开始（开发者工作流）

v2 以 **Docker Compose 单服务器部署** 为主要交付方式，并提供完整的开发者 CLI：

```bash
lingxigraph new my-agent      # 脚手架一个可直接运行的多智能体项目
cd my-agent && pip install -e .
lingxigraph dev               # 本地内存栈 + 内嵌 Worker + Studio
```

`lingxigraph dev` 启动一个零外部依赖的开发服务器（内存 checkpoint/store、内嵌 Worker），
自动打开 Studio（`http://localhost:8124/studio`），无需 PostgreSQL 或 Redis。

四个开发者命令：

| 命令 | 作用 |
| --- | --- |
| `lingxigraph new <name>` | 脚手架项目：图模块、manifest、Docker Compose、Dockerfile |
| `lingxigraph dev` | 内存 + 内嵌 Worker + Studio 的本地开发服务器（支持 `--reload`） |
| `lingxigraph build` | 构建部署镜像（`--wheel` 构建 Python wheel） |
| `lingxigraph up` | 启动 Docker Compose 单服务器栈 |

## Studio 1.0

内嵌 Studio 是连接真实 Agent Server 的图调试 IDE，服务于 `/studio`：

- **图浏览器**：从 `/v1/graphs/{id}/structure` 渲染真实拓扑，分层布局、条件边可视化。
- **X-ray 子图**：逐层展开嵌套子图，面包屑下钻，解释多智能体图的完整结构。
- **实时运行轨迹**：一键运行图，通过 SSE 实时呈现节点级执行时间线与完整事件流。
- **状态与检查点检查器**：从真实 thread 读取状态快照、历史与检查点，JSON 高亮与复制。
- **节点解释与调试**：展示节点实现、Runtime 注入、超时/重试/并发护栏、后继与控制流解释。
- **中断/恢复**：在 interrupt 暂停点检查状态并提交 resume 值继续运行。

## 单服务器部署（Docker Compose）

```bash
docker compose up --build          # 或 lingxigraph up
```

Compose 启动 PostgreSQL 16、Redis 7.2、迁移任务，以及一个内嵌 Worker 的 Agent Server
（`--embedded-worker`），服务位于 `http://localhost:8124`，Studio 在 `/studio`。示例可信图
由 [lingxigraph.json](lingxigraph.json) 注册。本地栈启用显式的不安全开发认证；不要在生产环境
使用该设置。

需要独立扩展 Worker 的多进程生产部署：

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
    "support": [
      {"path": "myapp.graphs:support_v1", "version": "1.0.0"},
      {"path": "myapp.graphs:support_v2", "version": "2.0.0"}
    ]
  }
}
```

同一 graph ID 可同时部署多个版本；列表最后一项是新 assistant 的默认版本。assistant 创建时
可显式指定 `graph_version`，run 会固定 graph/version/config/context，重试和恢复不会漂移到
新部署版本。

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

## 后续路线图

当前 MVP 已包含 graph version pinning、DLQ redrive、OTel 进程启动激活和生产 Worker
生命周期。后续版本聚焦 cron 执行器、webhook、不可变 assistant revision、分页/线程搜索复制、
MCP 同步结果与 A2A message/stream。

## 验证

```powershell
$env:PYTHONPATH = (Resolve-Path "src").Path
python -m unittest discover -s tests -v
python -m compileall -q src tests
python -m build
```

PostgreSQL、Redis、RLS 与故障注入集成场景在 CI 的 Testcontainers 作业执行。发布流水线还
运行 Ruff、mypy、分支覆盖率、依赖审计、镜像扫描与 CycloneDX SBOM 生成。
