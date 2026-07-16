# LingxiGraph 1.0

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

builder = StateGraph(State, context_schema=Context, name="support", version="1.0.0")
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

v1 Worker 只导入随镜像或签名制品发布的 Python 图，不接受在线上传代码：

```json
{
  "graphs": {
    "support": {"path": "myapp.graphs:graph", "version": "1.0.0"}
  }
}
```

部署前 `lingxigraph doctor` 会导入清单、编译图并校验可生成 JSON Schema。运行状态和事件
只允许安全 JSON typed serializer 支持的类型；pickle 不再用于生产状态。

## 文档

- [v1 架构与执行语义](docs/architecture.md)
- [REST、SSE 与 Python SDK](docs/api.md)
- [生产运维手册](docs/operations.md)
- [安全与多租户](docs/security.md)
- [ADR：耐久执行与平台边界](docs/adr/0001-durable-agent-platform.md)

## 验证

```powershell
$env:PYTHONPATH = (Resolve-Path "src").Path
python -m unittest discover -s tests -v
python -m compileall -q src tests
python -m build
```

PostgreSQL、Redis、RLS 与故障注入集成场景在 CI 的 Testcontainers 作业执行。发布流水线还
运行 Ruff、mypy、分支覆盖率、依赖审计、镜像扫描与 CycloneDX SBOM 生成。
