<div align="center">

# LingxiGraph

**面向生产环境的 Agent 图运行时。**

将 Agent、工具与普通 Python 节点组织成可持久化、可恢复、可观察的状态图。

[English](README.en.md) · [中文文档](Wiki/content/docs/zh/index.mdx) · [快速开始](Wiki/content/docs/zh/quickstart/installation.mdx) · [Changelog](CHANGELOG.md)

</div>

## 关于 LingxiGraph

LingxiGraph 是 LingXi 系列的 Agent 执行底座，负责一次任务如何运行、暂停、恢复、重试和产生事件。

它不绑定具体模型厂商，也不负责产品 UI 或业务规则。上层应用只需要定义状态、节点和边，运行时负责把它们可靠地执行下去。

```text
Application
    │
    ▼
LingxiGraph
StateGraph · Runtime · Checkpoint · Events
    │
    ├── Models
    ├── Tools / Skills
    └── PostgreSQL / Redis
```

## 核心能力

- **StateGraph**：用节点和边描述 Agent 与业务执行逻辑。
- **Durable Execution**：支持 checkpoint、暂停恢复、重试与长任务执行。
- **Durable Steering**：运行中（甚至暂停中）durably 注入新的结构化输入，无需取消 run；投递
  durable、有序、去重，并能在 worker 崩溃和 paused run resume 后依然可靠送达。
- **Streaming Events**：通过 Python Runtime、REST 与可续传 SSE 暴露运行状态。
- **Production Runtime**：提供 Agent Server、Worker、持久化、权限、审计与观测能力。
- **Open Integrations**：支持 OpenAI-compatible、Coze、A2A、MCP 与开放 Agent Skills。

## 快速开始

要求 Python 3.11+。

```bash
pip install lingxigraph
```

```python
from typing import TypedDict
from lingxigraph import START, END, StateGraph

class State(TypedDict):
    text: str

def hello(state: State):
    return {"text": f"Hello, {state['text']}"}

graph = StateGraph(State)
graph.add_node("hello", hello)
graph.add_edge(START, "hello")
graph.add_edge("hello", END)

app = graph.compile()
print(app.invoke({"text": "LingxiGraph"}))
```

本地 Agent 开发：

```bash
pip install "lingxigraph[server]"
lingxigraph new my-agent
cd my-agent
lingxigraph dev
```

Studio 默认可用于查看图结构、运行事件、thread、checkpoint 与中断恢复。

## 运行方式

```text
Python SDK        嵌入现有 Python 应用
Agent Server      REST / SSE 服务入口
Worker            分布式任务执行
Studio            图与运行状态调试界面
PostgreSQL        持久状态与任务真相来源
Redis             可选缓存、取消与事件加速
```

## 文档

完整文档位于 [`Wiki/`](Wiki/README.md)：

- [安装](Wiki/content/docs/zh/quickstart/installation.mdx)
- [第一个图](Wiki/content/docs/zh/quickstart/first-graph.mdx)
- [Agent Server](Wiki/content/docs/zh/quickstart/agent-server.mdx)
- [核心架构](Wiki/content/docs/zh/concepts/architecture.mdx)
- [Agent Skills](Wiki/content/docs/zh/concepts/agent-skills.mdx)

## License

LingxiGraph 采用 [MIT License](LICENSE)。
