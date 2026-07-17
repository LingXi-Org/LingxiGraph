# Agent、工具与多智能体模式

LingxiGraph 2.0 在零依赖核心中定义消息、工具和模型协议，不绑定 LangChain 或任何模型厂商。

`MessagesState` 的 `messages` 使用 `add_messages` reducer：按 ID 更新、保持顺序，并支持
`RemoveMessage`。系统、人类、AI、工具消息和流式 chunk 都能经 JSON serializer v2 无损
进入 SQLite/PostgreSQL checkpoint。

`@tool` 从 Python 类型注解生成 JSON Schema。`ToolNode` 并行执行 AI 消息中的 tool calls，
默认把异常转成 `ToolMessage(status="error")`；返回 `Command` 的 handoff 工具必须是该轮唯一
工具调用。运行前会验证必填/未知/类型/enum 参数，并可配置：

- `timeout`：单次调用截止；
- `permissions` 与 `tool_authorize`：静态 capability 和动态策略双重授权；
- `secret_refs` 与 `secret_resolver`：凭据只在调用边界注入，不进入模型 schema/state；
- `requires_approval`：由 `create_agent` 自动接入耐久 HITL；
- `Runtime`、`ToolCall`、`idempotency_key` 参数：按签名自动注入。

预算、取消和 run deadline 属于控制流异常，不会被转换为普通工具消息。并行工具共享同一
线程安全预算计数器。

`create_agent(model, tools)` 构造 agent → tools → agent 的耐久 ReAct 循环，支持 system
prompt、pre/post model hook、流式消息、remaining-steps 收尾，以及 interrupt 驱动的工具审批。
模型实现 `ChatModel` 协议即可接入。配置 `response_format` 后，最终结果会做 JSON Schema 或
Pydantic 校验，并在 `structured_retries` 范围内携带验证错误请求模型修复。

自定义流遵循 LangGraph 的单参数 writer 接口：节点可调用
`get_stream_writer()(value)` 或 `runtime.stream_writer(value)`；`stream_mode="custom"` 直接
yield 原始 value，多模式时 yield `("custom", value)`。`runtime.emit(channel, value)` 是保留的
命名通道扩展。writer 写入会立刻唤醒外层 async iterator，不等待节点完成或超步 commit。

`@task` 为普通同步/异步函数提供稳定 task key。图配置 Store 后，成功结果会按 key 持久化，
Worker 重试或租约恢复复用结果；副作用服务仍应使用注入的 `idempotency_key` 做最终去重。

多智能体层提供 supervisor、manager-as-tools、handoff、swarm、group chat、plan-execute 与
parallel review。`create_handoff_tool()` 返回 `Command(scope=PARENT)`，使 LLM 可把控制权从
agent 子图交还父级编排图。持久 swarm 应在 state 中声明 `active_agent`。
group chat 必须设置有限 `max_turns`（默认 20），也可提供 deterministic termination；停止时
发出 `group_chat_stopped` custom event。LLM selector 的调用同样计入 run 模型预算。
