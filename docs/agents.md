# Agent、工具与多智能体模式

LingxiGraph 2.0 在零依赖核心中定义消息、工具和模型协议，不绑定 LangChain 或任何模型厂商。

`MessagesState` 的 `messages` 使用 `add_messages` reducer：按 ID 更新、保持顺序，并支持
`RemoveMessage`。系统、人类、AI、工具消息和流式 chunk 都能经 JSON serializer v2 无损
进入 SQLite/PostgreSQL checkpoint。

`@tool` 从 Python 类型注解生成 JSON Schema。`ToolNode` 并行执行 AI 消息中的 tool calls，
默认把异常转成 `ToolMessage(status="error")`；返回 `Command` 的 handoff 工具必须是该轮唯一
工具调用。

`create_agent(model, tools)` 构造 agent → tools → agent 的耐久 ReAct 循环，支持 system
prompt、pre/post model hook、流式消息、remaining-steps 收尾，以及 interrupt 驱动的工具审批。
模型实现 `ChatModel` 协议即可接入。

多智能体层提供 supervisor、manager-as-tools、handoff、swarm、group chat、plan-execute 与
parallel review。`create_handoff_tool()` 返回 `Command(scope=PARENT)`，使 LLM 可把控制权从
agent 子图交还父级编排图。持久 swarm 应在 state 中声明 `active_agent`。
