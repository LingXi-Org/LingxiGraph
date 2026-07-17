# ADR-0002：中立 Agent 层与 Coze 优先集成

状态：已接受（LingxiGraph 2.0）

## 决策

核心包加入供应商中立的消息 dataclass、工具 Schema、ChatModel Protocol、ToolNode 与
create_agent，但继续保持零第三方依赖。厂商能力放在可选 `integrations` 边界；Coze 是旗舰
集成，OpenAI-compatible REST 是通用补充。

## 理由

消息 reducer、工具调用和 ReAct 循环是可组合多智能体运行时的基础语义，不应由每个应用
重复发明。与此同时，把 cozepy、OpenAI SDK 或 LangChain 放进核心会推翻 ADR-0001 的部署
边界并放大依赖面。基于 httpx 的窄适配器能保持协议透明、便于 MockTransport 契约测试。

## 后果

应用可直接编排 Coze Bot/工作流，也能实现自己的 ChatModel。Coze API 漂移需要维护集中端点
表和 SSE fixture；远端调用仍是至少一次语义，业务副作用必须幂等。
