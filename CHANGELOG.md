# Changelog

## Unreleased

- Coze 集成补齐开发者文档全量能力：`AsyncCozeClient.upload_file` 走 `/v1/files/upload`
  （multipart）；新增 `file_object`/`image_object`/`text_object` 与 `object_string` 消息编码，
  支持在 `additional_messages` 中携带文件/图片。
- `CozeAgentNode`/`CozeChatModel` 流式输出 `reasoning_content`（思考信息，打
  `additional_kwargs={"reasoning": True}` 标记）并收集 `follow_up` 用户问题建议，写入最终
  `AIMessage.additional_kwargs`（`reasoning_content`/`follow_ups`）与 `response_metadata`。
  `CozeAgentNode(suggestions_key=...)` 可选把建议写入 state（默认 `None`，不破坏严格 schema）。
- 修复：`conversation.message.delta`/`.completed` 流式路径此前未按 `data.type` 过滤，
  `verbose`（多智能体 jump 信息）、`function_call`、`knowledge_recall` 等非正文消息会被
  错误拼接进可见回答；新增 `_is_answer_delta` 只放行 `type in (None, "answer")`。
- 新增会话/消息管理端点：`conversation_retrieve`、`conversation_message_create`、
  `conversation_message_list`（游标分页）、`conversation_message_retrieve`；新增
  `file_retrieve`（查询上传文件状态）与 `bot_retrieve`（bot 元信息）。
- `conversation.chat.completed` 的 `usage`（`token_count`/`input_count`/`output_count`）
  现在会写入最终 `AIMessage.usage`，流式与轮询路径均覆盖。

## 2.0.0

### 开发者体验与 Studio 1.0

- 新增开发者 CLI：`lingxigraph new`（项目脚手架）、`dev`（内存 + 内嵌 Worker + Studio 的本地
  开发服务器，支持 `--reload`）、`build`（镜像/wheel 构建）、`up`（Docker Compose 单服务器栈）。
  以 Docker Compose 单服务器部署为主要交付方式（`api` 服务内嵌 Worker 并托管 Studio）。
- 完整实现 Studio 1.0：从真实 Agent Server API 驱动的图调试 IDE。真实图拓扑渲染（分层布局、
  条件边）、一键运行图并通过 SSE 实时呈现节点级执行轨迹与事件流、真实 thread 状态/历史/检查点
  检查器、节点解释与调试（实现/Runtime/超时/重试/并发护栏/控制流）、interrupt 检查与 resume。
- 实现 `CompiledGraph.get_graph(xray=True)` 递归子图展开与调试元数据（`kind`、`debug`、嵌套
  `subgraph`），`draw_mermaid(xray=True)` 输出嵌套 Mermaid 子图；Studio 支持 X-ray 逐层下钻。
- `/v1/graphs/{id}/structure` 返回节点调试元数据、图信息与 Mermaid；Studio 静态资源在存在时
  始终挂载于 `/studio`，`/` 重定向至 Studio。
- 新增 `lingxigraph.examples.multi_agent_graph`：模型中立的多智能体展示图（并行 fan-out、
  reducer 归并、嵌套研究子图），演示真正的多智能体图运行时。

### 平台核心

- 完成 MVP P0/P1 硬化：强类型 state/output/工具参数校验、结构化输出修复、工具权限/审批/
  secret/timeout，以及共享模型/工具/token/cost 预算。
- graph registry 改为 ID+version 双键，manifest 支持同 ID 多版本；assistant/run/resume/Worker
  固定精确图版本与执行配置。
- Run API 增加 tenant 级 `Idempotency-Key` 冲突检测；PostgreSQL advisory lock 防止并发重复入队。
- Worker 增加 transient retry、dead-letter、redrive、SIGTERM drain、独立 health/readiness；
  API readiness 检查数据库，增加 request/rate/state/event 限额。
- OpenAI-compatible 与 Coze adapter 增加 Retry-After/退避、稳定 provider 幂等 key、流式 usage、
  SSE resume/去重和协作式远端取消。
- `get_stream_writer()`/`Runtime.stream_writer` 对齐 LangGraph `writer(value)`，custom/message
  chunk 在节点结束前实时交付；关闭 consumer 会取消尚未完成的流式 task。
- 进程启动自动激活 JSON 日志与可配置 OTel，API/package/tracer 统一使用 `2.0.0` 版本。

- 新增中立消息、`add_messages`、工具 Schema/ToolNode、ChatModel 与 `create_agent`。
- 新增 Coze Bot/工作流/模型集成和 OpenAI-compatible 模型适配器。
- 实现 `Command(scope=PARENT)`、`output_schema`、per-run concurrency 与纯异步 saver API。
- serializer/checkpoint 写 v2、读 v1；SQLite pending writes 主键加入 namespace 并自动迁移。
- `Durability.ASYNC` 使用有序后台写与完成/中断 flush 屏障。
- custom/message emit 改为实时泵出；messages 模式载荷改为 `(message, metadata)`；组合流模式
  产出 `(mode, chunk)`。
- Event sequence 在每个 run 内从 1 单调递增，重试发出 `NODE_RETRYING`。
- 新增 Topic、EphemeralValue、图结构/Mermaid、Store TTL 与 Embedder 钩子。

升级后包含注册消息类型的节点缓存键会发生一次性 miss。旧 serializer v1 继续可读；SQLite
setup 会把 v1 writes 迁入默认 namespace，无法推断的历史子图 writes 按至少一次语义重跑。
