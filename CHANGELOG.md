# Changelog

## Unreleased

### Durable mid-run steering (issue #16)

- 新增 `POST /v1/runs/{run_id}/steer`：在 Run 执行期间（含 `paused`）durably 注入新的
  结构化输入，不等待完成、不强制取消、也不需要先暂停。支持 `Idempotency-Key`
  header/body 去重、`kind`/`payload`/`metadata` 结构化 payload、大小上限校验。
- Python SDK（同步/异步）新增 `client.runs.steer(run_id, kind=..., payload=..., metadata=...,
  idempotency_key=...)`。
- Runtime 新增 `runtime.has_steering`、`runtime.peek_steering()`、
  `runtime.drain_steering()`；`compiled_graph.steer(run_id, ...)` 在嵌入式/无 Server
  场景下提供同一套语义，未使用时零开销、不泄漏 channel 状态。
- Run 生命周期新增 `run.steer.accepted`、`run.steer.consumed`、`run.steer.superseded`
  SSE 事件；`consumed` 事件携带 `steering_event_id`、`source_event_id`、`sequence`、
  `kind`、`queue_latency_seconds`、`node`、`namespace`、`task_id`；`superseded` 事件
  在 resume-transfer 与 final-boundary disposition 两种场景下幂等写入，同样携带
  `sequence`/`kind`/`source_event_id`。
- Resume 语义：`POST /v1/runs/{run_id}/resume` 在**同一原子事务**中创建新 Run 并把旧
  Run 上仍 `pending`/`delivered` 的 steering 事件迁移到新 Run（保序、保留身份）；旧
  Run 之后再收到 `/steer` 或 `/cancel` 返回 `409 run_superseded`。`source_event_id`
  在跨多次 pause/resume 时始终指向客户端最初 `/steer` 收到的根 id，`created_at`/
  `queue_latency_seconds` 从最初 durable accepted 时刻起算，不因中间的 resume 跳数
  被重置。旧 Run 是否已被 resume 取代由新增的 typed `runs.superseded_by_run_id` 列
  记录（与该事务同时原子写入），而不是用户可写的 `metadata`，避免调用方在创建 Run
  时伪造该字段来假冒 lineage 状态。
- Cancel 语义：`request_cancel()` 现在对每个非终态状态都有明确策略——`pending`/
  `paused`（没有存活 worker 可协作）立即变为 `cancelled`；`running` 变为
  `cancelling`，由持有该 Run 的 worker 协作完成终结；已被 resume 取代
  （`superseded_by_run_id` 已设置）的历史 Run 拒绝为 `409 run_superseded`，不会
  假成功。`claim_run()` 的租约过期回收同样区分：过期的 `running` 恢复为 `pending`；
  过期的 `cancelling`（worker 在收到 cancel 后、终结提交前崩溃）恢复为 `cancelled`，
  避免 Run 永久卡死在 `cancelling` 并阻塞同 thread 后续任务。
- `commit_steering_consumptions_if_owned()`（状态更新 + `run.steer.consumed` 写入）
  与 `finalize_run_with_steering_disposition_if_owned()`（终态提交 + 未消费 steering
  的 `superseded` disposition + `run.steer.superseded` 写入）均在同一事务/临界区内
  完成，并按 `(lease_owner, attempt)` fenced：同一批 consumption 的重复提交（例如
  worker 在收到 ack 前连接断开后重试）只会产生一次 `run.steer.consumed`；旧 attempt
  在 lease 被新 worker 接管后无法再修改该 Run 的 durable 状态。`retry_run_with_event_
  if_owned()` 同样把状态转换与 `worker_retrying` 事件合并为一次原子写入，cancel 优先
  于 stale 的 retry 结果。
- PostgreSQL schema：新增 `run_steering_events` 表及 `runs` 表的多个新增列，共 4 个
  纯新增、forward-only 的 Alembic revision：
  - `0002_steering` — `run_steering_events` 表本身；
  - `0003_steering_source_event` — `source_event_id` 列/局部索引；
  - `0004_steering_closed` — `runs.steering_closed`（finalization 阶段的 steering
    admission 门控）；
  - `0005_superseded_by_run_id` — `runs.superseded_by_run_id`（typed resume-lineage
    marker，见上）。
  `alembic upgrade head` 是生产环境唯一支持的迁移路径，**必须到达
  `0005_superseded_by_run_id`**——`/steer`、`/cancel`、finalization、resume 的生产
  SQL 直接依赖 `0004`/`0005` 新增的列，只迁移到 `0003` 会导致这些代码路径在缺列的
  schema 上报 `UndefinedColumn` 错误。四个 revision 均可在部署新二进制前对存活数据库
  提前执行（schema 层面向后兼容，旧二进制会忽略新增的 nullable 列）；但 schema
  兼容不等于语义兼容——只有在控制面实例全部升级到本版本后，`steering_closed`/
  `superseded_by_run_id` 才会被读取并生效，回滚二进制会失去这些新语义（不是完整的
  语义回滚保证），必要时应在回滚前 quiesce 或等待 in-flight Run 结束。Redis 仅用于
  可选的低延迟通知，从不作为唯一数据源；Redis 丢失不影响最终投递。
- 兼容性：不影响不使用 steering 的现有图与调用方；上述 schema 变更均为纯新增列/表，
  无 breaking API 变更。
- 新增中英文文档：`Wiki/content/docs/{zh,en}/api/threads-runs.mdx`（steer API 全量
  语义）、`python-sdk.mdx`（SDK 示例）、`errors-events.mdx`（错误码与生命周期事件）、
  新增概念页 `Wiki/content/docs/{zh,en}/concepts/steering.mdx`、新增运维页
  `Wiki/content/docs/{zh,en}/operations/steering-operations.mdx`；`docs/api.md`
  同步更新。

## 2.2.0

### Cache-first prompt optimization

- 新增 provider-neutral 的 `ImmutablePrefix`、`CacheFirstConfig` 与 `CacheFirstChatModel`，将稳定
  system prompt、固定约束、few-shot 和 canonical tool catalog 与动态请求后缀分离。
- `create_agent()` 和 `OpenAICompatChatModel` 保持向后兼容；支持显式 prefix、prefix drift 诊断、
  `cache_first=False` 关闭路径，以及自定义 `ChatModel` 包装。
- 新增 history hygiene、bounded context compaction、确定性摘要回退、Skills/MCP progressive
  discovery 和 `cache_telemetry` custom channel。
- 统一 DeepSeek、OpenAI-compatible 和兼容 Anthropic usage 字段，提供 cache hit/miss、命中率、
  token savings、成本估算与可重启的 `UsageLedger` 累计统计；不把完整 prompt 或 tool result 写入
  telemetry/checkpoint metadata。
- 新增中英文 cache-first 指南、2.2.0 迁移说明、DeepSeek benchmark 脚本与示例。

## 2.1.0

### Agent Skills Runtime

- 新增符合开放 Agent Skills 规范的 `SKILL.md` runtime，包括 `SkillSource`、
  `FilesystemSkillSource`、`SkillRegistry`、发现、严格验证与 progressive disclosure。
- `create_agent()` / `create_react_agent()` 新增向后兼容的 keyword-only `skills=` 参数；初始
  上下文只注入经 XML 转义的 name 与 description，完整指令和资源仅通过
  `read_skill` / `read_skill_resource` 按需读取。
- Skill 工具沿用现有 Tool Calling、tool budget、timeout、动态授权、HITL、流式事件与
  checkpoint 路径；`allowed-tools` 仅为提示，不能创建能力或绕过权限。
- 资源访问仅限 `references/`、`scripts/` 与 `assets/` 中的普通文件，包含 traversal、
  symlink、junction、reparse point、特殊文件与大小限制防护；不提供脚本执行 API。
- 新增中英文概念/API/安全/迁移文档、完整 `skills/hello` 以及离线 ReAct 动态加载示例。
- 产品包、MCP serverInfo、Helm chart/image 与脚手架依赖元数据升级至 2.1.0。

## 2.0.1

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
