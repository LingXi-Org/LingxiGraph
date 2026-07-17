# Changelog

## 2.0.0

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
