# Changelog

## 2.0.0

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
