# Coze（扣子）集成

安装 `lingxigraph[coze]` 后可使用 `AsyncCozeClient`、`CozeAgentNode`、
`CozeWorkflowNode` 和 `CozeChatModel`。客户端只依赖 httpx；PAT 可直接传入，也可使用同步或
异步 `token_provider` 做企业令牌轮换。

`CozeAgentNode` 将 MessagesState 转成 additional_messages，并把每个 bot 的
conversation_id 写入用户声明的 `coze_conversations` 状态键。SSE delta 通过 messages 模式
实时输出：每个 delta 立即形成 `AIMessageChunk` event，节点仍在等待后续 Coze SSE 数据时外层
`astream(..., stream_mode="messages"|"events")` 已可消费，Chainlit 可直接逐 token 调用
`stream_token()`。requires_action 可交给本地工具或通过 `hitl=True` 触发耐久审批 interrupt。

## 完整能力（对齐 Coze 开发者文档）

- **流式返回**：SSE `conversation.message.delta` 的 `content` 逐 token 通过 messages 模式输出。
- **思考信息流式输出**：delta 中的 `reasoning_content` 单独形成 `AIMessageChunk`，并在
  `additional_kwargs={"reasoning": True}` 上打标，Chainlit 侧据此渲染独立的“思考中”Step；
  完整思维链也汇总到最终 `AIMessage.additional_kwargs["reasoning_content"]`。
- **用户问题建议（follow-up）**：`conversation.message.completed` 中 `type == "follow_up"`
  的消息被收集为建议问题，写入最终 `AIMessage.additional_kwargs["follow_ups"]` 与
  `response_metadata["follow_ups"]`；若 `CozeAgentNode(suggestions_key=...)` 指定了状态键
  （且图 state schema 已声明），也会写入该键。非流式轮询路径从 `chat/message/list` 的
  `type == "follow_up"` 条目提取，逻辑一致。
- **文件上传**：`AsyncCozeClient.upload_file(content, filename=..., content_type=...)` 调用
  `/v1/files/upload`（multipart），返回含 `id` 的文件元数据。用 `file_object(file_id)` /
  `image_object(file_id)` / `text_object(text)` 构造 `object_string` 项，放入
  `HumanMessage(additional_kwargs={"objects": [...]})`；`_message_to_coze` 会自动切换为
  `content_type="object_string"`，并在缺省时把消息正文补成首个 text 项。

`suggestions_key` 默认 `None`：只有显式指定且 state schema 声明该键时才写入，避免破坏严格
schema 的图。

`CozeWorkflowNode` 遇到工作流 interrupt 时返回 `coze_workflow_question`。恢复值必须回显
`event_id`、`interrupt_type` 和 `resume_data`，例如：

```python
Command(resume={
    "event_id": "event-id-from-interrupt",
    "interrupt_type": 2,
    "resume_data": "用户答案",
})
```

外部 Coze 调用采用至少一次语义：若进程在远端成功、checkpoint 提交前崩溃，恢复可能重放
一次调用。对有副作用的本地工具使用 `runtime.idempotency_key`，并在业务侧去重。

客户端对 408/409/425/429/5xx 和网络错误执行有上限的指数退避，遵守 `Retry-After`。同一
逻辑调用复用 `X-Idempotency-Key`；SSE 重连携带 `Last-Event-ID` 并按 event ID 去重。run
取消时，流式与轮询 bot 调用会尽力调用 Coze cancel endpoint，然后传播取消状态。

Coze 本地 requires_action 工具复用核心 `ToolNode`，因此参数 schema、permission、动态授权、
secret resolver、timeout、预算与取消语义一致。

端点集中在 `integrations/coze.py` 的 `_ENDPOINTS`。默认中国站为 `api.coze.cn`；国际站、
Coze Studio 或企业网关应显式配置 `base_url`，并在升级时用 MockTransport 契约测试核对事件名。
