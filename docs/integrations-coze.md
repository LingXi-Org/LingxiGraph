# Coze（扣子）集成

安装 `lingxigraph[coze]` 后可使用 `AsyncCozeClient`、`CozeAgentNode`、
`CozeWorkflowNode` 和 `CozeChatModel`。客户端只依赖 httpx；PAT 可直接传入，也可使用同步或
异步 `token_provider` 做企业令牌轮换。

`CozeAgentNode` 将 MessagesState 转成 additional_messages，并把每个 bot 的
conversation_id 写入用户声明的 `coze_conversations` 状态键。SSE delta 通过 messages 模式
实时输出。requires_action 可交给本地工具或通过 `hitl=True` 触发耐久审批 interrupt。

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

端点集中在 `integrations/coze.py` 的 `_ENDPOINTS`。默认中国站为 `api.coze.cn`；国际站、
Coze Studio 或企业网关应显式配置 `base_url`，并在升级时用 MockTransport 契约测试核对事件名。
