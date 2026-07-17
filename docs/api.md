# REST、SSE 与 Python SDK

OpenAPI 是 Agent Server 的协议真相。默认服务地址为 `http://localhost:8124`，版本化资源
位于 `/v1`。除 `/health` 与 `/ready` 外，接口需要 OIDC bearer token 或受控开发 API key。

## 资源

| 资源 | 主要接口 |
| --- | --- |
| Graph registry | `GET /v1/graphs`、`GET /v1/graphs/{id}` |
| Assistants | `POST/GET /v1/assistants`、`GET/PATCH/DELETE /v1/assistants/{id}` |
| Threads | `POST/GET /v1/threads`、`GET/PATCH/DELETE /v1/threads/{id}`、state、history、fork、runs |
| Runs | threaded/stateless create、get/list、join、resume、cancel、redrive、stream |
| Store | `POST /v1/store/batch`、`GET /v1/store/search` |
| Schedules | create/list/update/delete |
| Interop | `/a2a/{assistant_id}`、`/mcp` |
| Operations | `/health`、`/ready`、`/metrics` |

创建 run 返回 HTTP 202 和 `pending` 资源。状态固定为 `pending`、`running`、`paused`、
`succeeded`、`failed`、`cancelling`、`cancelled`、`timed_out`、`dead_letter`。业务失败不会用 HTTP 状态
覆盖 run 状态；查询 run 的 `error.code` 获取稳定机器码。

创建 threaded/stateless run 可携带 `Idempotency-Key`（1–255 字符）。key 在 tenant 内唯一；
相同 key 和相同请求返回原 run，不会再次入队；不同请求复用 key 返回 HTTP 409 和
`idempotency_conflict`。建议所有自动重试客户端都发送稳定 key。

`RunCreate` 可设置 `max_model_calls`、`max_tool_calls`、`max_tokens`、`max_cost` 和
`run_timeout`。预算由父子图共享，超限 run 以 `budget_exceeded` 失败。transient delivery
耗尽重试后进入 `dead_letter`；排障后调用 `POST /v1/runs/{run_id}/redrive` 重置 attempt 并重新入队。

assistant 可在创建时指定 `graph_version`。每个 run 固定 graph ID/version 以及合并后的
config/context；paused run 恢复时继续原执行契约，不读取随后修改过的 assistant 配置。

## 请求示例

```bash
curl -X POST http://localhost:8124/v1/assistants \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-ID: acme' \
  -d '{"graph_id":"production-support","name":"support"}'

curl -X POST http://localhost:8124/v1/threads \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-ID: acme' \
  -d '{}'

curl -X POST http://localhost:8124/v1/runs \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-ID: acme' \
  -H 'Idempotency-Key: support-ticket-123-attempt-1' \
  -d '{"assistant_id":"...","input":{"request":"reset access"},"max_model_calls":8}'
```

`X-Tenant-ID` 只在 `LINGXIGRAPH_INSECURE_DEV_AUTH=true` 时生效。生产 tenant 必须从已验证
JWT claim 派生，绝不信任调用方自报 header。

## SSE 续传

```text
GET /v1/runs/{run_id}/stream
Accept: text/event-stream
Last-Event-ID: 17
```

每条事件形如：

```text
id: 18
event: node_completed
data: {"run_id":"...","sequence":18,"kind":"node_completed","data":{...}}
```

事件在发送前已写入 PostgreSQL。断线、API Pod 重启或 Redis 重启后，客户端使用最后确认
的 id 继续。客户端应按 `(run_id, sequence)` 去重，并允许 heartbeat 注释行。

## Python SDK

```python
from lingxigraph.sdk import LingxiGraphClient

with LingxiGraphClient(
    "https://agents.example.com",
    token="...",
) as client:
    assistant = client.assistants.create(graph_id="support")
    thread = client.threads.create()
    run = client.runs.create(
        assistant_id=assistant["id"],
        thread_id=thread["id"],
        input={"request": "reset access", "result": ""},
    )
    for event in client.runs.stream(run["id"]):
        print(event)
```

`AsyncLingxiGraphClient` 提供资源一一对应的异步方法。SDK 对非 2xx 响应抛出包含 HTTP 状态、
稳定 problem code、request ID 和 retryable 标记的错误。

## Problem details

平台错误使用 `application/problem+json`：

```json
{
  "type": "about:blank",
  "title": "Quota Exceeded",
  "status": 429,
  "detail": "tenant queued-run quota exceeded",
  "code": "quota_exceeded",
  "request_id": "...",
  "retryable": true
}
```

客户端只能根据 `code` 和 `retryable` 分支，不应解析自然语言 detail。
