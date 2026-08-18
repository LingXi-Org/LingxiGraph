# REST、SSE 与 Python SDK

OpenAPI 是 Agent Server 的协议真相。默认服务地址为 `http://localhost:8124`，版本化资源
位于 `/v1`。除 `/health` 与 `/ready` 外，接口需要 OIDC bearer token 或受控开发 API key。

## 资源

| 资源 | 主要接口 |
| --- | --- |
| Graph registry | `GET /v1/graphs`、`GET /v1/graphs/{id}` |
| Assistants | `POST/GET /v1/assistants`、`GET/PATCH/DELETE /v1/assistants/{id}` |
| Threads | `POST/GET /v1/threads`、`GET/PATCH/DELETE /v1/threads/{id}`、state、history、fork、runs |
| Runs | threaded/stateless create、get/list、join、resume、cancel、redrive、steer、stream |
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

## 运行中输入（Steering）

`POST /v1/runs/{run_id}/steer` 在 Run 执行期间durably地注入新的结构化输入，不等待完成、
不强制取消、也不需要先暂停。请求体：

```json
{"kind": "user_input", "payload": {"message": "..."}, "metadata": {}, "idempotency_key": "..."}
```

语义：

- 202 只代表已durably写入 PostgreSQL 的 steering inbox，不代表图已经处理；配置了
  `EventBus`（进程内或 Redis）时会发布一个可选的低延迟唤醒通知供 worker 心跳循环等待，
  但它不是事件流中的具名事件（不存在 `run.steer.available` 事件），也从不作为唯一来源——
  worker 自身的周期性 PostgreSQL 安全点检查始终是最终保证。
- `running`/`pending`/`cancelling` 均可接受；`paused` run 也接受durable写入。
- 终止态（`succeeded`/`failed`/`cancelled`/`timed_out`/`dead_letter`）对一个**新**事件
  返回 HTTP 409 `run_terminal`，不会静默生成一条永远不会被消费的事件。
- **finalization 闸门（`run_finalizing`）：** 一旦某个 worker 的图执行到达真正的终态结果，
  该 worker 会立即（在 durably flush 最终 steering 消费、提交最终状态**之前**）关闭这次
  投递尝试的新 steering 准入——此时 Run 行可能仍读作 `running`/`cancelling`，但已经没有
  更多图安全点可以消费任何新输入了。在这个窗口内提交一个**新**事件（尚无匹配
  idempotency key）会返回 HTTP 409 `run_finalizing`，而不是被静默接受进一个再也没人会
  drain 的 channel。`paused` 不属于这条终结路径，也不会关闭闸门；它会继续接受 steering，
  并在 resume 时把 pending steering 迁移到新的 descendant run。
- `Idempotency-Key` header 或 body 的 `idempotency_key` 二选一；同一 `(tenant, run, key)`
  只会创建一条事件——**且这条幂等重放检查发生在上面所有准入判断之前，而不是之后**：
  同一个 key 的重试永远返回已存在事件的当前持久化状态（`id`/`sequence` 不变），即使该
  Run 此后已经进入 `run_finalizing`、终止态、或被 superseded，也绝不会返回一个新的
  409。只有真正全新的 key 才会受终止态/`run_finalizing`/superseded 这些准入检查约束。
- cancel 优先于 steer：先 steer 后 cancel，cancel 依然立即生效，steer 从不撤销/延迟取消。
- payload+metadata 序列化后大小受限（默认约 32KB / 服务端事件字节上限，先到者生效）。

**Paused run 的 steering 归属哪个 run_id？** `POST /v1/runs/{run_id}/resume` 会创建一条
*新* Run（新的 `run_id`，通过 `metadata.resumed_from_run_id` 关联旧 Run），worker 只会为它
正在执行的 run_id 拉取 pending steering。因此 resume 端点在创建新 Run 后，会原子地把旧
Run 上所有仍处于 `pending`/`delivered` 的 steering 事件**迁移**到新 Run（保序、保留
`kind`/`payload`/`metadata`/`idempotency_key`，旧事件被标记为 `superseded`），随后由新 Run
的 worker 按正常路径投递、消费——即：paused 时提交的 steering 会在 resume 之后被新 Run
实际消费，而不是永远 pending。旧 Run 同时通过类型化的 `superseded_by_run_id` 字段标记为
「已被 resume 取代」；resume 之后再对旧 `run_id` 调 `/steer` 或 `/cancel` 会返回 HTTP 409
`run_superseded`，提示调用方改为操作新 Run。

图内部通过 `runtime.has_steering`、`runtime.peek_steering()`、`runtime.drain_steering()`
在安全点（节点开始前、节点完成后、下一 superstep 前、重试前、resume 之后）读取；
LingxiGraph 只保证durable delivery、顺序、去重与安全消费，"新消息是否意味着重新规划"完全
由业务图决定。嵌入式/无 Server 场景下同一套 API 生效：`compiled_graph.steer(run_id, ...)`。

**"安全点"由谁定义？** LingxiGraph 保证的是 channel 本身的一致性（原子 drain-once、去重、
顺序）以及*调用* `drain_steering()` 这件事发生时机的新鲜性——每次节点被 executor 调用时，
它读到的都是当前最新、未被消费过的事件集合。但**在节点函数内部的哪一行代码调用
`drain_steering()`，完全由应用节点自己决定**：executor 不会在节点函数内部插入任何强制
边界（例如"只能在函数开头调用一次"），也不会暂停用户代码的执行去插入安全点。这是刻意的
设计选择而非缺口：真正的边界语义体现在 executor *何时把节点当作一个 task 来调度*
（节点开始前、下一 superstep 前、重试前、resume 之后见上），而不是节点内部的执行位置。

**`source_event_id`：跨 resume 的稳定关联。** 迁移到新 Run 的 steering 事件会获得一个
全新的 `id`（它是新 Run 下一条独立的durable行），但其 `source_event_id` 字段回指客户端
最初 `/steer` 调用收到的那个 `id`。即使一个 Run 被反复 pause/resume 多次（A → B → C →
……），每一跳迁移都保留**最初**那个根 id（`source_event_id = 上一跳的 source_event_id
或上一跳的 id`），不会被中间某一跳的 id 覆盖。`created_at` 同样保留最初 durable
accepted 的时间，因此后续 `run.steer.consumed` 的 `queue_latency_seconds`
包含了全部 pause 等待时间，而不仅仅是最后一次 resume 之后的等待时间。客户端只需记住
最初 `/steer` 返回的 `id`，即可用它在后续 lifecycle 事件的 `source_event_id` 字段里找到
最终归宿，无需跟踪中间产生的每一个 run_id。

**accepted ≠ consumed。** `POST /steer` 的 202 响应只代表事件已durably写入
`run_steering_events`（`run.steer.accepted`，状态 `pending`）；只有当某个安全点的图代码
真正调用 `drain_steering()` 并且该 drain 结果被
`commit_steering_consumptions_if_owned()` durably 提交后，才会出现 `run.steer.consumed`。
两者严格分离：调用方不能仅凭 202 假设图已经看到这条输入。

**观测性：** steering 有三种专用 durable lifecycle 事件：

- `run.steer.accepted`：事件被 durably 接受；
- `run.steer.consumed`：图 drain 该事件且 fenced consumption commit 成功，包含
  `steering_event_id`、`source_event_id`（若来自迁移，否则省略）、`sequence`、`kind`、
  `queue_latency_seconds`、`node`、`namespace`、`task_id`；
- `run.steer.superseded`：事件未被消费但获得明确 durable 归宿。`reason=resume_transfer` 表示
  paused run 被 resume 后旧事件被迁移到 descendant run；
  `reason=unconsumed_at_final_boundary` 表示真正终态 finalization 时仍有
  `pending`/`delivered` steering。该 disposition、对应 lifecycle event 与 Run 的终态写入在
  同一个 lease/attempt-fenced 事务中提交，因此不会出现“Run 已终结但 steering 归宿丢失”的
  中间状态。

`commit_steering_consumptions_if_owned()` 本身是幂等并按 `(lease_owner, attempt)` fenced：
只有真正发生 `pending`/`delivered` → `consumed` 状态转换的事件才会生成一条
`run.steer.consumed`；worker 因网络/DB 瞬时故障重试同一批 consumption 时，不会产生第二条
重复 lifecycle 事件。

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
    accepted = client.runs.steer(
        run["id"],
        payload={"message": "switch to Spanish"},
        idempotency_key="msg-123",
    )
    print(accepted["id"], accepted["status"])
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

## Agent Skills Python API

核心公开 `SkillMetadata`、`SkillSpec`、`SkillResource`、`SkillSource`、
`FilesystemSkillSource`、`SkillRegistry` 和 `validate_skill()`。启用 `skills=` 后，ReAct Agent
增加 `read_skill(skill_name)` 与 `read_skill_resource(skill_name, path)` 两个标准 Tool Calling
能力；它们不是 REST 管理端点，也不会自动执行 `scripts/`。
