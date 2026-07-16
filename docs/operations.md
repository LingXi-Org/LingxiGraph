# 生产运维手册

## 依赖与拓扑

- Python 3.11+；PostgreSQL 15+；Redis 7.2+；Kubernetes 1.27+。
- Agent Server 无状态水平扩展；Worker 按 PostgreSQL 队列深度扩展。
- PostgreSQL 必须使用高可用、PITR 和加密备份。Redis 不承载恢复所需的唯一数据。
- API 与 Worker 镜像必须包含相同 `lingxigraph.json` 和可信图版本。

## 上线顺序

1. 构建不可变镜像并生成 SBOM、漏洞扫描和签名。
2. 运行 `lingxigraph doctor`；校验图导入、JSON Schema、OIDC 和数据库配置。
3. 独立 Job 执行 `lingxigraph migrate` 或 `alembic upgrade head`。
4. 先滚动 Worker，再滚动 Agent Server；保持至少一个可用实例。
5. 创建 canary run，检查 run、task、checkpoint spans 和 SSE 顺序。

Chart 的 API initContainer 默认也会执行幂等迁移。严格变更窗口可移除 initContainer，改用
单独的迁移 Job。

## 关键环境变量

| 名称 | 用途 |
| --- | --- |
| `LINGXIGRAPH_POSTGRES_URL` | Agent Server/Worker 主数据库 DSN |
| `LINGXIGRAPH_REDIS_URL` | 可选 PubSub/cache/cancel 加速 |
| `LINGXIGRAPH_OIDC_ISSUER` | JWT issuer |
| `LINGXIGRAPH_OIDC_AUDIENCE` | JWT audience |
| `LINGXIGRAPH_OIDC_JWKS_URL` | 可信 JWKS endpoint |
| `LINGXIGRAPH_TENANT_CLAIM` | tenant claim，默认 `tenant_id` |
| `LINGXIGRAPH_ROLES_CLAIM` | roles claim，默认 `roles` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP collector |

生产环境不得设置 `LINGXIGRAPH_INSECURE_DEV_AUTH=true`，也不得把数据库、OIDC 或远程 Agent
凭据写入图 state、context、事件或 ConfigMap。

## 扩缩容与 SLO

建议告警：

- pending queue age p95 > 2s 或持续增长；
- run failure/timed_out rate、lease recovery、dead-letter error code 增长；
- checkpoint commit p95、PostgreSQL transaction/connection saturation；
- SSE active clients、断线率和事件端到端 p95；
- Redis errors（警告级）与 PostgreSQL errors（高优先级）。

部门级基线为 100 并发 I/O runs、1,000 排队 runs、200 SSE；典型 state ≤256 KiB 时 CRUD
p95 <250ms、启动 p95 <2s、事件 p95 <500ms。容量测试必须在目标云、数据库规格与真实网络
上重新执行，不能用开发机结果代替。

对目标环境执行门槛脚本（`--input` 是示例图的整数输入；实际图可按需调整脚本负载）：

```bash
python scripts/capacity.py --base-url https://agents.example.com \
  --graph-id production-support --concurrent-runs 100 \
  --queued-runs 1000 --sse-clients 200
```

## 故障演练

每次大版本发布至少执行：

1. 强杀正在执行并已有 pending writes 的 Worker；确认租约过期后只执行缺失任务。
2. checkpoint commit 后、Worker 确认前终止进程；确认状态不重复归并。
3. 重启 Redis；确认 run 继续、SSE 最迟在数据库轮询周期恢复、取消最终生效。
4. PostgreSQL 短暂断连；确认请求返回 retryable error、租约未产生双 active run。
5. PostgreSQL/Redis 滚动重启；核对 run 数、checkpoint lineage 和事件 sequence 无缺口。

## 备份与恢复

备份范围至少包括 `lingxigraph` schema 和 Alembic version table。恢复后先以只读方式校验：

- runs 与 checkpoints 的 thread/tenant 对应；
- `checkpoint_writes` 基准 checkpoint 可找到；
- `run_events` 的 `(tenant_id, run_id, sequence)` 唯一且递增；
- 没有两个相同 tenant/thread 的 active run。

Redis 可清空重建。不要从 Redis 恢复业务状态。

## 优雅终止

Kubernetes 先发送 SIGTERM，API 停止接收新流量；Worker 停止领取新 run，并让当前任务在
termination grace 内完成。超时强杀后由租约回收器接管。节点应响应 cancellation token，
避免依赖无限阻塞的同步 I/O。
