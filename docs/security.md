# 安全与多租户

## 身份与授权

Agent Server 验证 OIDC issuer、audience、JWKS 签名以及 token 的 `exp`、`iat`、`sub`。
tenant 仅从配置的 claim 映射。角色固定为：

- `viewer`：读取 graph、assistant、thread、run、state、event 和指标。
- `operator`：创建 thread/run、resume/cancel、store 和 schedule 操作。
- `developer`：管理 assistant 与受信 graph 配置。
- `tenant-admin`：本 tenant 内所有权限。

API 层每次资源查询都带 tenant 条件；PostgreSQL 连接通过 transaction-local
`app.tenant_id` 激活 RLS policy。生产 API 数据库角色不得拥有 `BYPASSRLS`、superuser 或表
owner 权限。跨 tenant 的 Worker 服务角色可单独授予受控 `BYPASSRLS`，且不能暴露给 API。

## 数据分类

默认审计只记录 actor、action、resource、result、trace ID 和非敏感 metadata，不记录
prompt、完整 state、模型消息或凭据。日志/遥测输出前递归脱敏 authorization、token、secret、
password、api_key 和 cookie 字段。

远程 A2A/MCP 凭据使用 `secret_ref` 及部署时 secret resolver。引用可以出现在节点配置；
解析后的 secret 不得进入 state、checkpoint、event、cache key 或 trace attribute。

## 可信执行边界

当前版本不提供在线 Python 上传、多租户代码沙箱或微虚机。Worker 只执行镜像/签名制品中的
`lingxigraph.json` 导入路径。因此：

- 图变更与普通生产代码执行相同的 review、CI、SBOM、签名和发布流程。
- 不允许 tenant 提交 import path、pickle、Python bytecode 或任意 callable。
- MCP/A2A 远程结果仍须经过 JSON serializer 和状态 schema 校验。

如需运行不可信代码，必须在 LingxiGraph 之外使用独立沙箱、最小权限身份和网络出口策略。

## PostgreSQL 加固

- TLS、磁盘加密、PITR、独立 API/Worker/迁移角色和最小 schema grants。
- tenant 业务表启用 RLS；CI 使用非 owner API 角色执行越权负面测试。
- API role 只能调用 tenant 范围 CRUD；Worker role 才能执行跨 tenant `SKIP LOCKED` claim。
- 限制连接池、statement timeout、idle transaction timeout 和 state/event 最大尺寸。

## 网络与容器

- API 只通过 TLS ingress 暴露；PostgreSQL、Redis、OTLP 和远程 Agent 使用 NetworkPolicy。
- 容器以 UID/GID 10001 运行，禁用提权、丢弃 Linux capabilities、只读 rootfs、RuntimeDefault
  seccomp；ServiceAccount 不挂载不需要的 Kubernetes token。
- 镜像使用不可变 digest，依赖有上限并在发布流水线执行漏洞扫描和 CycloneDX SBOM。

## 配额与滥用防护

按 tenant 限制 active/queued runs、SSE、请求速率、state 大小和长期存储。超限返回稳定
429 problem code。节点层同时设置超时、并发上限、递归限制和最大重试次数，避免图循环或
故障下游放大资源消耗。

工具按最小 capability 配置 `permissions`，并在 run config 中仅授予本次工作所需权限；高风险
调用设置 `requires_approval=True`，需要资源级判断时使用 `tool_authorize`。凭据参数通过
`secret_refs`/resolver 注入，模型生成的参数不能覆盖 secret。模型调用、工具调用、token 和
成本预算应视为租户配额的第二道边界。

API 对 `Idempotency-Key` 做 tenant 级唯一约束和请求摘要冲突检测，避免客户端重试重复创建
run。provider adapter 在每次逻辑操作内复用下游幂等 key；业务副作用仍必须在最终服务端实现
去重，因为 checkpoint 提交前崩溃采用至少一次交付。
