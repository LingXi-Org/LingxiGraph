# ADR-0001：耐久多智能体图平台

- 状态：Accepted
- 日期：2026-07-17
- 版本：1.0.0

## 背景

企业级多智能体运行平台需要同时覆盖确定性图执行、崩溃恢复、分布式队列、租约、
多租户控制面和标准网络协议，并保持嵌入式运行时不绑定模型供应商。

## 决策

采用“嵌入式 SDK + Agent Server + PostgreSQL 队列/状态 + 分布式 Worker + Redis 可选
加速 + 协议适配器”的分层架构。

1. 继续使用 plan/execute/commit 超步；状态按计划顺序确定性归并。
2. 每个成功任务先写 pending writes；checkpoint 事务提交后形成新 lineage。
3. PostgreSQL 是唯一恢复真相，队列使用 `SKIP LOCKED`、lease、heartbeat 和唯一索引。
4. Redis 不承载业务真相，故障时退化到 PostgreSQL polling。
5. 生产 serializer 仅接受版本化 JSON typed values，不执行 pickle。
6. graph code 是随镜像发布的可信制品；v1 不执行 tenant 上传代码。
7. OpenAPI 是 REST/SDK 真相；事件先持久化再 SSE，A2A/MCP 位于适配层。
8. tenant 在 JWT、API 查询和 PostgreSQL RLS 三处保持同一安全边界。

## 参考语义

持久化和 Agent Server 资源模型参考 LangGraph；manager/handoff 参考 OpenAI Agents；并行、
group chat 与 plan-execute 参考 Microsoft Agent Framework 和 AutoGen GraphFlow；动态/静态
Agent 组合参考 Google ADK。LingxiGraph 不复制模型/provider SDK，只吸收可验证的编排语义。

## 后果

优点：进程崩溃恢复、同线程串行化、水平扩展、事件续传、多租户审计和 provider-neutral
模板成为平台内建能力。代价：生产部署必须运营 PostgreSQL、身份系统与迁移；外部副作用
仍需使用稳定幂等键；大规模多区域队列分片和不可信代码沙箱延期到后续版本。
