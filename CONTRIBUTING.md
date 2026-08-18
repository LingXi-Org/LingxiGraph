# 贡献指南

感谢你参与 LingxiGraph。LingxiGraph 是面向生产环境的 Agent 图运行时；贡献代码时，请优先保证运行语义、持久化兼容性、可恢复性和公共 API 的稳定性。

## 开发前必读

- 阅读 [`README.md`](README.md) 了解 Runtime、Agent Server、Worker、Studio 和持久化组件的职责。
- 涉及公开行为、持久化、事件模型、Agent Server 或 Skills 时，先查阅 [`Wiki/`](Wiki/) 中对应文档。
- 涉及已发布行为变化时，检查 [`CHANGELOG.md`](CHANGELOG.md)，避免无意破坏现有兼容性。

## 项目边界

LingxiGraph 负责状态图执行、checkpoint、interrupt/resume、retry、streaming events、持久化、工具与 Skill Runtime 等通用能力。

贡献代码时请保持以下边界：

- Runtime 不应包含具体产品、学科、业务流程或用户界面的领域规则。
- Provider、Tool、Skill 和上层应用逻辑应通过公开扩展点接入，不要把特定厂商行为写死到核心图执行路径。
- Python SDK、Agent Server、Worker 和持久化实现应尽量共享同一组运行语义；不要让同一个图因入口不同而产生不必要的行为差异。
- 事件、checkpoint 和 thread 数据属于可恢复执行协议的一部分。修改字段或顺序语义时必须考虑旧数据、重放和恢复兼容性。

## 开发环境

LingxiGraph 支持 Python 3.11、3.12 和 3.13。CI 会覆盖多个 Python 版本；本地至少使用其中一个受支持版本开发。

推荐使用仓库锁定的开发依赖：

```bash
python -m pip install --upgrade pip
python -m pip install --require-hashes -r requirements-dev.lock
python -m pip install --no-deps -e .
```

依赖变更必须同步维护锁定文件。不要只修改 `pyproject.toml` 而留下不可复现的开发环境。

## 核心实现约束

修改 Runtime、Graph、Checkpoint、Interrupt、Retry 或事件系统时：

- 相同输入、相同 checkpoint 和相同恢复点应保持可解释、可测试的执行语义。
- interrupt/resume 不得静默丢失状态、重复执行已经确认不可重复的副作用，或绕过既有恢复协议。
- retry 应区分可重试错误与确定性失败；不要用无限重试掩盖逻辑错误。
- streaming event 的新增应保持向后兼容；删除、重命名或改变现有事件含义属于高风险修改。
- 持久化实现不得依赖仅存在于单进程内存中的状态作为事实来源。
- 涉及 PostgreSQL、Redis 或迁移时，应同时验证无外部服务的基础路径和相应集成路径。

修改 Tool、MCP、A2A、Provider 或 Skill 能力时：

- 不绕过 `ToolSpec.permissions`、授权回调、HITL、timeout、预算或其他宿主策略。
- Skill 资源访问必须保持路径隔离，不允许绝对路径、`..` 逃逸或通过 symlink/特殊文件越界读取。
- `allowed-tools` 等 Skill 元数据只能表达能力提示，不能提升 Runtime 权限。
- Provider-specific 代码应封装在对应适配层，不要污染核心 Runtime 的公共抽象。

## 公共 API 与兼容性

以下改动需要特别谨慎：

- `lingxigraph` 顶层导出的类、函数、常量和协议；
- StateGraph 的构图、编译和 invoke/stream 行为；
- checkpoint/thread/event 的持久化格式；
- Agent Server REST/SSE 契约；
- CLI 命令和 `lingxigraph.json` 配置格式；
- Skill discovery、load 和 resource access 行为。

如果必须改变公开行为，应提供测试、迁移或兼容层，并同步文档和 Changelog。不要通过悄悄改变默认值来引入破坏性行为。

## 提交前检查

核心代码至少运行：

```bash
ruff check src tests examples/react_agent_skills.py skills/hello/scripts/hello.py
mypy --ignore-missing-imports src/lingxigraph
python -m unittest discover -s tests -v
python -m build
```

覆盖率不得低于仓库当前门槛：

```bash
coverage run --branch -m unittest discover -s tests -v
coverage report --fail-under=79
```

涉及 PostgreSQL / Redis 持久化时应运行对应集成测试。涉及依赖、安全或镜像构建时，应确保供应链检查能够通过，包括依赖审计和容器漏洞检查。

涉及 `adapters/chainlit` 时，请在该目录使用锁定环境并运行 Ruff、Mypy、pytest/coverage 和 build；不要只验证核心包。

## 测试要求

- 修复缺陷时优先加入能够复现原问题的回归测试。
- 新增 Runtime 语义时覆盖正常执行、失败、重试、恢复和边界条件中与改动相关的路径。
- 持久化相关修改应验证进程重启或重新加载后的行为，而不只是单次内存执行。
- 并发、取消、interrupt、resume 等修改应避免依赖时间碰巧正确的脆弱测试。
- 不要为了通过 CI 删除有效断言、降低覆盖率门槛或忽略真实类型错误。

## 文档与示例

公共 API、CLI、配置、事件或运行语义变化应同步更新 `Wiki/` 或相关示例。示例代码应使用公开 API，不要依赖测试辅助函数或内部私有实现。

如改动影响发布版本的用户可观察行为，请同时维护 `CHANGELOG.md`。

## 分支、提交与 Pull Request

- 不直接向 `main` 提交功能代码；从最新 `main` 创建独立分支并通过 Pull Request 合并。
- 一个 PR 应保持单一职责，避免将无关重构、格式化、依赖升级和行为修改混在一起。
- 提交信息建议使用清晰的 Conventional Commit 风格，例如 `feat:`、`fix:`、`docs:`、`test:`、`refactor:`、`chore:`。
- PR 描述应明确运行语义是否变化、公开 API 是否变化、是否涉及持久化/迁移，以及执行过哪些验证。
- 合并前应确保 required checks 通过，并处理仍然有效的 review conversation。

对于会改变核心执行模型、持久化协议、公共 API 或安全边界的大型改动，建议先通过 Issue 说明设计目标、兼容性和迁移方式，再开始实现。
