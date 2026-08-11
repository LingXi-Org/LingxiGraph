# Migrating to LingxiGraph 2.1.0 / 迁移到 LingxiGraph 2.1.0

## English

LingxiGraph 2.1.0 is backward compatible with 2.0.x. Existing `create_agent(model, tools)` and
`create_react_agent` calls do not need to change. To opt in, pass a Skill root, source, or registry:

```python
agent = create_agent(model, tools=tools, skills="skills")
```

Use the standard Agent Skills `SKILL.md` frontmatter. Do not convert Skills to a LingxiGraph-specific
manifest. Invalid or duplicate configured Skills now fail agent construction. Resource paths must be
relative to `references/`, `scripts/`, or `assets/`; links and path traversal are rejected.

`allowed-tools` does not grant permissions. Continue to define executable capabilities as
`ToolSpec` values and grant their required permissions in run configuration. Script files are never
executed automatically.

## 中文

LingxiGraph 2.1.0 与 2.0.x 向后兼容。现有 `create_agent(model, tools)` 与
`create_react_agent` 调用无需修改。需要 Agent Skills 时传入目录、source 或 registry：

```python
agent = create_agent(model, tools=tools, skills="skills")
```

继续使用标准 Agent Skills `SKILL.md` frontmatter，不要转换为 LingxiGraph 私有清单。显式配置
的无效或重名 Skill 会使 Agent 构建失败。资源路径必须位于 `references/`、`scripts/` 或
`assets/`，symlink 与路径穿越会被拒绝。

`allowed-tools` 不授予权限。可执行能力仍应声明为 `ToolSpec`，并在 run config 中授予所需权限；
Skill 中的脚本不会自动执行。
