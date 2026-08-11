"""Offline ReAct example that progressively loads the bundled hello Skill."""

from __future__ import annotations

from pathlib import Path

from lingxigraph import AIMessage, HumanMessage, ToolCall, ToolMessage, create_agent


class SkillAwareModel:
    """A deterministic model stub that demonstrates the Agent Skills tool loop."""

    async def agenerate(self, messages, *, tools=None, **kwargs):
        del tools, kwargs
        results = [message for message in messages if isinstance(message, ToolMessage)]
        if not results:
            return AIMessage(
                "",
                tool_calls=(ToolCall("read_skill", {"skill_name": "hello"}, "skill-1"),),
            )
        if len(results) == 1:
            return AIMessage(
                "",
                tool_calls=(
                    ToolCall(
                        "read_skill_resource",
                        {"skill_name": "hello", "path": "references/greetings.md"},
                        "resource-1",
                    ),
                ),
            )
        return AIMessage("你好，欢迎使用 LingxiGraph Agent Skills！")


skills_root = Path(__file__).resolve().parents[1] / "skills"
agent = create_agent(SkillAwareModel(), skills=skills_root)
result = agent.invoke({"messages": [HumanMessage("请用中文问候我")]})
print(result["messages"][-1].content)
