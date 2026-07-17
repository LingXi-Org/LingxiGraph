"""Provider-neutral reusable multi-agent graph patterns."""

from .multi_agent import (
    AgentTool,
    build_group_chat,
    build_handoff,
    build_manager_as_tools,
    build_parallel_review,
    build_plan_execute,
    build_supervisor,
    build_swarm,
    create_handoff_tool,
)

__all__ = [
    "AgentTool",
    "build_group_chat",
    "build_handoff",
    "build_manager_as_tools",
    "build_parallel_review",
    "build_plan_execute",
    "build_supervisor",
    "build_swarm",
    "create_handoff_tool",
]
