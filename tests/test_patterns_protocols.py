import asyncio
import operator
import unittest
from typing import Annotated, TypedDict
from unittest.mock import patch

from lingxigraph import END, START, Command, Runtime, StateGraph
from lingxigraph.patterns import (
    build_group_chat,
    build_handoff,
    build_manager_as_tools,
    build_parallel_review,
    build_plan_execute,
    build_supervisor,
    build_swarm,
)
from lingxigraph.protocols import MCPToolset, RemoteAgentNode


class PatternState(TypedDict):
    result: str


class PatternTests(unittest.TestCase):
    def test_supervisor_and_handoff_compile(self) -> None:
        def supervisor(_state):
            return Command(goto=END)

        self.assertEqual(
            build_supervisor(PatternState, supervisor, {"worker": lambda _s: {}})
            .compile()
            .invoke({"result": ""})["result"],
            "",
        )
        graph = build_handoff(
            PatternState,
            {"worker": lambda _s: Command(update={"result": "done"}, goto=END)},
            entry="worker",
        ).compile()
        self.assertEqual(graph.invoke({"result": ""})["result"], "done")

    def test_manager_as_tools_invokes_plain_callable(self) -> None:
        async def manager(state, runtime: Runtime, tools):
            return await tools["specialist"](state, runtime)

        graph = build_manager_as_tools(
            PatternState,
            manager,
            {"specialist": lambda _state: {"result": "tool-result"}},
        ).compile()
        self.assertEqual(graph.invoke({"result": ""})["result"], "tool-result")

    def test_swarm_group_chat_and_plan_execute(self) -> None:
        swarm = build_swarm(
            PatternState,
            {"peer": lambda _state: Command(update={"result": "swarm"}, goto=END)},
            entry="peer",
        ).compile()
        self.assertEqual(swarm.invoke({"result": ""})["result"], "swarm")

        class ChatState(TypedDict):
            active_agent: str
            turn: int
            result: str

        chat = build_group_chat(
            ChatState,
            {
                "a": lambda _state: {"result": "a"},
                "b": lambda _state: {"result": "b"},
            },
            termination=lambda state: state["turn"] >= 1,
        ).compile()
        chat_result = chat.invoke({"active_agent": "a", "turn": 0, "result": ""})
        self.assertEqual(chat_result["result"], "b")

        planned = build_plan_execute(
            PatternState,
            lambda _state: {"result": "planned"},
            lambda _state: {"result": "executed"},
            lambda _state: Command(goto=END),
        ).compile()
        self.assertEqual(planned.invoke({"result": ""})["result"], "executed")

    def test_parallel_review_map_reduce(self) -> None:
        class ReviewState(TypedDict):
            reviews: Annotated[list[str], operator.add]
            result: str

        graph = build_parallel_review(
            ReviewState,
            lambda _state: {},
            {
                "security": lambda _state: {"reviews": ["security"]},
                "quality": lambda _state: {"reviews": ["quality"]},
            },
            lambda state: {"result": ",".join(state["reviews"])},
        ).compile()
        result = graph.invoke({"reviews": [], "result": ""})
        self.assertEqual(result["reviews"], ["security", "quality"])
        self.assertEqual(result["result"], "security,quality")

    def test_remote_a2a_node_and_mcp_toolset(self) -> None:
        class Response:
            def __init__(self, value):
                self.value = value

            def raise_for_status(self):
                return None

            def json(self):
                return self.value

        class Client:
            responses = []
            requests = []

            def __init__(self, *args, **kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

            async def post(self, url, **kwargs):
                self.requests.append((url, kwargs))
                return Response(self.responses.pop(0))

        class RemoteState(TypedDict):
            request: str
            remote_agent: dict

        remote = RemoteAgentNode(
            "https://remote.example/a2a",
            secret_ref="remote-token",
            secret_resolver=lambda reference: f"resolved-{reference}",
            poll_interval=0,
        )
        builder = StateGraph(RemoteState)
        builder.add_node("remote", remote)
        builder.add_edge(START, "remote")
        builder.add_edge("remote", END)
        Client.responses = [
            {"result": {"id": "task", "status": "running"}},
            {"result": {"id": "task", "status": "succeeded", "output": "done"}},
        ]
        with patch("httpx.AsyncClient", Client):
            result = builder.compile().invoke({"request": "work", "remote_agent": {}})
        self.assertEqual(result["remote_agent"]["status"], "succeeded")
        self.assertEqual(
            Client.requests[0][1]["headers"]["authorization"],
            "Bearer resolved-remote-token",
        )

        Client.responses = [
            {"result": {"tools": [{"name": "search"}]}},
            {"result": {"content": [{"type": "text", "text": "found"}]}},
        ]
        toolset = MCPToolset(
            "https://tools.example/mcp",
            secret_ref="mcp-token",
            secret_resolver=lambda _reference: "secret",
        )

        class ToolState(TypedDict):
            query: str
            tool_result: dict

        async def use_tools():
            tools = await toolset.list_tools()
            node = toolset.node("search", output_key="tool_result")
            graph_builder = StateGraph(ToolState)
            graph_builder.add_node("tool", node)
            graph_builder.add_edge(START, "tool")
            graph_builder.add_edge("tool", END)
            result = await graph_builder.compile().ainvoke({"query": "x", "tool_result": {}})
            return tools, result

        with patch("httpx.AsyncClient", Client):
            tools, result = asyncio.run(use_tools())
        self.assertEqual(tools, [{"name": "search"}])
        self.assertEqual(result["tool_result"]["content"][0]["text"], "found")


if __name__ == "__main__":
    unittest.main()
