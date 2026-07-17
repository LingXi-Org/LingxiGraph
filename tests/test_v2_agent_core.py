import asyncio
import sqlite3
import time
import unittest
from collections.abc import AsyncIterator
from typing import Annotated, TypedDict

from lingxigraph import (
    END,
    START,
    AIMessage,
    AIMessageChunk,
    Command,
    CommandScope,
    HumanMessage,
    InMemorySaver,
    JsonSerializer,
    SqliteSaver,
    StateGraph,
    ToolCall,
    ToolMessage,
    add_messages,
    create_agent,
    entrypoint,
    tool,
)


class V2AgentCoreTests(unittest.TestCase):
    def test_messages_upsert_remove_and_serializer_v2(self) -> None:
        first = HumanMessage("old", id="m1")
        merged = add_messages([first], [HumanMessage("new", id="m1"), AIMessage("ok")])
        self.assertEqual([item.content for item in merged], ["new", "ok"])
        serializer = JsonSerializer()
        restored = serializer.loads(serializer.dumps({"messages": merged}))
        self.assertEqual(restored, {"messages": merged})
        self.assertEqual(serializer.version, 2)

    def test_tool_node_via_create_agent(self) -> None:
        @tool
        def multiply(a: int, b: int) -> int:
            """Multiply two integers."""
            return a * b

        class Model:
            def __init__(self) -> None:
                self.calls = 0

            async def agenerate(self, messages, *, tools=None, **kwargs):
                del kwargs
                self.calls += 1
                self.assert_tools = tools
                if self.calls == 1:
                    return AIMessage(
                        "",
                        tool_calls=(ToolCall("multiply", {"a": 6, "b": 7}, "call-1"),),
                    )
                self.last_messages = messages
                return AIMessage("42")

        model = Model()
        graph = create_agent(model, [multiply])
        result = graph.invoke({"messages": [HumanMessage("calculate")]})
        self.assertEqual(result["messages"][-1].content, "42")
        self.assertTrue(any(isinstance(item, ToolMessage) for item in model.last_messages))
        self.assertEqual(multiply.parameters["properties"]["a"], {"type": "integer"})

    def test_output_schema_projection(self) -> None:
        class State(TypedDict):
            public: int
            private: str

        class Output(TypedDict):
            public: int

        builder = StateGraph(State, output_schema=Output)
        builder.add_node("work", lambda state: {"public": 2, "private": "secret"})
        builder.add_edge(START, "work").add_edge("work", END)
        self.assertEqual(
            builder.compile().invoke({"public": 0, "private": "seed"}),
            {"public": 2},
        )

    def test_parent_command_handoff(self) -> None:
        class State(TypedDict):
            trace: Annotated[list[str], list.__add__]

        child = StateGraph(State)
        child.add_node(
            "handoff",
            lambda state: Command(
                update={"trace": ["child"]},
                goto="finish",
                scope=CommandScope.PARENT,
            ),
        )
        child.add_edge(START, "handoff").add_edge("handoff", END)

        parent = StateGraph(State)
        parent.add_node("team", child.compile(), destinations=["finish"])
        parent.add_node("finish", lambda state: {"trace": ["parent"]})
        parent.add_edge(START, "team").add_edge("team", END).add_edge("finish", END)
        self.assertEqual(parent.compile().invoke({"trace": []})["trace"], ["child", "parent"])

    def test_event_sequences_are_monotonic(self) -> None:
        class State(TypedDict):
            value: int

        builder = StateGraph(State)
        builder.add_node("node", lambda state: {"value": state["value"] + 1})
        builder.add_edge(START, "node").add_edge("node", END)
        events = list(builder.compile().stream({"value": 0}, stream_mode="events"))
        self.assertEqual([event.sequence for event in events], list(range(1, len(events) + 1)))

    def test_async_only_checkpointer(self) -> None:
        class AsyncOnly:
            def __init__(self) -> None:
                self.inner = InMemorySaver()

            async def aget_tuple(self, config):
                return self.inner.get_tuple(config)

            async def aput(self, config, checkpoint, metadata):
                return self.inner.put(config, checkpoint, metadata)

            async def aput_writes(self, config, checkpoint_id, writes):
                self.inner.put_writes(config, checkpoint_id, writes)

            async def aget_writes(self, config, checkpoint_id):
                return self.inner.get_writes(config, checkpoint_id)

            async def alist(self, config) -> AsyncIterator:
                for item in self.inner.list(config):
                    yield item

        class State(TypedDict):
            value: int

        async def run() -> None:
            saver = AsyncOnly()
            builder = StateGraph(State)
            builder.add_node("node", lambda state: {"value": state["value"] + 1})
            builder.add_edge(START, "node").add_edge("node", END)
            graph = builder.compile(checkpointer=saver)
            config = {"configurable": {"thread_id": "async-only"}}
            self.assertEqual((await graph.ainvoke({"value": 0}, config))["value"], 1)
            self.assertEqual((await graph.aget_state(config)).values["value"], 1)

        asyncio.run(run())

    def test_stream_mode_list_and_live_custom_emission(self) -> None:
        class State(TypedDict):
            value: int

        async def work(state, runtime):
            runtime.emit("progress", {"percent": 50})
            await asyncio.sleep(0.05)
            return {"value": state["value"] + 1}

        builder = StateGraph(State)
        builder.add_node("work", work)
        builder.add_edge(START, "work").add_edge("work", END)
        graph = builder.compile()

        async def run() -> None:
            iterator = graph.astream({"value": 0}, stream_mode="custom")
            started = time.monotonic()
            first = await anext(iterator)
            self.assertLess(time.monotonic() - started, 0.045)
            self.assertEqual(first, {"progress": {"percent": 50}})
            await iterator.aclose()
            combined = [
                item
                async for item in graph.astream(
                    {"value": 0}, stream_mode=["values", "events"]
                )
            ]
            self.assertIn(("values", {"value": 1}), combined)
            self.assertTrue(any(mode == "events" for mode, _ in combined))

        asyncio.run(run())

    def test_graph_structure_and_mermaid(self) -> None:
        class State(TypedDict):
            value: int

        builder = StateGraph(State)
        builder.add_node("work", lambda state: {"value": 1})
        builder.add_edge(START, "work").add_edge("work", END)
        graph = builder.compile()
        self.assertEqual([node.id for node in graph.get_graph().nodes], [START, "work", END])
        self.assertIn("flowchart TD", graph.draw_mermaid())

    def test_sqlite_v1_writes_migrate_and_namespace_isolate(self) -> None:
        connection = sqlite3.connect(":memory:", check_same_thread=False)
        connection.execute(
            """CREATE TABLE checkpoint_writes_v1 (
                thread_id TEXT NOT NULL, checkpoint_id TEXT NOT NULL,
                task_id TEXT NOT NULL, write_index INTEGER NOT NULL,
                write_json BLOB NOT NULL,
                PRIMARY KEY (thread_id, checkpoint_id, task_id, write_index))"""
        )
        saver = SqliteSaver(connection)
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        self.assertIn("checkpoint_writes_v2", tables)
        self.assertNotIn("checkpoint_writes_v1", tables)
        saver.close()

    def test_subgraph_stream_namespace_and_deferred_node(self) -> None:
        class State(TypedDict):
            value: int
            trace: Annotated[list[str], list.__add__]

        child_builder = StateGraph(State)
        child_builder.add_node(
            "child_work", lambda state: {"value": state["value"] + 1, "trace": ["child"]}
        )
        child_builder.add_edge(START, "child_work").add_edge("child_work", END)

        parent = StateGraph(State)
        parent.add_node("team", child_builder.compile())
        parent.add_node("normal", lambda state: {"trace": ["normal"]})
        parent.add_node("cleanup", lambda state: {"trace": ["cleanup"]}, defer=True)
        parent.add_edge(START, "team")
        parent.add_edge("team", "normal").add_edge("team", "cleanup")
        parent.add_edge("normal", END).add_edge("cleanup", END)
        graph = parent.compile()
        chunks = list(
            graph.stream({"value": 0, "trace": []}, stream_mode="values", subgraphs=True)
        )
        self.assertTrue(any(isinstance(item, tuple) and item[0] == ("team",) for item in chunks))
        self.assertEqual(chunks[-1]["trace"], ["child", "normal", "cleanup"])

    def test_functional_entrypoint(self) -> None:
        class State(TypedDict):
            value: int

        @entrypoint(State)
        def double(state):
            return {"value": state["value"] * 2}

        self.assertEqual(double.invoke({"value": 3}), {"value": 6})

    def test_streaming_agent_structured_response_and_hitl(self) -> None:
        class StreamingModel:
            async def astream(self, messages, *, tools=None, **kwargs):
                del messages, tools, kwargs
                yield AIMessageChunk("stream")
                yield AIMessageChunk("ed")

            async def agenerate(self, messages, *, tools=None, **kwargs):
                del messages, tools, kwargs
                return AIMessage('{"answer": 42}')

        graph = create_agent(StreamingModel(), response_format={"type": "object"})
        streamed = list(
            graph.stream({"messages": [HumanMessage("answer")]}, stream_mode="messages")
        )
        self.assertEqual("".join(item[0].content for item in streamed), "streamed")
        result = graph.invoke({"messages": [HumanMessage("answer")]})
        self.assertEqual(result["structured_response"], {"answer": 42})

        executions = []

        @tool
        def sensitive(value: str) -> str:
            """Perform a sensitive operation."""
            executions.append(value)
            return "approved"

        class ApprovalModel:
            def __init__(self):
                self.calls = 0

            async def agenerate(self, messages, *, tools=None, **kwargs):
                del messages, tools, kwargs
                self.calls += 1
                if self.calls == 1:
                    return AIMessage(
                        "",
                        tool_calls=(ToolCall("sensitive", {"value": "x"}, "approval"),),
                    )
                return AIMessage("finished")

        saver = InMemorySaver()
        approval_graph = create_agent(
            ApprovalModel(),
            [sensitive],
            interrupt_on=["sensitive"],
            checkpointer=saver,
        )
        config = {"configurable": {"thread_id": "approval"}}
        paused = approval_graph.invoke({"messages": [HumanMessage("run")]}, config)
        self.assertIn("__interrupt__", paused)
        resumed = approval_graph.invoke(Command(resume={"action": "approve"}), config)
        self.assertEqual(resumed["messages"][-1].content, "finished")
        self.assertEqual(executions, ["x"])

        reject_model = ApprovalModel()
        reject_graph = create_agent(
            reject_model,
            [sensitive],
            interrupt_on={"sensitive": True},
            checkpointer=InMemorySaver(),
        )
        reject_config = {"configurable": {"thread_id": "reject"}}
        reject_graph.invoke({"messages": [HumanMessage("run")]}, reject_config)
        rejected = reject_graph.invoke(
            Command(resume={"action": "reject", "message": "not allowed"}),
            reject_config,
        )
        self.assertEqual(rejected["messages"][-1].content, "finished")
        self.assertEqual(executions, ["x"])

        edit_model = ApprovalModel()
        edit_graph = create_agent(
            edit_model,
            [sensitive],
            interrupt_on=["sensitive"],
            checkpointer=InMemorySaver(),
        )
        edit_config = {"configurable": {"thread_id": "edit"}}
        edit_graph.invoke({"messages": [HumanMessage("run")]}, edit_config)
        edited = edit_graph.invoke(
            Command(
                resume={
                    "action": "edit",
                    "tool_calls": [
                        {"id": "approval", "name": "sensitive", "args": {"value": "edited"}}
                    ],
                }
            ),
            edit_config,
        )
        self.assertEqual(edited["messages"][-1].content, "finished")
        self.assertEqual(executions, ["x", "edited"])


if __name__ == "__main__":
    unittest.main()
