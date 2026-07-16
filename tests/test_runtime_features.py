import asyncio
import operator
import unittest
from typing import Annotated, TypedDict

from lingxigraph import (
    END,
    START,
    Command,
    InMemorySaver,
    StateGraph,
    get_config,
    interrupt,
)


class State(TypedDict, total=False):
    user: str
    trace: Annotated[list[str], operator.add]


class NodeConfigTests(unittest.TestCase):
    def test_two_argument_nodes_receive_the_run_config(self) -> None:
        def personalized(state, config):
            return {"user": config["configurable"]["user_id"]}

        builder = StateGraph(State).add_node("hello", personalized)
        builder.add_edge(START, "hello").add_edge("hello", END)
        result = builder.compile().invoke({}, {"configurable": {"user_id": "u-42"}})
        self.assertEqual(result["user"], "u-42")

    def test_conditional_paths_can_receive_config(self) -> None:
        def route(state, config):
            return config["configurable"]["branch"]

        builder = StateGraph(State)
        builder.add_node("start", lambda state: {})
        builder.add_node("left", lambda state: {"trace": ["left"]})
        builder.add_node("right", lambda state: {"trace": ["right"]})
        builder.add_edge(START, "start")
        builder.add_conditional_edges("start", route, {"l": "left", "r": "right"})
        builder.add_edge("left", END).add_edge("right", END)
        result = builder.compile().invoke(
            {"trace": []}, {"configurable": {"branch": "l"}}
        )
        self.assertEqual(result["trace"], ["left"])

    def test_get_config_inside_a_node(self) -> None:
        def check(state):
            return {"user": get_config()["configurable"]["thread_id"]}

        builder = StateGraph(State).add_node("check", check)
        builder.add_edge(START, "check").add_edge("check", END)
        graph = builder.compile(checkpointer=InMemorySaver())
        result = graph.invoke({}, {"configurable": {"thread_id": "cfg-thread"}})
        self.assertEqual(result["user"], "cfg-thread")


class ParallelInterruptTests(unittest.TestCase):
    def test_parallel_sibling_survives_an_interrupt(self) -> None:
        saver = InMemorySaver()
        config = {"configurable": {"thread_id": "parallel-interrupt"}}

        def ask(state):
            answer = interrupt("approve?")
            return {"trace": [f"ask={answer}"]}

        builder = StateGraph(State)
        builder.add_node("ask", ask)
        builder.add_node("work", lambda state: {"trace": ["work"]})
        builder.add_node("join", lambda state: {"trace": ["join"]})
        builder.add_edge(START, "ask").add_edge(START, "work")
        builder.add_edge(["ask", "work"], "join").add_edge("join", END)
        graph = builder.compile(checkpointer=saver)

        paused = graph.invoke({"trace": []}, config)
        self.assertEqual(paused["__interrupt__"][0].value, "approve?")
        # Both parallel tasks stay scheduled while the run is paused.
        self.assertEqual(graph.get_state(config).next, ("ask", "work"))

        result = graph.invoke(Command(resume="yes"), config)
        self.assertEqual(result["trace"], ["ask=yes", "work", "join"])

    def test_two_parallel_interrupts_resolve_sequentially(self) -> None:
        saver = InMemorySaver()
        config = {"configurable": {"thread_id": "double-interrupt"}}

        def gate(name):
            def node(state):
                answer = interrupt(f"{name}?")
                return {"trace": [f"{name}={answer}"]}

            return node

        builder = StateGraph(State)
        builder.add_node("a", gate("a")).add_node("b", gate("b"))
        builder.add_edge(START, "a").add_edge(START, "b")
        builder.add_edge(["a", "b"], "done")
        builder.add_node("done", lambda state: {"trace": ["done"]})
        builder.add_edge("done", END)
        graph = builder.compile(checkpointer=saver)

        paused = graph.invoke({"trace": []}, config)
        self.assertEqual([m.value for m in paused["__interrupt__"]], ["a?", "b?"])
        second = graph.invoke(Command(resume="one"), config)
        self.assertEqual([m.value for m in second["__interrupt__"]], ["b?"])
        result = graph.invoke(Command(resume="two"), config)
        self.assertEqual(result["trace"], ["a=one", "b=two", "done"])


class BuilderAliasTests(unittest.TestCase):
    def test_entry_and_finish_point_aliases(self) -> None:
        builder = StateGraph(State)
        builder.add_node("only", lambda state: {"trace": ["ran"]})
        builder.set_entry_point("only").set_finish_point("only")
        result = builder.compile().invoke({"trace": []})
        self.assertEqual(result["trace"], ["ran"])


class EventLoopGuardTests(unittest.TestCase):
    def test_sync_invoke_inside_running_loop_is_rejected(self) -> None:
        builder = StateGraph(State).add_node("noop", lambda state: {})
        builder.add_edge(START, "noop").add_edge("noop", END)
        graph = builder.compile()

        async def call_sync():
            graph.invoke({})

        with self.assertRaisesRegex(RuntimeError, "use ainvoke"):
            asyncio.run(call_sync())


if __name__ == "__main__":
    unittest.main()
