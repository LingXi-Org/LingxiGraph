import asyncio
import operator
import time
import unittest
from typing import Annotated, TypedDict

from lingxigraph import (
    END,
    START,
    Command,
    GraphRecursionError,
    StateGraph,
)


class State(TypedDict, total=False):
    value: int
    route: str
    trace: Annotated[list[str], operator.add]


class ExecutionTests(unittest.TestCase):
    def test_sequential_chain(self) -> None:
        builder = StateGraph(State)
        builder.add_node("one", lambda state: {"value": state.get("value", 0) + 1})
        builder.add_node("two", lambda state: {"value": state["value"] * 2})
        builder.add_edge(START, "one").add_edge("one", "two").add_edge("two", END)
        self.assertEqual(builder.compile().invoke({"value": 2})["value"], 6)

    def test_conditional_edges_select_one_branch(self) -> None:
        builder = StateGraph(State)
        builder.add_node("choose", lambda state: {})
        builder.add_node("left", lambda state: {"trace": ["left"]})
        builder.add_node("right", lambda state: {"trace": ["right"]})
        builder.add_edge(START, "choose")
        builder.add_conditional_edges(
            "choose", lambda state: state["route"], {"l": "left", "r": "right"}
        )
        builder.add_edge("left", END).add_edge("right", END)
        result = builder.compile().invoke({"route": "r", "trace": []})
        self.assertEqual(result["trace"], ["right"])

    def test_parallel_fan_in_is_deterministic(self) -> None:
        builder = StateGraph(State)

        def slow_a(state):
            time.sleep(0.02)
            return {"trace": ["a"]}

        def fast_b(state):
            return {"trace": ["b"]}

        builder.add_node("a", slow_a).add_node("b", fast_b)
        builder.add_node("join", lambda state: {"trace": ["join:" + "".join(state["trace"])]})
        builder.add_edge(START, "a").add_edge(START, "b")
        builder.add_edge(["a", "b"], "join").add_edge("join", END)
        result = builder.compile().invoke({"trace": []})
        self.assertEqual(result["trace"], ["a", "b", "join:ab"])

    def test_command_applies_update_and_goto(self) -> None:
        builder = StateGraph(State)
        builder.add_node(
            "first", lambda state: Command(update={"trace": ["first"]}, goto="second")
        )
        builder.add_node("second", lambda state: {"trace": ["second"]})
        builder.add_edge(START, "first").add_edge("first", "second").add_edge("second", END)
        result = builder.compile().invoke({"trace": []})
        self.assertEqual(result["trace"], ["first", "second"])

    def test_recursion_limit(self) -> None:
        builder = StateGraph(State)
        builder.add_node("loop", lambda state: {"trace": ["tick"]})
        builder.add_edge(START, "loop").add_edge("loop", "loop")
        graph = builder.compile()
        with self.assertRaises(GraphRecursionError):
            graph.invoke({"trace": []}, {"recursion_limit": 3})

    def test_sync_and_async_nodes_mix(self) -> None:
        builder = StateGraph(State)
        builder.add_node("sync", lambda state: {"trace": ["sync"]})

        async def async_node(state):
            await asyncio.sleep(0)
            return {"trace": ["async"]}

        builder.add_node("async", async_node)
        builder.add_edge(START, "sync").add_edge("sync", "async").add_edge("async", END)
        result = asyncio.run(builder.compile().ainvoke({"trace": []}))
        self.assertEqual(result["trace"], ["sync", "async"])


if __name__ == "__main__":
    unittest.main()
