import operator
import unittest
from typing import Annotated, TypedDict

from lingxigraph import (
    END,
    START,
    Command,
    GraphValidationError,
    InMemorySaver,
    StateGraph,
    interrupt,
)


class ParentState(TypedDict, total=False):
    messages: Annotated[list[str], operator.add]
    approved: bool


class ChildState(TypedDict, total=False):
    messages: Annotated[list[str], operator.add]
    scratch: str


class SubgraphTests(unittest.TestCase):
    def make_child(self):
        builder = StateGraph(ChildState)
        builder.add_node("research", lambda state: {"messages": ["child: researched"]})
        builder.add_node("draft", lambda state: {"messages": ["child: drafted"]})
        builder.add_edge(START, "research").add_edge("research", "draft").add_edge("draft", END)
        return builder.compile()

    def test_subgraph_runs_as_node_and_merges_shared_keys(self) -> None:
        parent = StateGraph(ParentState)
        parent.add_node("intro", lambda state: {"messages": ["parent: start"]})
        parent.add_node("team", self.make_child())
        parent.add_node("outro", lambda state: {"messages": ["parent: done"]})
        parent.add_edge(START, "intro").add_edge("intro", "team")
        parent.add_edge("team", "outro").add_edge("outro", END)
        result = parent.compile().invoke({"messages": []})
        self.assertEqual(
            result["messages"],
            ["parent: start", "child: researched", "child: drafted", "parent: done"],
        )
        # The child's private key must not leak into the parent state.
        self.assertNotIn("scratch", result)

    def test_subgraph_without_shared_keys_is_rejected(self) -> None:
        class Unrelated(TypedDict):
            other: int

        child = StateGraph(Unrelated).add_node("noop", lambda state: {})
        child.add_edge(START, "noop").add_edge("noop", END)
        parent = StateGraph(ParentState)
        with self.assertRaisesRegex(GraphValidationError, "shares no state keys"):
            parent.add_node("team", child.compile())

    def test_interrupt_inside_subgraph_pauses_parent_and_resumes(self) -> None:
        child = StateGraph(ChildState)

        def approval(state):
            answer = interrupt({"question": "publish the draft?"})
            return {"messages": [f"child: approval={answer}"]}

        child.add_node("draft", lambda state: {"messages": ["child: drafted"]})
        child.add_node("approve", approval)
        child.add_edge(START, "draft").add_edge("draft", "approve").add_edge("approve", END)

        parent = StateGraph(ParentState)
        parent.add_node("team", child.compile())
        parent.add_node("publish", lambda state: {"messages": ["parent: published"]})
        parent.add_edge(START, "team").add_edge("team", "publish").add_edge("publish", END)

        saver = InMemorySaver()
        graph = parent.compile(checkpointer=saver)
        config = {"configurable": {"thread_id": "subgraph-hitl"}}

        paused = graph.invoke({"messages": []}, config)
        self.assertEqual(paused["__interrupt__"][0].value["question"], "publish the draft?")
        self.assertEqual(graph.get_state(config).next, ("team",))

        result = graph.invoke(Command(resume="yes"), config)
        self.assertEqual(
            result["messages"],
            ["child: drafted", "child: approval=yes", "parent: published"],
        )

    def test_two_sequential_interrupts_inside_subgraph(self) -> None:
        child = StateGraph(ChildState)

        def gate_one(state):
            answer = interrupt("first gate?")
            return {"messages": [f"gate1={answer}"]}

        def gate_two(state):
            answer = interrupt("second gate?")
            return {"messages": [f"gate2={answer}"]}

        child.add_node("one", gate_one).add_node("two", gate_two)
        child.add_edge(START, "one").add_edge("one", "two").add_edge("two", END)

        parent = StateGraph(ParentState)
        parent.add_node("team", child.compile())
        parent.add_edge(START, "team").add_edge("team", END)
        graph = parent.compile(checkpointer=InMemorySaver())
        config = {"configurable": {"thread_id": "subgraph-two-gates"}}

        first = graph.invoke({"messages": []}, config)
        self.assertEqual(first["__interrupt__"][0].value, "first gate?")
        second = graph.invoke(Command(resume="a"), config)
        self.assertEqual(second["__interrupt__"][0].value, "second gate?")
        final = graph.invoke(Command(resume="b"), config)
        self.assertEqual(final["messages"], ["gate1=a", "gate2=b"])

    def test_parent_loop_reinvokes_subgraph_with_fresh_input(self) -> None:
        child = StateGraph(ChildState)
        child.add_node("echo", lambda state: {"messages": [f"seen:{len(state['messages'])}"]})
        child.add_edge(START, "echo").add_edge("echo", END)

        parent = StateGraph(ParentState)
        parent.add_node("team", child.compile())
        parent.add_node(
            "check",
            lambda state: Command(goto="team" if len(state["messages"]) < 2 else END),
            destinations=("team",),
        )
        parent.add_edge(START, "team").add_edge("team", "check")
        graph = parent.compile(checkpointer=InMemorySaver())
        config = {"configurable": {"thread_id": "subgraph-loop"}}
        result = graph.invoke({"messages": []}, config)
        self.assertEqual(result["messages"], ["seen:0", "seen:1"])


if __name__ == "__main__":
    unittest.main()
