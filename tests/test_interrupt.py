import unittest
from typing import TypedDict

from lingxigraph import (
    END,
    START,
    Command,
    InMemorySaver,
    StateGraph,
    interrupt,
)


class State(TypedDict, total=False):
    count: int
    approval: str


class InterruptTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = {"configurable": {"thread_id": "interrupt-test"}}

    def test_interrupt_before_pauses_and_resumes(self) -> None:
        saver = InMemorySaver()
        builder = StateGraph(State)
        builder.add_node("human", lambda state: {"count": state["count"] + 1})
        builder.add_edge(START, "human").add_edge("human", END)
        graph = builder.compile(checkpointer=saver, interrupt_before=["human"])
        self.assertEqual(graph.invoke({"count": 0}, self.config)["count"], 0)
        self.assertEqual(graph.invoke(None, self.config)["count"], 1)

    def test_interrupt_after_pauses_with_next_node_persisted(self) -> None:
        saver = InMemorySaver()
        builder = StateGraph(State)
        builder.add_node("first", lambda state: {"count": state["count"] + 1})
        builder.add_node("second", lambda state: {"count": state["count"] + 1})
        builder.add_edge(START, "first").add_edge("first", "second").add_edge("second", END)
        graph = builder.compile(checkpointer=saver, interrupt_after=["first"])
        self.assertEqual(graph.invoke({"count": 0}, self.config)["count"], 1)
        self.assertEqual(graph.get_state(self.config).next, ("second",))
        self.assertEqual(graph.invoke(None, self.config)["count"], 2)

    def test_dynamic_interrupt_reexecutes_node_with_resume_value(self) -> None:
        saver = InMemorySaver()
        calls = []

        def approval(state):
            calls.append("called")
            answer = interrupt({"question": "approve?"})
            return {"approval": answer}

        builder = StateGraph(State).add_node("approval", approval)
        builder.add_edge(START, "approval").add_edge("approval", END)
        graph = builder.compile(checkpointer=saver)
        paused = graph.invoke({"count": 0}, self.config)
        self.assertEqual(paused["__interrupt__"][0].value, {"question": "approve?"})
        resumed = graph.invoke(Command(resume="yes"), self.config)
        self.assertEqual(resumed["approval"], "yes")
        self.assertEqual(len(calls), 2)

    def test_multiple_dynamic_interrupts_match_resume_call_order(self) -> None:
        saver = InMemorySaver()

        def collect_answers(state):
            first = interrupt("first?")
            second = interrupt("second?")
            return {"approval": f"{first}/{second}"}

        builder = StateGraph(State).add_node("questions", collect_answers)
        builder.add_edge(START, "questions").add_edge("questions", END)
        graph = builder.compile(checkpointer=saver)

        first_pause = graph.invoke({"count": 0}, self.config)
        self.assertEqual(first_pause["__interrupt__"][0].value, "first?")
        second_pause = graph.invoke(Command(resume="one"), self.config)
        self.assertEqual(second_pause["__interrupt__"][0].value, "second?")
        completed = graph.invoke(Command(resume="two"), self.config)
        self.assertEqual(completed["approval"], "one/two")

    def test_dynamic_interrupt_without_checkpointer_fails(self) -> None:
        builder = StateGraph(State).add_node("approval", lambda state: interrupt("approve?"))
        builder.add_edge(START, "approval").add_edge("approval", END)
        with self.assertRaisesRegex(RuntimeError, "requires a checkpointer"):
            builder.compile().invoke({"count": 0})


if __name__ == "__main__":
    unittest.main()
