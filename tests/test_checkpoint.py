import unittest
from typing import TypedDict

from lingxigraph import END, START, InMemorySaver, StateGraph


class State(TypedDict):
    count: int


def make_graph(saver, *, interrupt_before=()):
    builder = StateGraph(State)
    builder.add_node("one", lambda state: {"count": state["count"] + 1})
    builder.add_node("two", lambda state: {"count": state["count"] + 1})
    builder.add_edge(START, "one").add_edge("one", "two").add_edge("two", END)
    return builder.compile(checkpointer=saver, interrupt_before=interrupt_before)


class CheckpointTests(unittest.TestCase):
    def setUp(self) -> None:
        self.saver = InMemorySaver()
        self.config = {"configurable": {"thread_id": "checkpoint-test"}}

    def test_each_superstep_is_checkpointed(self) -> None:
        graph = make_graph(self.saver)
        graph.invoke({"count": 0}, self.config)
        history = list(graph.get_state_history(self.config))
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0].values["count"], 2)
        self.assertEqual(history[0].next, ())

    def test_same_thread_resumes_from_next_nodes(self) -> None:
        graph = make_graph(self.saver, interrupt_before=["two"])
        paused = graph.invoke({"count": 0}, self.config)
        self.assertEqual(paused["count"], 1)
        self.assertEqual(graph.get_state(self.config).next, ("two",))
        resumed = graph.invoke(None, self.config)
        self.assertEqual(resumed["count"], 2)
        self.assertEqual(graph.get_state(self.config).next, ())

    def test_get_history_and_update_state(self) -> None:
        graph = make_graph(self.saver, interrupt_before=["two"])
        graph.invoke({"count": 0}, self.config)
        graph.update_state(self.config, {"count": 40})
        snapshot = graph.get_state(self.config)
        self.assertEqual(snapshot.values["count"], 40)
        self.assertEqual(snapshot.next, ("two",))
        self.assertEqual(snapshot.metadata["source"], "update_state")
        self.assertEqual(len(list(graph.get_state_history(self.config))), 3)


if __name__ == "__main__":
    unittest.main()
