import tempfile
import unittest
from pathlib import Path
from typing import TypedDict

from lingxigraph import END, START, Command, SqliteSaver, StateGraph, interrupt


class State(TypedDict, total=False):
    count: int
    approval: str


def make_graph(saver, *, interrupt_before=()):
    builder = StateGraph(State)
    builder.add_node("one", lambda state: {"count": state["count"] + 1})
    builder.add_node("two", lambda state: {"count": state["count"] + 1})
    builder.add_edge(START, "one").add_edge("one", "two").add_edge("two", END)
    return builder.compile(checkpointer=saver, interrupt_before=interrupt_before)


class SqliteSaverTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = {"configurable": {"thread_id": "sqlite-test"}}

    def test_in_memory_database_checkpoints_each_superstep(self) -> None:
        with SqliteSaver() as saver:
            graph = make_graph(saver)
            graph.invoke({"count": 0}, self.config)
            history = list(graph.get_state_history(self.config))
            self.assertEqual(len(history), 2)
            self.assertEqual(history[0].values["count"], 2)
            self.assertEqual(history[0].next, ())

    def test_pause_and_resume_within_one_saver(self) -> None:
        with SqliteSaver() as saver:
            graph = make_graph(saver, interrupt_before=["two"])
            paused = graph.invoke({"count": 0}, self.config)
            self.assertEqual(paused["count"], 1)
            self.assertEqual(graph.get_state(self.config).next, ("two",))
            resumed = graph.invoke(None, self.config)
            self.assertEqual(resumed["count"], 2)

    def test_interrupt_survives_process_style_reopen(self) -> None:
        def approval(state):
            answer = interrupt({"question": "publish?"})
            return {"approval": answer, "count": state["count"]}

        def build(saver):
            builder = StateGraph(State)
            builder.add_node("approval", approval)
            builder.add_edge(START, "approval").add_edge("approval", END)
            return builder.compile(checkpointer=saver)

        with tempfile.TemporaryDirectory() as tmp:
            database = str(Path(tmp) / "checkpoints.sqlite")
            with SqliteSaver(database) as saver:
                paused = build(saver).invoke({"count": 3}, self.config)
                self.assertEqual(paused["__interrupt__"][0].value["question"], "publish?")
            # A fresh saver over the same file must resume where the first left off.
            with SqliteSaver(database) as saver:
                result = build(saver).invoke(Command(resume="yes"), self.config)
                self.assertEqual(result["approval"], "yes")
                self.assertEqual(result["count"], 3)

    def test_lookup_by_checkpoint_id_returns_historic_state(self) -> None:
        with SqliteSaver() as saver:
            graph = make_graph(saver)
            graph.invoke({"count": 0}, self.config)
            history = list(graph.get_state_history(self.config))
            oldest = history[-1]
            snapshot = graph.get_state(oldest.config)
            self.assertEqual(snapshot.values["count"], oldest.values["count"])

    def test_threads_are_isolated(self) -> None:
        with SqliteSaver() as saver:
            graph = make_graph(saver)
            graph.invoke({"count": 0}, {"configurable": {"thread_id": "a"}})
            graph.invoke({"count": 10}, {"configurable": {"thread_id": "b"}})
            state_a = graph.get_state({"configurable": {"thread_id": "a"}})
            state_b = graph.get_state({"configurable": {"thread_id": "b"}})
            self.assertEqual(state_a.values["count"], 2)
            self.assertEqual(state_b.values["count"], 12)


if __name__ == "__main__":
    unittest.main()
