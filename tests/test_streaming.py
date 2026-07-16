import unittest
from typing import TypedDict

from lingxigraph import END, START, EventKind, StateGraph


class State(TypedDict):
    count: int


def make_graph():
    builder = StateGraph(State)
    builder.add_node("one", lambda state: {"count": state["count"] + 1})
    builder.add_node("two", lambda state: {"count": state["count"] + 1})
    builder.add_edge(START, "one").add_edge("one", "two").add_edge("two", END)
    return builder.compile()


class StreamingTests(unittest.TestCase):
    def test_values_streams_full_state_after_each_step(self) -> None:
        values = list(make_graph().stream({"count": 0}, stream_mode="values"))
        self.assertEqual(values, [{"count": 1}, {"count": 2}])

    def test_updates_streams_node_updates(self) -> None:
        updates = list(make_graph().stream({"count": 0}, stream_mode="updates"))
        self.assertEqual(updates, [{"one": {"count": 1}}, {"two": {"count": 2}}])

    def test_events_have_stable_lifecycle_order(self) -> None:
        events = list(make_graph().stream({"count": 0}, stream_mode="events"))
        self.assertEqual(
            [event.kind for event in events],
            [
                EventKind.RUN_STARTED,
                EventKind.NODE_STARTED,
                EventKind.NODE_COMPLETED,
                EventKind.STATE_UPDATED,
                EventKind.NODE_STARTED,
                EventKind.NODE_COMPLETED,
                EventKind.STATE_UPDATED,
                EventKind.RUN_COMPLETED,
            ],
        )
        self.assertEqual([event.node for event in events if event.node], ["one", "one", "two", "two"])


if __name__ == "__main__":
    unittest.main()
