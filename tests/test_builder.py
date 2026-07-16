import unittest
from typing import TypedDict

from lingxigraph import END, START, GraphValidationError, StateGraph


class State(TypedDict):
    value: int


def node(state):
    return {"value": state.get("value", 0) + 1}


class BuilderTests(unittest.TestCase):
    def test_valid_graph_compiles(self) -> None:
        builder = StateGraph(State)
        builder.add_node("a", node).add_edge(START, "a").add_edge("a", END)
        self.assertIsNotNone(builder.compile())

    def test_duplicate_and_reserved_node_names_fail(self) -> None:
        builder = StateGraph(State).add_node("a", node)
        with self.assertRaises(GraphValidationError):
            builder.add_node("a", node)
        with self.assertRaises(GraphValidationError):
            builder.add_node(START, node)

    def test_unknown_edge_endpoint_fails_at_compile(self) -> None:
        builder = StateGraph(State).add_node("a", node)
        builder.add_edge(START, "a").add_edge("a", "missing")
        with self.assertRaises(GraphValidationError):
            builder.compile()

    def test_missing_entry_fails(self) -> None:
        builder = StateGraph(State).add_node("a", node).add_edge("a", END)
        with self.assertRaises(GraphValidationError):
            builder.compile()

    def test_unknown_interrupt_node_fails(self) -> None:
        builder = StateGraph(State).add_node("a", node).add_edge(START, "a")
        with self.assertRaises(GraphValidationError):
            builder.compile(interrupt_before=["missing"])

    def test_unreachable_node_fails(self) -> None:
        builder = StateGraph(State)
        builder.add_node("a", node).add_node("orphan", node).add_edge(START, "a")
        with self.assertRaises(GraphValidationError):
            builder.compile()

    def test_conditional_target_is_validated(self) -> None:
        builder = StateGraph(State).add_node("a", node).add_edge(START, "a")
        builder.add_conditional_edges("a", lambda state: "bad", {"bad": "missing"})
        with self.assertRaises(GraphValidationError):
            builder.compile()


if __name__ == "__main__":
    unittest.main()
