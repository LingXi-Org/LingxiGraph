import operator
import unittest
from typing import Annotated, TypedDict

from lingxigraph import (
    END,
    START,
    Command,
    GraphValidationError,
    InMemorySaver,
    Send,
    StateGraph,
    interrupt,
)


class MapState(TypedDict, total=False):
    items: list[int]
    results: Annotated[list[int], operator.add]


class SendTests(unittest.TestCase):
    def test_conditional_edge_fans_out_with_private_inputs(self) -> None:
        builder = StateGraph(MapState)
        builder.add_node("plan", lambda state: {})
        builder.add_node("worker", lambda payload: {"results": [payload["value"] * 2]})
        builder.add_edge(START, "plan")
        builder.add_conditional_edges(
            "plan",
            lambda state: [Send("worker", {"value": item}) for item in state["items"]],
        )
        builder.add_edge("worker", END)
        result = builder.compile().invoke({"items": [1, 2, 3]})
        self.assertEqual(result["results"], [2, 4, 6])

    def test_command_goto_accepts_sends(self) -> None:
        builder = StateGraph(MapState)
        builder.add_node(
            "plan",
            lambda state: Command(
                goto=[Send("worker", {"value": item}) for item in state["items"]]
            ),
            destinations=("worker",),
        )
        builder.add_node("worker", lambda payload: {"results": [payload["value"] + 10]})
        builder.add_edge(START, "plan").add_edge("worker", END)
        result = builder.compile().invoke({"items": [5, 6]})
        self.assertEqual(result["results"], [15, 16])

    def test_conditional_entry_point_can_fan_out_from_start(self) -> None:
        builder = StateGraph(MapState)
        builder.add_node("worker", lambda payload: {"results": [payload["value"] * 3]})
        builder.add_conditional_edges(
            START,
            lambda state: [Send("worker", {"value": item}) for item in state["items"]],
        )
        builder.add_edge("worker", END)
        result = builder.compile().invoke({"items": [1, 2]})
        self.assertEqual(result["results"], [3, 6])

    def test_send_reduce_step_sees_merged_results(self) -> None:
        builder = StateGraph(MapState)
        builder.add_node("plan", lambda state: {})
        builder.add_node("worker", lambda payload: {"results": [payload["value"]]})
        builder.add_node("reduce", lambda state: {"results": [sum(state["results"])]})
        builder.add_edge(START, "plan")
        builder.add_conditional_edges(
            "plan",
            lambda state: [Send("worker", {"value": item}) for item in state["items"]],
        )
        builder.add_edge("worker", "reduce").add_edge("reduce", END)
        result = builder.compile().invoke({"items": [1, 2, 3]})
        self.assertEqual(result["results"], [1, 2, 3, 6])

    def test_send_to_unknown_node_fails_at_runtime(self) -> None:
        builder = StateGraph(MapState)
        builder.add_node("plan", lambda state: Command(goto=Send("ghost", {})))
        builder.add_edge(START, "plan").add_edge("plan", END)
        with self.assertRaisesRegex(GraphValidationError, "ghost"):
            builder.compile().invoke({"items": []})

    def test_pending_sends_survive_interrupt_and_resume(self) -> None:
        saver = InMemorySaver()
        config = {"configurable": {"thread_id": "send-durability"}}

        def worker(payload):
            answer = interrupt({"item": payload["value"]})
            return {"results": [payload["value"] if answer else -payload["value"]]}

        builder = StateGraph(MapState)
        builder.add_node("plan", lambda state: {})
        builder.add_node("worker", worker)
        builder.add_edge(START, "plan")
        builder.add_conditional_edges(
            "plan",
            lambda state: [Send("worker", {"value": item}) for item in state["items"]],
        )
        builder.add_edge("worker", END)
        graph = builder.compile(checkpointer=saver)

        paused = graph.invoke({"items": [7, 8]}, config)
        self.assertEqual(len(paused["__interrupt__"]), 2)
        self.assertEqual(
            [marker.value["item"] for marker in paused["__interrupt__"]], [7, 8]
        )
        self.assertEqual(graph.get_state(config).next, ("worker", "worker"))

        second = graph.invoke(Command(resume=True), config)
        self.assertEqual(len(second["__interrupt__"]), 1)
        final = graph.invoke(Command(resume=False), config)
        self.assertEqual(final["results"], [7, -8])

    def test_targeted_resume_by_interrupt_id_answers_both_tasks(self) -> None:
        saver = InMemorySaver()
        config = {"configurable": {"thread_id": "send-mapped-resume"}}

        def worker(payload):
            answer = interrupt({"item": payload["value"]})
            return {"results": [answer]}

        builder = StateGraph(MapState)
        builder.add_node("plan", lambda state: {})
        builder.add_node("worker", worker)
        builder.add_edge(START, "plan")
        builder.add_conditional_edges(
            "plan",
            lambda state: [Send("worker", {"value": item}) for item in state["items"]],
        )
        builder.add_edge("worker", END)
        graph = builder.compile(checkpointer=saver)

        paused = graph.invoke({"items": [1, 2]}, config)
        answers = {marker.id: marker.value["item"] * 100 for marker in paused["__interrupt__"]}
        final = graph.invoke(Command(resume=answers), config)
        self.assertEqual(final["results"], [100, 200])


if __name__ == "__main__":
    unittest.main()
