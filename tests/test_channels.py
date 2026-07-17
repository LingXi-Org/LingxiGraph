import operator
import unittest
from typing import Annotated, TypedDict

from lingxigraph.channels import (
    BinaryOperatorAggregate,
    EphemeralValue,
    LastValue,
    ReplaceValue,
    Topic,
    extract_channels,
    merge_updates,
)
from lingxigraph.errors import InvalidUpdateError


class State(TypedDict):
    messages: Annotated[list[str], operator.add]
    winner: str


class ChannelTests(unittest.TestCase):
    def setUp(self) -> None:
        self.channels = extract_channels(State)

    def test_schema_extracts_reducer_and_last_value(self) -> None:
        self.assertIsInstance(self.channels["messages"], BinaryOperatorAggregate)
        self.assertIsInstance(self.channels["winner"], LastValue)

    def test_last_value_overwrites_across_steps(self) -> None:
        result = merge_updates(
            {"messages": [], "winner": "old"},
            [("agent", {"winner": "new"})],
            self.channels,
        )
        self.assertEqual(result["winner"], "new")

    def test_reducer_merges_in_task_order(self) -> None:
        result = merge_updates(
            {"messages": ["start"]},
            [("b", {"messages": ["b"]}), ("a", {"messages": ["a"]})],
            self.channels,
        )
        self.assertEqual(result["messages"], ["start", "b", "a"])

    def test_parallel_last_value_writes_fail(self) -> None:
        with self.assertRaises(InvalidUpdateError):
            merge_updates(
                {},
                [("a", {"winner": "a"}), ("b", {"winner": "b"})],
                self.channels,
            )

    def test_unknown_key_fails(self) -> None:
        with self.assertRaises(InvalidUpdateError):
            merge_updates({}, [("a", {"missing": 1})], self.channels)

    def test_topic_and_ephemeral_channels(self) -> None:
        class AdvancedState(TypedDict):
            events: Annotated[list[str], Topic(str, accumulate=True)]
            current: Annotated[str, EphemeralValue(str)]

        channels = extract_channels(AdvancedState)
        first = merge_updates(
            {},
            [("a", {"events": ["a"], "current": "now"}), ("b", {"events": "b"})],
            channels,
        )
        self.assertEqual(first, {"events": ["a", "b"], "current": "now"})
        second = merge_updates(first, [("c", {"events": ["c"]})], channels)
        self.assertEqual(second, {"events": ["a", "b", "c"]})
        replaced = merge_updates(
            second,
            [("d", {"events": ReplaceValue(["reset"]), "current": ReplaceValue("x")})],
            channels,
        )
        self.assertEqual(replaced, {"events": ["reset"], "current": "x"})
        with self.assertRaises(InvalidUpdateError):
            merge_updates(
                {},
                [("a", {"current": "a"}), ("b", {"current": "b"})],
                channels,
            )

        non_accumulating = Topic(str)
        self.assertEqual(non_accumulating.merge(["old"], [], key="events"), [])


if __name__ == "__main__":
    unittest.main()
