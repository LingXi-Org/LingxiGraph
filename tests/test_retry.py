import unittest
from typing import TypedDict

from lingxigraph import END, START, RetryPolicy, StateGraph


class State(TypedDict, total=False):
    value: int


class RetryTests(unittest.TestCase):
    def test_node_retries_until_success(self) -> None:
        attempts: list[int] = []

        def flaky(state):
            attempts.append(1)
            if len(attempts) < 3:
                raise ConnectionError("transient failure")
            return {"value": state["value"] + 1}

        builder = StateGraph(State)
        builder.add_node(
            "flaky",
            flaky,
            retry=RetryPolicy(max_attempts=3, initial_interval=0, jitter=False),
        )
        builder.add_edge(START, "flaky").add_edge("flaky", END)
        result = builder.compile().invoke({"value": 1})
        self.assertEqual(result["value"], 2)
        self.assertEqual(len(attempts), 3)

    def test_exhausted_retries_raise_the_last_error(self) -> None:
        attempts: list[int] = []

        def always_fails(state):
            attempts.append(1)
            raise ConnectionError("still down")

        builder = StateGraph(State)
        builder.add_node(
            "down",
            always_fails,
            retry=RetryPolicy(max_attempts=2, initial_interval=0, jitter=False),
        )
        builder.add_edge(START, "down").add_edge("down", END)
        with self.assertRaisesRegex(ConnectionError, "still down"):
            builder.compile().invoke({"value": 0})
        self.assertEqual(len(attempts), 2)

    def test_unmatched_exceptions_are_not_retried(self) -> None:
        attempts: list[int] = []

        def wrong_kind(state):
            attempts.append(1)
            raise KeyError("not retryable")

        builder = StateGraph(State)
        builder.add_node(
            "wrong",
            wrong_kind,
            retry=RetryPolicy(max_attempts=5, initial_interval=0, retry_on=ConnectionError),
        )
        builder.add_edge(START, "wrong").add_edge("wrong", END)
        with self.assertRaises(KeyError):
            builder.compile().invoke({"value": 0})
        self.assertEqual(len(attempts), 1)

    def test_policy_rejects_invalid_configuration(self) -> None:
        with self.assertRaises(ValueError):
            RetryPolicy(max_attempts=0)
        with self.assertRaises(ValueError):
            RetryPolicy(backoff_factor=0.5)


if __name__ == "__main__":
    unittest.main()
