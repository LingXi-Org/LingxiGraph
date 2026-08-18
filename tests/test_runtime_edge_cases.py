"""Direct unit tests for lingxigraph.runtime.Runtime edge cases.

These exercise Runtime as a plain dataclass -- no graph execution needed --
targeting branches that graph-execution tests don't happen to hit (deadline
expiry, cancellation-token raise, empty steering channel, legacy stream
writer misuse, budget-less consume_* no-ops, get_config outside a node).
"""

import unittest
from datetime import UTC, datetime, timedelta

from lingxigraph.errors import GraphCancelledError, GraphTimeoutError
from lingxigraph.runtime import CancellationToken, Runtime, get_config, get_store


class CancellationTokenTests(unittest.TestCase):
    def test_raise_if_cancelled_raises_after_cancel(self) -> None:
        token = CancellationToken()
        self.assertFalse(token.cancelled)
        token.cancel()
        self.assertTrue(token.cancelled)
        with self.assertRaises(GraphCancelledError):
            token.raise_if_cancelled()


class RuntimeEdgeCaseTests(unittest.TestCase):
    def _runtime(self, **overrides: object) -> Runtime:
        defaults: dict[str, object] = {"context": None, "config": {}}
        defaults.update(overrides)
        return Runtime(**defaults)  # type: ignore[arg-type]

    def test_peek_and_drain_steering_are_empty_without_a_channel(self) -> None:
        runtime = self._runtime()
        self.assertEqual(runtime.peek_steering(), ())
        self.assertEqual(runtime.drain_steering(), ())
        self.assertFalse(runtime.has_steering)

    def test_raise_if_cancelled_raises_on_expired_deadline(self) -> None:
        runtime = self._runtime(deadline=datetime.now(UTC) - timedelta(seconds=1))
        with self.assertRaises(GraphTimeoutError):
            runtime.raise_if_cancelled()

    def test_raise_if_cancelled_delegates_to_cancellation_token(self) -> None:
        token = CancellationToken()
        token.cancel()
        runtime = self._runtime(cancellation=token)
        with self.assertRaises(GraphCancelledError):
            runtime.raise_if_cancelled()

    def test_raise_if_cancelled_is_a_no_op_when_nothing_set(self) -> None:
        runtime = self._runtime()
        runtime.raise_if_cancelled()  # must not raise

    def test_emit_rejects_empty_or_non_string_channel(self) -> None:
        runtime = self._runtime()
        with self.assertRaises(ValueError):
            runtime.emit("", 1)
        with self.assertRaises(ValueError):
            runtime.emit(None, 1)  # type: ignore[arg-type]

    def test_emit_without_a_sink_is_a_silent_no_op(self) -> None:
        runtime = self._runtime()
        runtime.emit("progress", {"a": 1})  # must not raise, no emitter attached

    def test_stream_writer_legacy_two_arg_form_requires_string_channel(self) -> None:
        runtime = self._runtime()
        writer = runtime.stream_writer
        with self.assertRaises(TypeError):
            writer(123, "extra", "too-many")  # more than one legacy arg
        with self.assertRaises(TypeError):
            writer(123, "channel")  # value must be the channel string in legacy form

    def test_stream_writer_legacy_form_emits_when_sink_present(self) -> None:
        captured: list[tuple[str, object]] = []
        runtime = self._runtime(_emit=lambda channel, value: captured.append((channel, value)))
        writer = runtime.stream_writer
        writer("hello", "custom-channel")
        self.assertEqual(captured, [("hello", "custom-channel")])

    def test_consume_helpers_are_no_ops_without_a_budget(self) -> None:
        runtime = self._runtime()
        runtime.consume_tool_call("some_tool")
        runtime.consume_model_usage({"tokens": 5})
        runtime.consume_model_call()  # none of these should raise


class ContextAccessorTests(unittest.TestCase):
    def test_get_config_outside_a_node_raises(self) -> None:
        with self.assertRaises(RuntimeError):
            get_config()

    def test_get_store_outside_a_node_raises(self) -> None:
        with self.assertRaises(RuntimeError):
            get_store()


if __name__ == "__main__":
    unittest.main()
