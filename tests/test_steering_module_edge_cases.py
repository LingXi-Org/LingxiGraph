"""Direct unit tests for lingxigraph.steering module-level edge cases."""

import unittest

from lingxigraph.steering import (
    SteeringChannel,
    SteeringPayloadTooLarge,
    validate_steering_payload,
)


class ValidateSteeringPayloadTests(unittest.TestCase):
    def test_rejects_unserializable_payload(self) -> None:
        class Unserializable:
            pass

        with self.assertRaises(SteeringPayloadTooLarge):
            validate_steering_payload({"bad": Unserializable()})


class SteeringChannelDrainCallbackTests(unittest.TestCase):
    def test_on_drain_exception_is_swallowed(self) -> None:
        channel = SteeringChannel(run_id="run-1")

        def boom(_drained: object) -> None:
            raise RuntimeError("observability sink is down")

        channel.on_drain = boom
        channel.submit(kind="note", payload={"a": 1})
        # Must not raise even though on_drain blows up.
        drained = channel.drain()
        self.assertEqual(len(drained), 1)

    def test_ack_consumed_with_empty_ids_is_a_no_op(self) -> None:
        channel = SteeringChannel(run_id="run-1")
        channel.submit(kind="note", payload={"a": 1})
        channel.drain()
        before = channel.peek_consumed()
        channel.ack_consumed([])
        after = channel.peek_consumed()
        self.assertEqual(before, after)

    def test_ack_consumed_is_a_no_op_when_log_already_empty(self) -> None:
        channel = SteeringChannel(run_id="run-1")
        # Nothing submitted/drained -- consumed log starts empty.
        channel.ack_consumed(["nonexistent-id"])  # must not raise
        self.assertEqual(channel.peek_consumed(), ())


if __name__ == "__main__":
    unittest.main()
