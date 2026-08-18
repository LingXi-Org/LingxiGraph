"""Tests for durable mid-run steering (issue #16)."""

from __future__ import annotations

import asyncio
import operator
import time
import unittest
from typing import Annotated, Any, TypedDict

from fastapi.testclient import TestClient

from lingxigraph import END, START, RetryPolicy, Runtime, Send, StateGraph
from lingxigraph.errors import RunResumeConflictError, RunTerminalError
from lingxigraph.server import GraphRegistry, create_app
from lingxigraph.server.models import ThreadCreate
from lingxigraph.server.repository import InMemoryRepository
from lingxigraph.server.security import Authenticator
from lingxigraph.steering import (
    MAX_STEERING_PAYLOAD_BYTES,
    SteeringChannel,
    SteeringConsumption,
    SteeringPayloadTooLarge,
    validate_steering_payload,
)
from lingxigraph.types import RunStatus


class State(TypedDict, total=False):
    ticks: int
    drained: list[str]


def wait_for_status(client, run_id, headers, expected, attempts: int = 200):
    value = None
    for _ in range(attempts):
        value = client.get(f"/v1/runs/{run_id}", headers=headers)
        if value.json()["status"] in expected:
            return value
        time.sleep(0.01)
    return value


class SteeringChannelTests(unittest.TestCase):
    """Core steering primitive: ordering, dedup, drain-once semantics."""

    def test_ordering_and_drain_once(self) -> None:
        channel = SteeringChannel("run-1")
        for index in range(3):
            channel.submit(kind="user_input", payload={"i": index})
        self.assertTrue(channel.has_pending)
        drained = channel.drain()
        self.assertEqual([event.payload["i"] for event in drained], [0, 1, 2])
        self.assertEqual([event.sequence for event in drained], [1, 2, 3])
        # Draining is one-shot: nothing left to drain again.
        self.assertFalse(channel.has_pending)
        self.assertEqual(channel.drain(), ())

    def test_idempotency_key_dedup(self) -> None:
        channel = SteeringChannel("run-1")
        first, created_first = channel.submit(
            kind="user_input", payload={"message": "a"}, idempotency_key="msg-1"
        )
        second, created_second = channel.submit(
            kind="user_input",
            payload={"message": "duplicate-should-be-ignored"},
            idempotency_key="msg-1",
        )
        self.assertTrue(created_first)
        self.assertFalse(created_second)
        self.assertEqual(first.id, second.id)
        self.assertEqual(len(channel.peek()), 1)

    def test_dedup_survives_after_drain(self) -> None:
        channel = SteeringChannel("run-1")
        channel.submit(kind="user_input", payload={}, idempotency_key="msg-1")
        channel.drain()
        _, created = channel.submit(kind="user_input", payload={}, idempotency_key="msg-1")
        self.assertFalse(created)

    def test_payload_size_cap(self) -> None:
        huge = {"message": "x" * (MAX_STEERING_PAYLOAD_BYTES + 1)}
        with self.assertRaises(SteeringPayloadTooLarge):
            validate_steering_payload(huge)

    def test_ingest_preserves_durable_sequence(self) -> None:
        channel = SteeringChannel("run-1")
        from datetime import UTC, datetime

        from lingxigraph.steering import SteeringEvent

        channel.ingest(
            SteeringEvent(
                id="db-1",
                run_id="run-1",
                sequence=5,
                kind="user_input",
                payload={},
                metadata={},
                created_at=datetime.now(UTC),
            )
        )
        self.assertTrue(channel.has_pending)
        # A locally-submitted event after ingest gets a higher sequence.
        event, _ = channel.submit(kind="user_input", payload={})
        self.assertGreater(event.sequence, 5)


class RuntimeDrainTests(unittest.TestCase):
    """Runtime.has_steering / drain_steering / peek_steering (embedded mode)."""

    def test_embedded_graph_steer_drain_and_ordering(self) -> None:
        seen: list[list[str]] = []

        def node(state: State, runtime: Runtime[Any]) -> dict[str, Any]:
            events = runtime.drain_steering()
            seen.append([event.payload["message"] for event in events])
            return {"ticks": state.get("ticks", 0) + 1}

        builder = StateGraph(State)
        builder.add_node("node", node)
        builder.add_edge(START, "node").add_edge("node", END)
        graph = builder.compile()

        # Submit three steers in a row *before* the run starts -- they
        # should all be visible, in order, at the very first safe point.
        run_id = "embedded-run-1"
        graph.steer(run_id, kind="user_input", payload={"message": "a"})
        graph.steer(run_id, kind="user_input", payload={"message": "b"})
        graph.steer(run_id, kind="user_input", payload={"message": "c"})
        self.assertTrue(graph.has_pending_steering(run_id))

        graph.invoke({"ticks": 0}, {"configurable": {}}, run_id=run_id)
        self.assertEqual(seen, [["a", "b", "c"]])
        self.assertFalse(graph.has_pending_steering(run_id))

    def test_no_channel_is_a_safe_no_op(self) -> None:
        def node(state: State, runtime: Runtime[Any]) -> dict[str, Any]:
            self_check = runtime.drain_steering()
            assert self_check == ()
            assert runtime.has_steering is False
            assert runtime.steering_pending is False
            return {"ticks": 1}

        builder = StateGraph(State)
        builder.add_node("node", node)
        builder.add_edge(START, "node").add_edge("node", END)
        result = builder.compile().invoke({"ticks": 0}, {"configurable": {}})
        self.assertEqual(result["ticks"], 1)


class SteeringConsumptionTests(unittest.TestCase):
    """SteeringChannel.drain()'s observability record (issue #16 review point 4):
    consuming node/namespace/task_id and queue latency, not just id/sequence/kind."""

    def test_drain_records_consumer_location_and_latency(self) -> None:
        channel = SteeringChannel("run-x")
        channel.submit(kind="user_input", payload={"i": 1})
        time.sleep(0.01)
        drained = channel.drain(node="worker", namespace=("team",), task_id="worker#0")
        self.assertEqual(len(drained), 1)
        consumed = channel.pop_consumed()
        self.assertEqual(len(consumed), 1)
        record = consumed[0]
        self.assertIsInstance(record, SteeringConsumption)
        self.assertEqual(record.event.id, drained[0].id)
        self.assertEqual(record.node, "worker")
        self.assertEqual(record.namespace, ("team",))
        self.assertEqual(record.task_id, "worker#0")
        self.assertGreater(record.queue_latency_seconds, 0)

    def test_drain_without_consumer_context_defaults_to_none(self) -> None:
        channel = SteeringChannel("run-x")
        channel.submit(kind="user_input", payload={})
        channel.drain()
        record = channel.pop_consumed()[0]
        self.assertIsNone(record.node)
        self.assertEqual(record.namespace, ())
        self.assertIsNone(record.task_id)
        self.assertGreaterEqual(record.queue_latency_seconds, 0)

    def test_runtime_drain_steering_reports_the_calling_node(self) -> None:
        """``Runtime.drain_steering()`` (the real node-facing API) must
        thread the node/namespace/task_id through to the consumption
        record automatically -- nodes never pass this by hand."""

        captured: list[SteeringConsumption] = []

        def node(state: State, runtime: Runtime[Any]) -> dict[str, Any]:
            runtime.drain_steering()
            return {"ticks": 1}

        builder = StateGraph(State)
        builder.add_node("node", node)
        builder.add_edge(START, "node").add_edge("node", END)
        graph = builder.compile()
        run_id = "consumption-node-run"
        graph.steer(run_id, kind="user_input", payload={})
        channel = graph.get_steering_channel(run_id)
        graph.invoke({"ticks": 0}, {"configurable": {}}, run_id=run_id)
        captured.extend(channel.pop_consumed())
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0].node, "node")
        self.assertEqual(captured[0].namespace, ())


class EmbeddedSteeringLifecycleTests(unittest.TestCase):
    """Issue #16 review point 2: embedded/library-mode steering channels
    must not accumulate one entry per run forever on a long-lived compiled
    graph, while a paused run's channel must still survive until resume
    actually completes."""

    def test_plain_invoke_never_using_steering_does_not_leak(self) -> None:
        builder = StateGraph(State)
        builder.add_node("node", lambda state: {"ticks": state.get("ticks", 0) + 1})
        builder.add_edge(START, "node").add_edge("node", END)
        graph = builder.compile()
        for _ in range(200):
            graph.invoke({"ticks": 0}, {"configurable": {}})
        self.assertEqual(len(graph._run_steering), 0)

    def test_steer_before_invoke_still_releases_after_the_run_completes(self) -> None:
        builder = StateGraph(State)

        def node(state: State, runtime: Runtime[Any]) -> dict[str, Any]:
            runtime.drain_steering()
            return {"ticks": state.get("ticks", 0) + 1}

        builder.add_node("node", node)
        builder.add_edge(START, "node").add_edge("node", END)
        graph = builder.compile()
        for index in range(50):
            run_id = f"leak-check-{index}"
            graph.steer(run_id, kind="user_input", payload={"i": index})
            graph.invoke({"ticks": 0}, {"configurable": {}}, run_id=run_id)
        self.assertEqual(len(graph._run_steering), 0)

    def test_paused_run_channel_survives_pause_and_releases_after_resume(self) -> None:
        from lingxigraph import Command, interrupt
        from lingxigraph.checkpoint import InMemorySaver

        def node(state: State, runtime: Runtime[Any]) -> dict[str, Any]:
            value = interrupt("pause")
            drained = [event.payload["m"] for event in runtime.drain_steering()]
            return {"ticks": int(value), "drained": drained}

        builder = StateGraph(State)
        builder.add_node("node", node)
        builder.add_edge(START, "node").add_edge("node", END)
        graph = builder.compile(checkpointer=InMemorySaver())
        config = {"configurable": {"thread_id": "leak-thread"}}
        run_id = "leak-pause-resume"

        graph.invoke({"ticks": 0}, config, run_id=run_id)
        # Paused: the channel must stay alive for a future resume/steer.
        self.assertEqual(len(graph._run_steering), 1)

        graph.steer(run_id, kind="user_input", payload={"m": "hi"})
        result = graph.invoke(Command(resume="9"), config, run_id=run_id)
        self.assertEqual(result["drained"], ["hi"])
        # The run truly finished (no further interrupt) -- released.
        self.assertEqual(len(graph._run_steering), 0)


class ParallelAndSubgraphSteeringTests(unittest.TestCase):
    """Issue #16 review point 3: node retry, parallel/superstep fan-out, and
    subgraph namespace sharing were called out as required but missing."""

    def test_retry_does_not_lose_steering_submitted_before_the_successful_attempt(
        self,
    ) -> None:
        """A transient failure *before* draining must not swallow the event
        -- it has to still be there for the attempt that actually succeeds."""

        attempts: list[int] = []
        seen: list[list[str]] = []

        def flaky(state: State, runtime: Runtime[Any]) -> dict[str, Any]:
            attempts.append(1)
            if len(attempts) < 2:
                raise ConnectionError("transient, before draining anything")
            drained = [event.payload["m"] for event in runtime.drain_steering()]
            seen.append(drained)
            return {"ticks": state.get("ticks", 0) + 1}

        builder = StateGraph(State)
        builder.add_node(
            "flaky", flaky, retry=RetryPolicy(max_attempts=3, initial_interval=0, jitter=False)
        )
        builder.add_edge(START, "flaky").add_edge("flaky", END)
        graph = builder.compile()
        run_id = "retry-not-swallowed"
        graph.steer(run_id, kind="user_input", payload={"m": "hello"})
        graph.invoke({"ticks": 0}, {"configurable": {}}, run_id=run_id)
        self.assertEqual(len(attempts), 2)
        self.assertEqual(seen, [["hello"]])

    def test_retry_does_not_re_expose_an_event_already_drained_by_a_failed_attempt(
        self,
    ) -> None:
        """Documented scope cut (see ``SteeringChannel.pop_consumed``):
        drain-once semantics apply per *channel read*, not per node
        *attempt*. If an attempt drains an event and then fails, that
        event is gone from the channel for good -- a retry must not see it
        a second time (no duplicate exposure), even though the attempt
        that originally read it never got to finish using it."""

        attempts: list[int] = []
        seen: list[list[str]] = []

        def flaky(state: State, runtime: Runtime[Any]) -> dict[str, Any]:
            attempts.append(1)
            drained = [event.payload["m"] for event in runtime.drain_steering()]
            seen.append(drained)
            if len(attempts) < 2:
                raise ConnectionError("transient, after already draining")
            return {"ticks": state.get("ticks", 0) + 1}

        builder = StateGraph(State)
        builder.add_node(
            "flaky", flaky, retry=RetryPolicy(max_attempts=3, initial_interval=0, jitter=False)
        )
        builder.add_edge(START, "flaky").add_edge("flaky", END)
        graph = builder.compile()
        run_id = "retry-no-duplicate"
        graph.steer(run_id, kind="user_input", payload={"m": "once"})
        graph.invoke({"ticks": 0}, {"configurable": {}}, run_id=run_id)
        self.assertEqual(len(attempts), 2)
        self.assertEqual(seen, [["once"], []])

    def test_parallel_send_fanout_drain_is_mutually_exclusive_and_ordered(self) -> None:
        """Multiple nodes in the same superstep race to call
        ``drain_steering()`` concurrently -- the channel's lock makes that
        atomic: exactly one task observes the full, correctly ordered
        batch and every other concurrent task observes nothing left."""

        class ParallelState(TypedDict, total=False):
            items: list[int]
            drained_by_task: Annotated[list[tuple[int, tuple[str, ...]]], operator.add]

        async def worker(payload: dict[str, Any], runtime: Runtime[Any]) -> dict[str, Any]:
            events = runtime.drain_steering()
            return {
                "drained_by_task": [
                    (payload["idx"], tuple(event.payload["m"] for event in events))
                ]
            }

        builder = StateGraph(ParallelState)
        builder.add_node("plan", lambda state: {})
        builder.add_node("worker", worker)
        builder.add_edge(START, "plan")
        builder.add_conditional_edges(
            "plan",
            lambda state: [Send("worker", {"idx": item}) for item in state["items"]],
        )
        builder.add_edge("worker", END)
        graph = builder.compile()
        run_id = "parallel-fanout-run"
        graph.steer(run_id, kind="user_input", payload={"m": "a"})
        graph.steer(run_id, kind="user_input", payload={"m": "b"})
        result = graph.invoke({"items": [0, 1, 2, 3]}, {"configurable": {}}, run_id=run_id)

        non_empty = [batch for _, batch in result["drained_by_task"] if batch]
        self.assertEqual(len(non_empty), 1, result["drained_by_task"])
        self.assertEqual(non_empty[0], ("a", "b"))
        total_events = sum(len(batch) for _, batch in result["drained_by_task"])
        self.assertEqual(total_events, 2)
        self.assertFalse(graph.has_pending_steering(run_id))

    def test_subgraph_node_shares_the_parent_runs_steering_channel(self) -> None:
        """A subgraph node's ``runtime.drain_steering()`` must observe (and
        durably consume) the exact same per-run channel as the parent --
        steering is scoped per top-level run, not per namespace."""

        class ChildState(TypedDict, total=False):
            messages: Annotated[list[str], operator.add]

        class ParentState(TypedDict, total=False):
            messages: Annotated[list[str], operator.add]

        def child_node(state: ChildState, runtime: Runtime[Any]) -> dict[str, Any]:
            drained = [event.payload["m"] for event in runtime.drain_steering()]
            self_namespace = runtime.namespace
            return {"messages": [f"child-drained:{drained}:{self_namespace}"]}

        child = StateGraph(ChildState)
        child.add_node("work", child_node)
        child.add_edge(START, "work").add_edge("work", END)

        parent = StateGraph(ParentState)
        parent.add_node("team", child.compile())
        parent.add_edge(START, "team").add_edge("team", END)
        graph = parent.compile()
        run_id = "subgraph-shared-channel"
        graph.steer(run_id, kind="user_input", payload={"m": "x"})
        self.assertTrue(graph.has_pending_steering(run_id))

        result = graph.invoke({"messages": []}, {"configurable": {}}, run_id=run_id)
        self.assertEqual(result["messages"], ["child-drained:['x']:('team',)"])
        self.assertFalse(graph.has_pending_steering(run_id))
        # Top-level embedded run finished cleanly -- no leaked channel.
        self.assertEqual(len(graph._run_steering), 0)


class RepositorySteeringTests(unittest.TestCase):
    """Durable-inbox semantics against InMemoryRepository (Postgres-shaped API)."""

    def test_ordering_dedup_and_consumption(self) -> None:
        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create(), graph_version="1"
            )
            run = await repo.create_run("acme", None, assistant, _run_create(assistant.id))

            e1, c1 = await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"i": 1}
            )
            e2, c2 = await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"i": 2}
            )
            e3, c3 = await repo.submit_steering(
                "acme",
                run.id,
                kind="user_input",
                payload={"i": 2},
                idempotency_key="dup",
            )
            e3b, c3b = await repo.submit_steering(
                "acme",
                run.id,
                kind="user_input",
                payload={"different": True},
                idempotency_key="dup",
            )
            self.assertTrue(c1 and c2 and c3)
            self.assertFalse(c3b)
            self.assertEqual(e3.id, e3b.id)

            pending = await repo.list_pending_steering("acme", run.id)
            self.assertEqual([event.sequence for event in pending], [1, 2, 3])

            await repo.mark_steering_consumed("acme", run.id, [e1.id, e2.id])
            pending_after = await repo.list_pending_steering("acme", run.id)
            self.assertEqual([event.id for event in pending_after], [e3.id])
            all_events = await repo.list_steering("acme", run.id)
            self.assertEqual(
                {event.id: event.status for event in all_events},
                {e1.id: "consumed", e2.id: "consumed", e3.id: "pending"},
            )

        asyncio.run(scenario())

    def test_terminal_run_rejects_steer(self) -> None:
        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create(), graph_version="1"
            )
            run = await repo.create_run("acme", None, assistant, _run_create(assistant.id))
            await repo.finish_run("acme", run.id, "succeeded", output={})
            with self.assertRaises(RunTerminalError):
                await repo.submit_steering("acme", run.id, kind="user_input", payload={})

        asyncio.run(scenario())

    def test_tenant_isolation(self) -> None:
        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create(), graph_version="1"
            )
            run = await repo.create_run("acme", None, assistant, _run_create(assistant.id))
            await repo.submit_steering("acme", run.id, kind="user_input", payload={})
            other_tenant_view = await repo.list_pending_steering("other-tenant", run.id)
            self.assertEqual(other_tenant_view, [])

        asyncio.run(scenario())


class AtomicResumeMigrationTests(unittest.TestCase):
    """Issue #16 PR #17 review point 1: resume-run creation and its steering
    migration must be a single atomic operation -- a worker must never be
    able to claim (let alone finish) the resumed Run before the steering
    that was pending on the paused run has already been migrated onto it."""

    def test_claim_run_never_observes_the_resumed_run_before_steering_migrated(
        self,
    ) -> None:
        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create(), graph_version="1"
            )
            old_run = await repo.create_run("acme", None, assistant, _run_create(assistant.id))
            await repo.submit_steering("acme", old_run.id, kind="user_input", payload={"m": 1})
            await repo.finish_run("acme", old_run.id, "paused", output={})

            claims: list[str] = []
            stop = asyncio.Event()

            async def hammer_claim() -> None:
                # Simulates a worker's poll loop racing the resume call --
                # before the fix, ``create_run`` committed the new Run as
                # ``pending`` (claimable) *before* the separate
                # ``transfer_pending_steering`` call ran, so this could
                # observe (and even finish) the new Run with nothing
                # migrated onto it yet.
                while not stop.is_set():
                    claimed = await repo.claim_run("worker-a", lease_seconds=30)
                    if claimed is not None:
                        claims.append(claimed.id)
                        if claimed.id != old_run.id:
                            pending = await repo.list_pending_steering("acme", claimed.id)
                            self.assertEqual(
                                len(pending),
                                1,
                                "resumed run was claimable before its steering migrated",
                            )
                    await asyncio.sleep(0)

            hammer = asyncio.create_task(hammer_claim())
            from lingxigraph.server.models import RunCreate

            request = RunCreate(
                assistant_id=assistant.id,
                resume=1,
                metadata={"resumed_from_run_id": old_run.id},
            )
            new_run, transferred = await repo.resume_run_with_pending_steering(
                "acme", old_run.thread_id, assistant, request, old_run.id
            )
            self.assertEqual(len(transferred), 1)
            for _ in range(500):
                if new_run.id in claims:
                    break
                await asyncio.sleep(0.001)
            stop.set()
            await hammer
            self.assertIn(new_run.id, claims)

        asyncio.run(scenario())

    def test_concurrent_resume_of_the_same_paused_run_only_succeeds_once(self) -> None:
        """Issue #16 PR #17 review round 4, point 1: two concurrent
        ``resume_run_with_pending_steering`` calls against the same paused
        run must not both create a descendant Run -- exactly one must
        succeed and ``superseded_by_run_id`` must be stable (never
        overwritten by the loser)."""

        from lingxigraph.server.models import RunCreate

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create(), graph_version="1"
            )
            old_run = await repo.create_run("acme", None, assistant, _run_create(assistant.id))
            await repo.finish_run("acme", old_run.id, "paused", output={})

            request = RunCreate(assistant_id=assistant.id, resume=1)
            results = await asyncio.gather(
                repo.resume_run_with_pending_steering(
                    "acme", old_run.thread_id, assistant, request, old_run.id
                ),
                repo.resume_run_with_pending_steering(
                    "acme", old_run.thread_id, assistant, request, old_run.id
                ),
                return_exceptions=True,
            )
            successes = [r for r in results if not isinstance(r, BaseException)]
            failures = [r for r in results if isinstance(r, BaseException)]
            self.assertEqual(len(successes), 1)
            self.assertEqual(len(failures), 1)
            self.assertIsInstance(failures[0], RunResumeConflictError)

            winner_run, _ = successes[0]
            final_old = await repo.get_run("acme", old_run.id)
            self.assertEqual(final_old.metadata.get("superseded_by_run_id"), winner_run.id)

            # The losing attempt must not have been silently allowed to
            # retry and create a second Run either.
            with self.assertRaises(RunResumeConflictError):
                await repo.resume_run_with_pending_steering(
                    "acme", old_run.thread_id, assistant, request, old_run.id
                )

        asyncio.run(scenario())

    def test_transferred_and_new_steering_sequences_never_collide(self) -> None:
        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create(), graph_version="1"
            )
            old_run = await repo.create_run("acme", None, assistant, _run_create(assistant.id))
            await repo.submit_steering("acme", old_run.id, kind="user_input", payload={"m": 1})
            await repo.submit_steering("acme", old_run.id, kind="user_input", payload={"m": 2})
            await repo.finish_run("acme", old_run.id, "paused", output={})

            from lingxigraph.server.models import RunCreate

            request = RunCreate(assistant_id=assistant.id, resume=1)
            new_run, transferred = await repo.resume_run_with_pending_steering(
                "acme", old_run.thread_id, assistant, request, old_run.id
            )
            self.assertEqual([event.sequence for event in transferred], [1, 2])
            # A concurrent-in-spirit ordinary /steer against the resumed
            # run must land on the next free sequence, never colliding
            # with the migrated ones.
            extra, _ = await repo.submit_steering(
                "acme", new_run.id, kind="user_input", payload={"m": 3}
            )
            self.assertEqual(extra.sequence, 3)
            all_events = await repo.list_steering("acme", new_run.id)
            self.assertEqual(sorted(event.sequence for event in all_events), [1, 2, 3])

        asyncio.run(scenario())


class DurableAckOrderingTests(unittest.TestCase):
    """Issue #16 PR #17 review point 2: the local consumption log must only
    be acked *after* the durable commit (steering status + lifecycle event)
    has actually succeeded -- never popped destructively beforehand."""

    def test_channel_peek_and_ack_never_lose_entries_appended_concurrently(self) -> None:
        channel = SteeringChannel("run-x")
        channel.submit(kind="user_input", payload={"i": 1})
        channel.drain(node="a")
        first_batch = channel.peek_consumed()
        self.assertEqual(len(first_batch), 1)
        # Simulate a second drain happening on another task before the
        # first batch is acked -- its entry must survive the ack below.
        channel.submit(kind="user_input", payload={"i": 2})
        channel.drain(node="b")
        channel.ack_consumed(entry.event.id for entry in first_batch)
        remaining = channel.peek_consumed()
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0].node, "b")

    def test_worker_retries_a_transient_durable_commit_failure_without_losing_it(
        self,
    ) -> None:
        from lingxigraph.server.registry import GraphRegistry
        from lingxigraph.server.worker import Worker
        from lingxigraph.steering import SteeringEvent

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create(), graph_version="1"
            )
            run = await repo.create_run("acme", None, assistant, _run_create(assistant.id))
            accepted, _ = await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"m": 1}
            )

            channel = SteeringChannel(run.id, owned_by_executor=False)
            channel.ingest(
                SteeringEvent(
                    id=accepted.id,
                    run_id=run.id,
                    sequence=accepted.sequence,
                    kind=accepted.kind,
                    payload=accepted.payload,
                    metadata=accepted.metadata,
                    created_at=accepted.created_at,
                )
            )
            # Simulate the graph safe point actually draining it -- this is
            # what populates the channel's local consumption log that
            # ``_sync_steering_out`` must not lose.
            channel.drain(node="the_node", task_id="task-0")

            class FakeGraph:
                def get_steering_channel(self, run_id: str):
                    return channel

            worker = Worker(GraphRegistry({}), repo)

            calls = {"n": 0}
            real_commit = repo.commit_steering_consumptions

            async def flaky_commit(tenant_id, run_id, worker_id, attempt, consumptions):
                calls["n"] += 1
                if calls["n"] == 1:
                    raise ConnectionError("transient database hiccup")
                return await real_commit(tenant_id, run_id, consumptions)

            repo.commit_steering_consumptions_if_owned = flaky_commit  # type: ignore[method-assign]

            # First sync hits the injected transient failure: nothing may
            # be lost -- the DB row must still read pending and the
            # channel must still hold the unacked consumption record.
            await worker._sync_steering_out(run, FakeGraph())
            still_pending = await repo.list_pending_steering("acme", run.id)
            self.assertEqual(len(still_pending), 1)
            self.assertEqual(len(channel.peek_consumed()), 1)
            consumed_events = [
                event
                for event in await repo.list_events("acme", run.id)
                if event.kind == "run.steer.consumed"
            ]
            self.assertEqual(consumed_events, [])

            # A later safe point (next heartbeat tick) retries and this
            # time the durable commit succeeds -- the consumption is fully
            # recorded exactly once, not lost and not duplicated.
            await worker._sync_steering_out(run, FakeGraph())
            self.assertEqual(calls["n"], 2)
            pending_after = await repo.list_pending_steering("acme", run.id)
            self.assertEqual(pending_after, [])
            self.assertEqual(channel.peek_consumed(), ())
            all_steering = await repo.list_steering("acme", run.id)
            self.assertEqual(all_steering[0].status, "consumed")
            consumed_events_after = [
                event
                for event in await repo.list_events("acme", run.id)
                if event.kind == "run.steer.consumed"
            ]
            self.assertEqual(len(consumed_events_after), 1)

        asyncio.run(scenario())

    def test_final_flush_failure_never_reports_success_with_steering_lost(self) -> None:
        """Issue #16 PR #17 review round 4, point 3 (round 5, point 1/2): a
        durable-commit failure on the *final* flush -- with the heartbeat
        already cancelled and no further safe point within this delivery
        attempt -- must not leave the run reported succeeded/paused while
        the DB steering row is silently stranded ``pending`` forever, and
        must not discard the run's intended outcome before the flush is
        known to have succeeded. This goes through the real
        ``Worker._execute()`` finalization path (not an isolated call to
        ``_sync_steering_out``).

        ``_final_steering_flush`` now retries for as long as it holds the
        lease rather than giving up after a fixed number of attempts (see
        its docstring), so this simulates the one case it does give up in
        -- the worker itself starting to drain mid-retry -- by setting
        ``worker._stop`` as a side effect of the first failed commit.
        """

        from lingxigraph.server.worker import Worker

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create_named("steerable"), graph_version="1"
            )
            run = await repo.create_run(
                "acme", None, assistant, _run_create(assistant.id, {"ticks": 0, "drained": []})
            )
            await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"message": "stop"}
            )

            worker = Worker(make_registry(), repo)

            # The heartbeat loop is irrelevant to what this test is
            # checking (it only needs the steering that was already
            # durably accepted *before* the run was claimed, which
            # ``_execute`` syncs in up front, before the heartbeat task is
            # even created) -- replace it with an inert coroutine so the
            # test isn't also exercising unrelated heartbeat/event-bus
            # concurrency while it deliberately holds the flush retry loop
            # open.
            async def inert_heartbeat(run, token, graph):
                await asyncio.Event().wait()

            worker._heartbeat = inert_heartbeat  # type: ignore[method-assign]

            async def always_fails(tenant_id, run_id, worker_id, attempt, consumptions):
                # Simulate the worker starting to drain mid-retry so the
                # flush's otherwise-unbounded retry loop bails immediately
                # instead of looping (and sleeping) forever in this test.
                worker._stop.set()
                raise ConnectionError("transient database hiccup on final flush")

            repo.commit_steering_consumptions_if_owned = always_fails  # type: ignore[method-assign]

            claimed = await worker.run_once()
            self.assertTrue(claimed)

            after_failed_flush = await repo.get_run("acme", run.id)
            status = (
                after_failed_flush.status.value
                if hasattr(after_failed_flush.status, "value")
                else after_failed_flush.status
            )
            # Must never be reported succeeded/paused while the durable
            # steering commit never actually happened.
            self.assertEqual(status, "pending")
            still_pending = await repo.list_pending_steering("acme", run.id)
            self.assertEqual(len(still_pending), 1)
            consumed_events = [
                event
                for event in await repo.list_events("acme", run.id)
                if event.kind == "run.steer.consumed"
            ]
            self.assertEqual(consumed_events, [])

            # Recovery: restore the durable commit, un-drain the worker,
            # and let the run be redelivered. It must eventually succeed
            # with exactly one ``run.steer.consumed`` event -- not lost,
            # not duplicated -- confirming the still-``pending`` DB row
            # (not any in-process channel state) is what a fresh delivery
            # attempt's brand new graph/channel actually recovers from.
            del repo.commit_steering_consumptions_if_owned  # type: ignore[attr-defined]
            worker._stop = asyncio.Event()
            claimed_again = await worker.run_once()
            self.assertTrue(claimed_again)

            finished = await repo.get_run("acme", run.id)
            finished_status = (
                finished.status.value if hasattr(finished.status, "value") else finished.status
            )
            self.assertEqual(finished_status, "succeeded")
            self.assertIn("stop", finished.output["drained"])
            self.assertEqual(await repo.list_pending_steering("acme", run.id), [])
            consumed_events_after = [
                event
                for event in await repo.list_events("acme", run.id)
                if event.kind == "run.steer.consumed"
            ]
            self.assertEqual(len(consumed_events_after), 1)

        asyncio.run(scenario())

    def test_concurrent_observers_never_see_a_transient_terminal_status_during_final_flush(
        self,
    ) -> None:
        """Issue #16 PR #17 review round 5, point 1 (BLOCKER): a run's
        terminal/paused status must never become externally observable
        (via ``GET``/``join``/SSE) before the final steering flush has
        durably succeeded -- and, symmetrically, once it *does* become
        observable it must never move back to non-terminal.

        This holds the final flush open with a gate the test controls,
        continuously polls ``repository.get_run`` (the same read path
        ``GET``/``join``/SSE all resolve to) throughout, and asserts:
        (1) while the flush is blocked, the status is never
        ``succeeded``/``paused``; (2) once ``succeeded`` is first
        observed, every subsequent poll is also ``succeeded`` -- it never
        reverts to ``pending``/``running``."""

        from lingxigraph.server.worker import Worker

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create_named("steerable"), graph_version="1"
            )
            run = await repo.create_run(
                "acme", None, assistant, _run_create(assistant.id, {"ticks": 0, "drained": []})
            )
            await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"message": "stop"}
            )

            worker = Worker(make_registry(), repo)

            async def inert_heartbeat(run, token, graph):
                await asyncio.Event().wait()

            worker._heartbeat = inert_heartbeat  # type: ignore[method-assign]

            gate = asyncio.Event()
            flush_entered = asyncio.Event()
            real_commit = repo.commit_steering_consumptions

            async def gated_commit(tenant_id, run_id, worker_id, attempt, consumptions):
                flush_entered.set()
                await gate.wait()
                return await real_commit(tenant_id, run_id, consumptions)

            repo.commit_steering_consumptions_if_owned = gated_commit  # type: ignore[method-assign]

            observed_statuses: list[str] = []
            stop_observing = asyncio.Event()

            async def observer() -> None:
                while not stop_observing.is_set():
                    current = await repo.get_run("acme", run.id)
                    status = current.status.value if hasattr(current.status, "value") else current.status
                    observed_statuses.append(status)
                    await asyncio.sleep(0.005)

            observer_task = asyncio.ensure_future(observer())
            execute_task = asyncio.ensure_future(worker.run_once())

            # Let the run reach and block inside the final flush before
            # releasing it -- this is the exact window the round 5 review
            # identified as unsafe in the pre-fix code.
            await asyncio.wait_for(flush_entered.wait(), timeout=5)
            # A few more observer ticks while still gated shut.
            await asyncio.sleep(0.05)
            self.assertNotIn("succeeded", observed_statuses)
            self.assertNotIn("paused", observed_statuses)

            gate.set()
            await asyncio.wait_for(execute_task, timeout=5)
            # A few more polls after completion to catch any regression
            # back out of the terminal state.
            await asyncio.sleep(0.05)
            stop_observing.set()
            await observer_task

            self.assertIn("succeeded", observed_statuses)
            first_succeeded = observed_statuses.index("succeeded")
            # Monotonic: nothing after the first "succeeded" observation
            # is ever anything else.
            self.assertTrue(all(s == "succeeded" for s in observed_statuses[first_succeeded:]))
            self.assertNotIn("paused", observed_statuses)

            final = await repo.get_run("acme", run.id)
            final_status = final.status.value if hasattr(final.status, "value") else final.status
            self.assertEqual(final_status, "succeeded")
            self.assertIn("stop", final.output["drained"])

        asyncio.run(scenario())

    def test_redelivery_after_flush_failure_uses_a_genuinely_new_graph_instance(
        self,
    ) -> None:
        """Issue #16 PR #17 review round 5, point 2: the previous claim that
        "not calling ``forget_steering()`` leaves the consumption log
        available for the next delivery" does not hold, because
        ``with_runtime()`` builds a *brand new* ``CompiledStateGraph`` (and
        therefore a brand new, empty ``_run_steering`` channel map) on
        every call -- the locally-bound ``graph``/channel object from a
        failed delivery attempt is never retained across attempts.

        This test proves recovery does NOT depend on graph/channel object
        identity surviving across attempts: it instruments
        ``GraphRegistry.get`` to hand back a distinct, individually
        identifiable ``CompiledStateGraph`` on each call, confirms the
        second ``run_once()`` really did receive a different graph object
        (not a hand-rolled second flush call on the first attempt's
        object), and confirms the run still recovers correctly -- because
        recovery is driven entirely by the still-``pending`` PostgreSQL
        row, not by anything held in memory across the attempt boundary.
        """

        from lingxigraph.server.worker import Worker

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create_named("steerable"), graph_version="1"
            )
            run = await repo.create_run(
                "acme", None, assistant, _run_create(assistant.id, {"ticks": 0, "drained": []})
            )
            await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"message": "stop"}
            )

            registry = make_registry()
            # Keep every bound graph alive for the rest of the test, not
            # just its id(): CPython is free to reuse a freed object's
            # memory address for the next allocation, so comparing ids of
            # objects that were allowed to go out of scope is not a
            # reliable "is this a genuinely distinct instance" check (this
            # aliased in practice on 3.13's GC timing, id()s collided).
            seen_graphs: list[object] = []
            compiled = registry.get("steerable")
            real_with_runtime = compiled.with_runtime

            def tracking_with_runtime(*args: Any, **kwargs: Any):
                bound = real_with_runtime(*args, **kwargs)
                seen_graphs.append(bound)
                return bound

            compiled.with_runtime = tracking_with_runtime  # type: ignore[method-assign]

            worker = Worker(registry, repo)

            async def inert_heartbeat(run, token, graph):
                await asyncio.Event().wait()

            worker._heartbeat = inert_heartbeat  # type: ignore[method-assign]

            real_commit = repo.commit_steering_consumptions
            fail_once = {"done": False}

            async def fail_first_attempt_only(tenant_id, run_id, worker_id, attempt, consumptions):
                if not fail_once["done"]:
                    fail_once["done"] = True
                    worker._stop.set()
                    raise ConnectionError("transient database hiccup on final flush")
                return await real_commit(tenant_id, run_id, consumptions)

            repo.commit_steering_consumptions_if_owned = fail_first_attempt_only  # type: ignore[method-assign]

            claimed = await worker.run_once()
            self.assertTrue(claimed)
            after_first_attempt = await repo.get_run("acme", run.id)
            status = (
                after_first_attempt.status.value
                if hasattr(after_first_attempt.status, "value")
                else after_first_attempt.status
            )
            self.assertEqual(status, "pending")
            self.assertEqual(len(seen_graphs), 1)

            worker._stop = asyncio.Event()
            claimed_again = await worker.run_once()
            self.assertTrue(claimed_again)

            # The critical assertion: recovery went through a *second*,
            # distinct ``with_runtime()`` call -- a genuinely new graph
            # instance, exactly as a real redelivery (by this worker or a
            # different one entirely) would -- and it still worked,
            # because the recovery source of truth is the ``pending`` DB
            # row, never anything held on the first attempt's discarded
            # graph/channel.
            self.assertEqual(len(seen_graphs), 2)
            self.assertIsNot(seen_graphs[0], seen_graphs[1])

            finished = await repo.get_run("acme", run.id)
            finished_status = (
                finished.status.value if hasattr(finished.status, "value") else finished.status
            )
            self.assertEqual(finished_status, "succeeded")
            self.assertIn("stop", finished.output["drained"])
            self.assertEqual(await repo.list_pending_steering("acme", run.id), [])
            consumed_events = [
                event
                for event in await repo.list_events("acme", run.id)
                if event.kind == "run.steer.consumed"
            ]
            self.assertEqual(len(consumed_events), 1)

        asyncio.run(scenario())


class FinalizationFencingTests(unittest.TestCase):
    """Issue #16 PR #17 review round 6 (BLOCKERs 1-3): all writes a worker
    makes during finalization must be fenced on its own
    ``(lease_owner, attempt)``, cancellation must win over a stale
    non-cancel intent inside that same fenced decision, and new steering
    admission must close before the final flush starts."""

    def test_stale_worker_cannot_revert_a_new_owners_lease_or_status(self) -> None:
        """Round 6, point 1 (BLOCKER): worker A claims with a very short
        lease and blocks inside the final flush; once the lease expires,
        worker B claims the same run; releasing A's block must not let A's
        now-stale finalization commit touch B's run at all."""

        from lingxigraph.server.worker import Worker

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create_named("steerable"), graph_version="1"
            )
            run = await repo.create_run(
                "acme", None, assistant, _run_create(assistant.id, {"ticks": 0, "drained": []})
            )
            await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"message": "stop"}
            )

            worker_a = Worker(
                make_registry(), repo, worker_id="worker-a", lease_seconds=1, heartbeat_seconds=10
            )

            async def inert_heartbeat(run, token, graph):
                await asyncio.Event().wait()

            worker_a._heartbeat = inert_heartbeat  # type: ignore[method-assign]

            gate = asyncio.Event()
            flush_entered = asyncio.Event()
            real_commit = repo.commit_steering_consumptions

            async def gated_commit(tenant_id, run_id, worker_id, attempt, consumptions):
                flush_entered.set()
                await gate.wait()
                return await real_commit(tenant_id, run_id, consumptions)

            repo.commit_steering_consumptions_if_owned = gated_commit  # type: ignore[method-assign]

            execute_task = asyncio.ensure_future(worker_a.run_once())
            await asyncio.wait_for(flush_entered.wait(), timeout=5)

            # Let A's 1-second lease actually expire without being renewed
            # (its heartbeat_seconds=10 means the independent finalization
            # renewal loop will not have ticked yet).
            await asyncio.sleep(1.3)

            claimed_b = await repo.claim_run("worker-b", lease_seconds=30)
            self.assertIsNotNone(claimed_b)
            self.assertEqual(claimed_b.id, run.id)
            self.assertEqual(claimed_b.lease_owner, "worker-b")
            self.assertEqual(claimed_b.attempt, 2)

            # Now release A's blocked flush -- its commit succeeds, but its
            # subsequent fenced finalize write must be refused because it
            # no longer owns the lease/attempt.
            gate.set()
            await asyncio.wait_for(execute_task, timeout=5)

            after = await repo.get_run("acme", run.id)
            self.assertEqual(after.lease_owner, "worker-b")
            self.assertEqual(after.attempt, 2)
            status = after.status.value if hasattr(after.status, "value") else after.status
            self.assertIn(status, ("running", "cancelling"))
            self.assertNotEqual(status, "pending")
            self.assertNotEqual(status, "succeeded")

        asyncio.run(scenario())

    def test_stale_worker_cannot_revert_new_owner_via_flush_failure_retry_path(self) -> None:
        """Round 6, point 1 (BLOCKER), the mirror case: A's final flush
        never succeeds at all (durable commit keeps failing) and A falls
        back to its retry/dead-letter path -- that path's writes must also
        be fenced, so A cannot reset B's actively-running attempt back to
        ``pending``."""

        from lingxigraph.server.worker import Worker

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create_named("steerable"), graph_version="1"
            )
            run = await repo.create_run(
                "acme", None, assistant, _run_create(assistant.id, {"ticks": 0, "drained": []})
            )
            await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"message": "stop"}
            )

            worker_a = Worker(
                make_registry(), repo, worker_id="worker-a", lease_seconds=1, heartbeat_seconds=10
            )

            async def inert_heartbeat(run, token, graph):
                await asyncio.Event().wait()

            worker_a._heartbeat = inert_heartbeat  # type: ignore[method-assign]

            entered = asyncio.Event()
            release = asyncio.Event()

            async def always_fails(tenant_id, run_id, worker_id, attempt, consumptions):
                entered.set()
                await release.wait()
                raise ConnectionError("simulated persistent database outage")

            repo.commit_steering_consumptions_if_owned = always_fails  # type: ignore[method-assign]

            execute_task = asyncio.ensure_future(worker_a.run_once())
            await asyncio.wait_for(entered.wait(), timeout=5)
            await asyncio.sleep(1.3)

            claimed_b = await repo.claim_run("worker-b", lease_seconds=30)
            self.assertIsNotNone(claimed_b)
            self.assertEqual(claimed_b.attempt, 2)

            release.set()
            await asyncio.wait_for(execute_task, timeout=5)

            after = await repo.get_run("acme", run.id)
            self.assertEqual(after.lease_owner, "worker-b")
            self.assertEqual(after.attempt, 2)
            status = after.status.value if hasattr(after.status, "value") else after.status
            self.assertIn(status, ("running", "cancelling"))

        asyncio.run(scenario())

    def test_cancel_always_wins_over_a_stale_success_intent_during_finalization(
        self,
    ) -> None:
        """Round 6, point 2 (BLOCKER): the graph has already computed a
        SUCCEEDED intent and the heartbeat is cancelled before a client's
        cancel request lands; the fenced finalize write must observe the
        run is ``cancelling`` and coerce the outcome to ``cancelled``,
        never letting the stale ``succeeded`` intent win."""

        from lingxigraph.server.worker import Worker

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create_named("double"), graph_version="1"
            )
            run = await repo.create_run(
                "acme", None, assistant, _run_create(assistant.id, {"ticks": 0})
            )

            worker = Worker(make_registry_double(), repo, worker_id="worker-a")

            async def inert_heartbeat(run, token, graph):
                await asyncio.Event().wait()

            worker._heartbeat = inert_heartbeat  # type: ignore[method-assign]

            # The graph here has no steering to drain, so the final flush
            # itself is a no-op -- gate the fenced finalize write instead,
            # which is exactly where round 6 point 2 requires the
            # cancelling re-check to happen.
            gate = asyncio.Event()
            commit_entered = asyncio.Event()
            # Issue #16 PR #17 review round 9, point 2 (BLOCKER): a
            # terminal ``finish`` intent now finalizes via the merged
            # ``finalize_run_with_steering_disposition_if_owned`` -- gate
            # that instead of the now-bypassed ``finish_run_if_owned``.
            real_finalize = repo.finalize_run_with_steering_disposition_if_owned

            async def gated_finalize(*args: Any, **kwargs: Any):
                commit_entered.set()
                await gate.wait()
                return await real_finalize(*args, **kwargs)

            repo.finalize_run_with_steering_disposition_if_owned = gated_finalize  # type: ignore[method-assign]

            execute_task = asyncio.ensure_future(worker.run_once())
            await asyncio.wait_for(commit_entered.wait(), timeout=5)

            # Cancellation arrives while the run still reads
            # running/cancelling (finalization has not committed yet).
            cancelled = await repo.request_cancel("acme", run.id)
            self.assertTrue(cancelled)
            mid_status = await repo.get_run("acme", run.id)
            self.assertEqual(
                mid_status.status.value
                if hasattr(mid_status.status, "value")
                else mid_status.status,
                "cancelling",
            )

            gate.set()
            await asyncio.wait_for(execute_task, timeout=5)

            final = await repo.get_run("acme", run.id)
            final_status = (
                final.status.value if hasattr(final.status, "value") else final.status
            )
            self.assertEqual(final_status, "cancelled")
            self.assertNotEqual(final_status, "succeeded")

        asyncio.run(scenario())

    def test_stale_worker_cannot_commit_steering_consumptions_after_lease_takeover(
        self,
    ) -> None:
        """Issue #16 PR #17 review round 7, point 2 (BLOCKER).

        Worker A drains steering event E during attempt N and blocks
        inside the durable consumption commit. Its lease expires; worker B
        claims the same run (attempt N+1). Releasing A's blocked commit
        must be rejected outright -- A's commit must never touch E's
        durable status or emit ``run.steer.consumed`` once it no longer
        owns the lease/attempt, exactly like the already-fenced
        ``finish_run_if_owned``/``retry_run_if_owned`` writes.
        """

        from lingxigraph.steering import SteeringConsumption, SteeringEvent

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create_named("double"), graph_version="1"
            )
            run = await repo.create_run("acme", None, assistant, _run_create(assistant.id))
            accepted, _ = await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"message": "stop"}
            )

            claimed_a = await repo.claim_run("worker-a", lease_seconds=1)
            assert claimed_a is not None
            self.assertEqual(claimed_a.attempt, 1)

            steering_event = SteeringEvent(
                id=accepted.id,
                run_id=run.id,
                sequence=accepted.sequence,
                kind=accepted.kind,
                payload=accepted.payload,
                metadata=accepted.metadata,
                created_at=accepted.created_at,
            )
            consumption = SteeringConsumption(
                event=steering_event,
                consumed_at=steering_event.created_at,
                node="n",
                namespace=(),
                task_id="t-0",
            )

            # A's lease expires without renewal; B claims the same run at
            # a new attempt while A is (conceptually) still "blocked" --
            # this test drives the two calls sequentially, which exercises
            # the exact same fencing check the concurrent version would.
            await asyncio.sleep(1.2)
            claimed_b = await repo.claim_run("worker-b", lease_seconds=30)
            assert claimed_b is not None
            self.assertEqual(claimed_b.attempt, 2)

            # A's stale, now-recovering commit must be rejected entirely --
            # nothing written, nothing acked.
            result = await repo.commit_steering_consumptions_if_owned(
                "acme", run.id, "worker-a", claimed_a.attempt, [consumption]
            )
            self.assertIsNone(result)

            # E's durable status must be untouched -- still pending, no
            # ``run.steer.consumed`` lifecycle event.
            still_pending = await repo.list_pending_steering("acme", run.id)
            self.assertEqual([event.id for event in still_pending], [accepted.id])
            consumed_events = [
                event
                for event in await repo.list_events("acme", run.id)
                if event.kind == "run.steer.consumed"
            ]
            self.assertEqual(consumed_events, [])

            # B, the actual current owner, can commit it normally.
            stored = await repo.commit_steering_consumptions_if_owned(
                "acme", run.id, "worker-b", claimed_b.attempt, [consumption]
            )
            self.assertEqual(len(stored), 1)
            self.assertEqual(await repo.list_pending_steering("acme", run.id), [])

        asyncio.run(scenario())


class SteeringClosedGateTests(unittest.TestCase):
    """Issue #16 PR #17 review round 6, point 3 (BLOCKER): once graph
    execution ends there is no safe point left for a newly accepted
    steering event to ever be consumed, even though the Run row may still
    read ``running``/``cancelling`` while the final flush is in flight."""

    def test_steer_during_finalization_window_gets_a_stable_error_not_a_stuck_pending_row(
        self,
    ) -> None:
        from lingxigraph.errors import RunFinalizingError
        from lingxigraph.server.worker import Worker

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create_named("double"), graph_version="1"
            )
            run = await repo.create_run(
                "acme", None, assistant, _run_create(assistant.id, {"ticks": 0})
            )

            worker = Worker(make_registry_double(), repo, worker_id="worker-a")

            async def inert_heartbeat(run, token, graph):
                await asyncio.Event().wait()

            worker._heartbeat = inert_heartbeat  # type: ignore[method-assign]

            gate = asyncio.Event()
            commit_entered = asyncio.Event()
            # Issue #16 PR #17 review round 9, point 2 (BLOCKER): a
            # terminal ``finish`` intent now finalizes via the merged
            # ``finalize_run_with_steering_disposition_if_owned`` -- gate
            # that instead of the now-bypassed ``finish_run_if_owned``.
            real_finalize = repo.finalize_run_with_steering_disposition_if_owned

            async def gated_finalize(*args: Any, **kwargs: Any):
                commit_entered.set()
                await gate.wait()
                return await real_finalize(*args, **kwargs)

            repo.finalize_run_with_steering_disposition_if_owned = gated_finalize  # type: ignore[method-assign]

            execute_task = asyncio.ensure_future(worker.run_once())
            await asyncio.wait_for(commit_entered.wait(), timeout=5)

            # The graph has already reached END (no safe point left) but
            # the run row still reads running/cancelling -- this is
            # exactly the window round 6 point 3 is about.
            still_active = await repo.get_run("acme", run.id)
            status = (
                still_active.status.value
                if hasattr(still_active.status, "value")
                else still_active.status
            )
            self.assertIn(status, ("running", "cancelling"))
            self.assertTrue(still_active.steering_closed)

            with self.assertRaises(RunFinalizingError):
                await repo.submit_steering(
                    "acme", run.id, kind="user_input", payload={"message": "too late"}
                )

            gate.set()
            await asyncio.wait_for(execute_task, timeout=5)

            # Never a terminal run with a pending steering inbox nobody
            # will ever consume.
            self.assertEqual(await repo.list_pending_steering("acme", run.id), [])
            final = await repo.get_run("acme", run.id)
            final_status = (
                final.status.value if hasattr(final.status, "value") else final.status
            )
            self.assertEqual(final_status, "succeeded")

        asyncio.run(scenario())

    def test_steering_closed_gate_resets_on_a_fresh_delivery_attempt(self) -> None:
        """A gate closed by an earlier, now-abandoned attempt must not
        leak into the next delivery attempt's fresh claim -- otherwise a
        perfectly ordinary retried run would spuriously reject all
        ``/steer`` calls forever."""

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create_named("double"), graph_version="1"
            )
            run = await repo.create_run("acme", None, assistant, _run_create(assistant.id))
            claimed = await repo.claim_run("worker-a", lease_seconds=1)
            assert claimed is not None
            closed = await repo.close_steering("acme", run.id, "worker-a", claimed.attempt)
            self.assertTrue(closed)

            await asyncio.sleep(1.2)
            reclaimed = await repo.claim_run("worker-b", lease_seconds=30)
            assert reclaimed is not None
            self.assertEqual(reclaimed.attempt, 2)
            self.assertFalse(reclaimed.steering_closed)

            # Steering must be accepted normally under the new attempt.
            accepted, created = await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"message": "hello"}
            )
            self.assertTrue(created)
            self.assertEqual(accepted.status, "pending")

        asyncio.run(scenario())

    def test_steer_that_wins_the_lock_race_before_gate_close_is_superseded_not_rerun(
        self,
    ) -> None:
        """Issue #16 PR #17 review round 8, point 1 (BLOCKER) -- CHOSEN FIX:
        "seal admission at the safe boundary + supersede late events".

        A ``/steer`` call can win the run row's lock *before*
        ``close_steering()`` does, even though the graph has already
        finished and the heartbeat has already stopped -- "landed before
        the gate closed" is not the same as "landed before the graph's
        last safe point". This artificially blocks ``close_steering()``,
        completes a ``submit_steering()`` while it is blocked (simulating
        the race winner), then releases the block. Round 7's fix forced an
        indefinite retry loop onto this run -- but the ``double`` graph
        below never calls ``drain_steering()`` at all, so forcing a rerun
        penalized a graph that is correctly, deliberately ignoring
        steering. The run must instead finalize normally on this very
        attempt, and the late-landing steering event must end up with a
        durable ``superseded`` disposition and matching
        ``run.steer.superseded`` lifecycle event -- never silently
        dropped, never forcing a rerun.
        """

        from lingxigraph.server.worker import Worker

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create_named("double"), graph_version="1"
            )
            run = await repo.create_run(
                "acme", None, assistant, _run_create(assistant.id, {"ticks": 0})
            )

            worker = Worker(make_registry_double(), repo, worker_id="worker-a")

            async def inert_heartbeat(run, token, graph):
                await asyncio.Event().wait()

            worker._heartbeat = inert_heartbeat  # type: ignore[method-assign]

            gate = asyncio.Event()
            entered = asyncio.Event()
            real_close = repo.close_steering

            async def blocked_close(tenant_id, run_id, worker_id, attempt):
                entered.set()
                await gate.wait()
                return await real_close(tenant_id, run_id, worker_id, attempt)

            repo.close_steering = blocked_close  # type: ignore[method-assign]

            execute_task = asyncio.ensure_future(worker.run_once())
            await asyncio.wait_for(entered.wait(), timeout=5)

            # The graph has already reached END and the heartbeat is
            # already cancelled -- there is no safe point left -- but the
            # gate has not closed yet. A concurrent ``/steer`` wins the
            # run lock first and durably commits.
            accepted, created = await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"message": "too late"}
            )
            self.assertTrue(created)

            gate.set()
            await asyncio.wait_for(execute_task, timeout=5)

            final = await repo.get_run("acme", run.id)
            final_status = (
                final.status.value if hasattr(final.status, "value") else final.status
            )
            # The run finalizes normally on this attempt -- never forced
            # into a retry loop just because a steering event existed that
            # the graph never looked at.
            self.assertEqual(final_status, "succeeded")

            # The late-landing steering event is durably superseded, not
            # left pending, not silently dropped.
            still_pending = await repo.list_pending_steering("acme", run.id)
            self.assertEqual(still_pending, [])
            all_steering = await repo.list_steering("acme", run.id)
            self.assertEqual(len(all_steering), 1)
            self.assertEqual(all_steering[0].id, accepted.id)
            self.assertEqual(all_steering[0].status, "superseded")

            # A durable, observable run.steer.superseded lifecycle event
            # was recorded for it -- not the resume-transfer reason, since
            # this is the late-boundary-race disposition.
            events = await repo.list_events("acme", run.id)
            superseded_events = [e for e in events if e.kind == "run.steer.superseded"]
            self.assertEqual(len(superseded_events), 1)
            self.assertEqual(superseded_events[0].data["steering_event_id"], accepted.id)
            self.assertEqual(superseded_events[0].data["reason"], "unconsumed_at_final_boundary")
            self.assertIsNone(superseded_events[0].data["superseded_by_run_id"])

        asyncio.run(scenario())

    def test_graph_that_never_drains_steering_still_finalizes_on_first_attempt(
        self,
    ) -> None:
        """Issue #16 PR #17 review round 8, point 1 (BLOCKER) required
        regression coverage (a): a graph that never calls
        ``drain_steering()`` at all, receiving one ordinary steering
        event, must still finalize successfully on its first attempt --
        no forced rerun just because steering happened to exist. This is
        the deliberate-ignore case explicitly allowed by issue #16 ("a
        node may call drain_steering() once, in a loop, or not at all")."""

        from lingxigraph.server.worker import Worker

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create_named("double"), graph_version="1"
            )
            run = await repo.create_run(
                "acme", None, assistant, _run_create(assistant.id, {"ticks": 0})
            )
            accepted, created = await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"message": "ignored"}
            )
            self.assertTrue(created)

            worker = Worker(make_registry_double(), repo, worker_id="worker-a")
            claimed = await worker.run_once()
            self.assertTrue(claimed)

            final = await repo.get_run("acme", run.id)
            final_status = (
                final.status.value if hasattr(final.status, "value") else final.status
            )
            self.assertEqual(final_status, "succeeded")
            self.assertEqual(final.attempt, 1)

            all_steering = await repo.list_steering("acme", run.id)
            self.assertEqual(len(all_steering), 1)
            self.assertEqual(all_steering[0].status, "superseded")
            events = await repo.list_events("acme", run.id)
            superseded_events = [e for e in events if e.kind == "run.steer.superseded"]
            self.assertEqual(len(superseded_events), 1)
            self.assertEqual(superseded_events[0].data["steering_event_id"], accepted.id)

        asyncio.run(scenario())


class RetryCommitCancelWinsEventTests(unittest.TestCase):
    """Issue #16 PR #17 review round 8, point 2 (BLOCKER).

    ``retry_run_if_owned`` already correctly coerces the transition to
    ``cancelled`` when the run is ``cancelling`` at commit time -- it must
    never be sent back to ``pending``. But the Worker used to only check
    ``updated is not None`` before unconditionally appending a
    ``worker_retrying`` event, so a durable "retrying" event could be
    appended even though the actual write coerced the run to
    ``cancelled``. These tests prove the Worker now branches on the
    *actual* resulting status for both retry call sites.
    """

    def test_retry_or_dead_letter_does_not_emit_worker_retrying_when_cancel_wins(self) -> None:
        from lingxigraph.server.worker import Worker

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create_named("double"), graph_version="1"
            )
            run = await repo.create_run(
                "acme", None, assistant, _run_create(assistant.id, {"ticks": 0})
            )
            claimed = await repo.claim_run("worker-a", lease_seconds=30)
            assert claimed is not None
            self.assertTrue(await repo.request_cancel("acme", run.id))
            cancelling = await repo.get_run("acme", run.id)
            self.assertEqual(cancelling.status, "cancelling")

            worker = Worker(make_registry_double(), repo, worker_id="worker-a")
            await worker._retry_or_dead_letter(
                claimed, {"code": "delivery_retry", "message": "transient"}
            )

            final = await repo.get_run("acme", run.id)
            self.assertEqual(final.status, "cancelled")
            events = await repo.list_events("acme", run.id)
            self.assertEqual([e for e in events if e.kind == "worker_retrying"], [])

        asyncio.run(scenario())

    def test_commit_intent_retry_does_not_emit_worker_retrying_when_cancel_wins(self) -> None:
        from lingxigraph.server.worker import Worker

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create_named("double"), graph_version="1"
            )
            run = await repo.create_run(
                "acme", None, assistant, _run_create(assistant.id, {"ticks": 0})
            )
            claimed = await repo.claim_run("worker-a", lease_seconds=30)
            assert claimed is not None
            self.assertTrue(await repo.request_cancel("acme", run.id))

            worker = Worker(make_registry_double(), repo, worker_id="worker-a")
            intent = Worker._Intent(
                kind="retry",
                status=RunStatus.PENDING,
                error={"code": "delivery_retry", "message": "transient"},
            )
            committed = await worker._commit_intent(claimed, intent)
            self.assertTrue(committed)

            final = await repo.get_run("acme", run.id)
            self.assertEqual(final.status, "cancelled")
            events = await repo.list_events("acme", run.id)
            self.assertEqual([e for e in events if e.kind == "worker_retrying"], [])

        asyncio.run(scenario())

    def test_ordinary_retry_still_emits_worker_retrying_when_pending(self) -> None:
        from lingxigraph.server.worker import Worker

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create_named("double"), graph_version="1"
            )
            run = await repo.create_run(
                "acme", None, assistant, _run_create(assistant.id, {"ticks": 0})
            )
            claimed = await repo.claim_run("worker-a", lease_seconds=30)
            assert claimed is not None

            worker = Worker(make_registry_double(), repo, worker_id="worker-a")
            await worker._retry_or_dead_letter(
                claimed, {"code": "delivery_retry", "message": "transient"}
            )

            final = await repo.get_run("acme", run.id)
            self.assertEqual(final.status, "pending")
            events = await repo.list_events("acme", run.id)
            self.assertEqual(len([e for e in events if e.kind == "worker_retrying"]), 1)

        asyncio.run(scenario())


class RetryWithEventAtomicityTests(unittest.TestCase):
    """Issue #16 PR #17 review round 10, point 1 (BLOCKER).

    ``retry_run_if_owned()`` and the durable ``worker_retrying`` event used
    to be two separate transactions, reopening a TOCTOU window between
    "the run is truly pending" and "the event says it's retrying". These
    tests prove the merged ``retry_run_with_event_if_owned`` closes both
    races described in the review.
    """

    def test_race_a_cancel_in_the_gap_leaves_no_late_worker_retrying_event(self) -> None:
        """A cancel that would have landed *between* the old two writes
        must never let a stale worker append ``worker_retrying`` for a run
        that is durably ``cancelled`` -- because there is no longer a gap
        for it to land in: the status write and the event are the same
        atomic operation."""

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create_named("double"), graph_version="1"
            )
            run = await repo.create_run(
                "acme", None, assistant, _run_create(assistant.id, {"ticks": 0})
            )
            claimed = await repo.claim_run("worker-a", lease_seconds=30)
            assert claimed is not None

            # Cancel arrives before the merged retry transaction runs --
            # the run is already ``cancelling`` when the fenced write
            # executes, so cancel-wins coercion applies inside the same
            # critical section.
            self.assertTrue(await repo.request_cancel("acme", run.id))

            result = await repo.retry_run_with_event_if_owned(
                "acme",
                run.id,
                "worker-a",
                claimed.attempt,
                error={"code": "delivery_retry", "message": "transient"},
                max_attempts=5,
            )
            assert result is not None
            updated, event = result
            self.assertEqual(updated.status, "cancelled")
            self.assertIsNone(event)

            final = await repo.get_run("acme", run.id)
            self.assertEqual(final.status, "cancelled")
            events = await repo.list_events("acme", run.id)
            self.assertEqual([e for e in events if e.kind == "worker_retrying"], [])

        asyncio.run(scenario())

    def test_race_b_worker_retrying_event_precedes_next_attempts_events(self) -> None:
        """Worker A's ``worker_retrying(attempt N)`` event must always be
        durable *before* the run becomes externally visible as
        ``pending`` -- so a worker B that then claims it (attempt N+1) and
        appends its own execution events can never produce an event with a
        lower sequence than ``worker_retrying``. Because the merged method
        makes both writes atomic, this is true by construction; this test
        proves the observable sequencing."""

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create_named("double"), graph_version="1"
            )
            run = await repo.create_run(
                "acme", None, assistant, _run_create(assistant.id, {"ticks": 0})
            )
            claimed = await repo.claim_run("worker-a", lease_seconds=30)
            assert claimed is not None

            result = await repo.retry_run_with_event_if_owned(
                "acme",
                run.id,
                "worker-a",
                claimed.attempt,
                error={"code": "delivery_retry", "message": "transient"},
                max_attempts=5,
            )
            assert result is not None
            updated, event = result
            self.assertEqual(updated.status, "pending")
            assert event is not None
            self.assertEqual(event.kind, "worker_retrying")

            # Worker B now claims the retried run (attempt N+1) and logs
            # its own execution event.
            claimed_b = await repo.claim_run("worker-b", lease_seconds=30)
            assert claimed_b is not None
            self.assertGreater(claimed_b.attempt, claimed.attempt)
            next_event = await repo.append_event(
                "acme", run.id, "node_started", {"attempt": claimed_b.attempt}
            )

            self.assertLess(event.sequence, next_event.sequence)

        asyncio.run(scenario())


class AtomicSteerAcceptedEventTests(unittest.TestCase):
    """Issue #16 PR #17 review round 4, point 2: accepting a steering row
    and recording its ``run.steer.accepted`` lifecycle event must be a
    single atomic, idempotent unit -- a retried request (same
    Idempotency-Key) that finds the steering row already durably created
    must still repair a missing ``run.steer.accepted`` event rather than
    permanently skip it."""

    def test_idempotency_key_retry_repairs_a_missing_accepted_event(self) -> None:
        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create(), graph_version="1"
            )
            run = await repo.create_run("acme", None, assistant, _run_create(assistant.id))

            event, created = await repo.submit_steering(
                "acme",
                run.id,
                kind="user_input",
                payload={"m": 1},
                idempotency_key="key-1",
            )
            self.assertTrue(created)
            accepted = [
                e for e in await repo.list_events("acme", run.id) if e.kind == "run.steer.accepted"
            ]
            self.assertEqual(len(accepted), 1)

            # Simulate the pre-fix failure window: the steering row
            # committed, but the ``run.steer.accepted`` append never
            # happened (e.g. the process crashed / the DB call transiently
            # failed right after the row committed).
            repo._events[("acme", run.id)] = [
                e for e in repo._events[("acme", run.id)] if e.kind != "run.steer.accepted"
            ]
            self.assertEqual(
                [
                    e
                    for e in await repo.list_events("acme", run.id)
                    if e.kind == "run.steer.accepted"
                ],
                [],
            )

            # The client retries with the same Idempotency-Key: the
            # steering row already exists (``created`` is False) but the
            # gap must still be repaired, not permanently skipped.
            retried_event, retried_created = await repo.submit_steering(
                "acme",
                run.id,
                kind="user_input",
                payload={"m": 1},
                idempotency_key="key-1",
            )
            self.assertFalse(retried_created)
            self.assertEqual(retried_event.id, event.id)
            accepted_after = [
                e
                for e in await repo.list_events("acme", run.id)
                if e.kind == "run.steer.accepted"
            ]
            self.assertEqual(len(accepted_after), 1)
            self.assertEqual(accepted_after[0].data["steering_event_id"], event.id)

            # A further retry must not duplicate it either.
            await repo.submit_steering(
                "acme",
                run.id,
                kind="user_input",
                payload={"m": 1},
                idempotency_key="key-1",
            )
            accepted_final = [
                e
                for e in await repo.list_events("acme", run.id)
                if e.kind == "run.steer.accepted"
            ]
            self.assertEqual(len(accepted_final), 1)

        asyncio.run(scenario())

    def test_transfer_records_accepted_event_for_the_new_run_atomically(self) -> None:
        """The paused-transfer path's ``run.steer.accepted`` must land on
        the new run inside the same migration, correlated back to the
        original event via ``source_event_id``/``transferred_from_run_id``."""

        from lingxigraph.server.models import RunCreate

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create(), graph_version="1"
            )
            old_run = await repo.create_run("acme", None, assistant, _run_create(assistant.id))
            original, _ = await repo.submit_steering(
                "acme", old_run.id, kind="user_input", payload={"m": 1}
            )
            await repo.finish_run("acme", old_run.id, "paused", output={})

            new_run, transferred = await repo.resume_run_with_pending_steering(
                "acme", old_run.thread_id, assistant, RunCreate(assistant_id=assistant.id), old_run.id
            )
            self.assertEqual(len(transferred), 1)
            new_events = await repo.list_events("acme", new_run.id)
            accepted = [e for e in new_events if e.kind == "run.steer.accepted"]
            self.assertEqual(len(accepted), 1)
            self.assertEqual(accepted[0].data["steering_event_id"], transferred[0].id)
            self.assertEqual(accepted[0].data["transferred_from_run_id"], old_run.id)
            self.assertEqual(accepted[0].data["source_event_id"], original.id)

        asyncio.run(scenario())


class IdempotencyKeyReplaySafeAcrossAdmissionGatesTests(unittest.TestCase):
    """Issue #16 PR #17 review round 7, point 3.

    A same-Idempotency-Key replay must return the already-existing event
    (id/sequence unchanged), never a fresh 409, regardless of whether the
    run has since gone finalizing/terminal/superseded -- that is the
    entire point of an idempotency key: safely retrying exactly this kind
    of uncertain-outcome window. Only a genuinely *new* key against a
    finalizing/terminal run should still get 409.
    """

    def test_replay_after_finalizing_returns_same_event_not_409(self) -> None:
        from lingxigraph.errors import RunFinalizingError

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create_named("double"), graph_version="1"
            )
            run = await repo.create_run("acme", None, assistant, _run_create(assistant.id))
            claimed = await repo.claim_run("worker-a", lease_seconds=30)
            assert claimed is not None

            original, created = await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"m": 1}, idempotency_key="key-1"
            )
            self.assertTrue(created)

            gate_closed = await repo.close_steering("acme", run.id, "worker-a", claimed.attempt)
            self.assertTrue(gate_closed)

            # Same key, replayed after the run started finalizing: must
            # return the same event, not a 409.
            replayed, replayed_created = await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"m": 1}, idempotency_key="key-1"
            )
            self.assertFalse(replayed_created)
            self.assertEqual(replayed.id, original.id)
            self.assertEqual(replayed.sequence, original.sequence)

            # A genuinely new key against the still-finalizing run must
            # still be rejected -- this is a real new-admission decision.
            with self.assertRaises(RunFinalizingError):
                await repo.submit_steering(
                    "acme", run.id, kind="user_input", payload={"m": 2}, idempotency_key="key-2"
                )

        asyncio.run(scenario())

    def test_replay_after_terminal_returns_same_event_not_409(self) -> None:
        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create_named("double"), graph_version="1"
            )
            run = await repo.create_run("acme", None, assistant, _run_create(assistant.id))

            original, created = await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"m": 1}, idempotency_key="key-1"
            )
            self.assertTrue(created)

            await repo.finish_run("acme", run.id, "succeeded", output={})

            replayed, replayed_created = await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"m": 1}, idempotency_key="key-1"
            )
            self.assertFalse(replayed_created)
            self.assertEqual(replayed.id, original.id)
            self.assertEqual(replayed.sequence, original.sequence)

            with self.assertRaises(RunTerminalError):
                await repo.submit_steering(
                    "acme", run.id, kind="user_input", payload={"m": 2}, idempotency_key="key-2"
                )

        asyncio.run(scenario())


class InMemoryRepositorySteeringEdgeCaseTests(unittest.TestCase):
    """Direct unit coverage of ``InMemoryRepository`` steering branches that
    the HTTP/worker end-to-end tests above don't happen to exercise: unknown
    runs, empty batches, mixed-status transfers, and idempotency-keyed
    transfers."""

    def test_submit_steering_on_unknown_run_raises_key_error(self) -> None:
        async def scenario() -> None:
            repo = InMemoryRepository()
            with self.assertRaises(KeyError):
                await repo.submit_steering("acme", "no-such-run", kind="user_input", payload={})

        asyncio.run(scenario())

    def test_mark_steering_consumed_with_empty_ids_is_a_no_op(self) -> None:
        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create(), graph_version="1"
            )
            run = await repo.create_run("acme", None, assistant, _run_create(assistant.id))
            await repo.submit_steering("acme", run.id, kind="user_input", payload={})
            # Should return without touching anything.
            await repo.mark_steering_consumed("acme", run.id, [])
            pending = await repo.list_pending_steering("acme", run.id)
            self.assertEqual(len(pending), 1)

        asyncio.run(scenario())

    def test_commit_steering_consumptions_with_empty_list_returns_empty(self) -> None:
        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create(), graph_version="1"
            )
            run = await repo.create_run("acme", None, assistant, _run_create(assistant.id))
            result = await repo.commit_steering_consumptions("acme", run.id, [])
            self.assertEqual(result, [])

        asyncio.run(scenario())

    def test_commit_steering_consumptions_skips_non_matching_ids(self) -> None:
        """The per-event update loop must keep scanning past events that
        don't match any id in the batch, not just check the first one."""

        from lingxigraph.steering import SteeringEvent

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create(), graph_version="1"
            )
            run = await repo.create_run("acme", None, assistant, _run_create(assistant.id))
            e1, _ = await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"i": 1}
            )
            e2, _ = await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"i": 2}
            )

            consumption = SteeringConsumption(
                event=SteeringEvent(
                    id=e2.id,
                    run_id=run.id,
                    sequence=e2.sequence,
                    kind=e2.kind,
                    payload=e2.payload,
                    metadata=e2.metadata,
                    created_at=e2.created_at,
                ),
                consumed_at=e2.created_at,
                node="the_node",
            )
            stored = await repo.commit_steering_consumptions("acme", run.id, [consumption])
            self.assertEqual(len(stored), 1)
            self.assertEqual(stored[0].data["steering_event_id"], e2.id)

            statuses = {
                event.id: event.status for event in await repo.list_steering("acme", run.id)
            }
            self.assertEqual(statuses[e1.id], "pending")
            self.assertEqual(statuses[e2.id], "consumed")

        asyncio.run(scenario())

    def test_resume_rejects_a_missing_old_run(self) -> None:
        """Issue #16 PR #17 review round 4, point 1: the locked
        revalidation inside ``resume_run_with_pending_steering`` must
        reject (rather than silently proceed for) an old run that is no
        longer present -- it can no longer prove the run was ``paused``."""

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create(), graph_version="1"
            )
            run = await repo.create_run("acme", None, assistant, _run_create(assistant.id))
            # Simulate the old run row being gone by the time the resume
            # migration runs.
            del repo._runs[("acme", run.id)]
            with self.assertRaises(RunResumeConflictError):
                await repo.resume_run_with_pending_steering(
                    "acme", None, assistant, _run_create(assistant.id), run.id
                )

        asyncio.run(scenario())

    def test_transfer_preserves_idempotency_key_and_skips_already_consumed(
        self,
    ) -> None:
        """Only ``pending``/``delivered`` events transfer; an already
        ``consumed`` event is left behind untouched, and a transferred
        event's idempotency key must still dedupe under the new run id."""

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create(), graph_version="1"
            )
            old_run = await repo.create_run("acme", None, assistant, _run_create(assistant.id))
            consumed_event, _ = await repo.submit_steering(
                "acme", old_run.id, kind="user_input", payload={"i": "done"}
            )
            await repo.mark_steering_consumed("acme", old_run.id, [consumed_event.id])
            pending_event, _ = await repo.submit_steering(
                "acme",
                old_run.id,
                kind="user_input",
                payload={"i": "pending"},
                idempotency_key="key-1",
            )
            await repo.finish_run("acme", old_run.id, "paused", output={})

            new_run, transferred = await repo.resume_run_with_pending_steering(
                "acme", None, assistant, _run_create(assistant.id), old_run.id
            )

            self.assertEqual(len(transferred), 1)
            self.assertEqual(transferred[0].source_event_id, pending_event.id)
            self.assertEqual(transferred[0].idempotency_key, "key-1")

            old_events = {
                event.id: event.status for event in await repo.list_steering("acme", old_run.id)
            }
            self.assertEqual(old_events[consumed_event.id], "consumed")
            self.assertEqual(old_events[pending_event.id], "superseded")

            # Resubmitting with the same idempotency key under the *new*
            # run id must dedupe against the transferred row, proving the
            # transfer registered it in ``_steering_keys`` for the new id.
            dup_event, created = await repo.submit_steering(
                "acme",
                new_run.id,
                kind="user_input",
                payload={"different": True},
                idempotency_key="key-1",
            )
            self.assertFalse(created)
            self.assertEqual(dup_event.id, transferred[0].id)

        asyncio.run(scenario())


class InMemoryRepositoryRunLifecycleEdgeCaseTests(unittest.TestCase):
    """Direct unit coverage of ``InMemoryRepository`` run-lifecycle branches
    unrelated to steering payloads but exercised alongside them in the same
    module (queue quotas, redrive, and claim blocking)."""

    def test_redrive_run_returns_none_for_non_redrivable_status(self) -> None:
        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create(), graph_version="1"
            )
            run = await repo.create_run("acme", None, assistant, _run_create(assistant.id))
            # A freshly created run is "pending", not dead_letter/failed.
            result = await repo.redrive_run("acme", run.id)
            self.assertIsNone(result)

        asyncio.run(scenario())

    def test_create_run_respects_run_timeout_and_budget_config(self) -> None:
        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create(), graph_version="1"
            )
            run = await repo.create_run(
                "acme",
                None,
                assistant,
                _run_create(assistant.id).model_copy(
                    update={"run_timeout": 42, "max_tool_calls": 3}
                ),
            )
            self.assertEqual(run.config["run_timeout"], 42)
            self.assertEqual(run.config["max_tool_calls"], 3)

        asyncio.run(scenario())

    def test_create_run_queued_quota_exceeded_raises(self) -> None:
        async def scenario() -> None:
            from lingxigraph.errors import ConcurrentRunError
            from lingxigraph.server.repository import RepositoryLimits

            repo = InMemoryRepository(
                limits=RepositoryLimits(max_active_runs=10, max_queued_runs=1)
            )
            assistant = await repo.create_assistant(
                "acme", _assistant_create(), graph_version="1"
            )
            # First run stays "pending" (queued) since nothing claims it.
            await repo.create_run("acme", None, assistant, _run_create(assistant.id))
            with self.assertRaises(ConcurrentRunError):
                await repo.create_run("acme", None, assistant, _run_create(assistant.id))

        asyncio.run(scenario())

    def test_claim_run_skips_thread_blocked_pending_run(self) -> None:
        """A pending run whose thread already has an active run must be
        skipped by ``claim_run`` (blocked branch), leaving it unclaimed."""

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create(), graph_version="1"
            )
            thread = await repo.create_thread("acme", ThreadCreate())
            first = await repo.create_run(
                "acme", thread.id, assistant, _run_create(assistant.id)
            )
            claimed_first = await repo.claim_run("worker-1")
            self.assertEqual(claimed_first.id, first.id)

            await repo.create_run("acme", thread.id, assistant, _run_create(assistant.id))
            # `second` is pending on the same thread as the still-running
            # `first`, so claim_run must skip it and return None.
            claimed_second = await repo.claim_run("worker-2")
            self.assertIsNone(claimed_second)

        asyncio.run(scenario())


def _assistant_create():
    from lingxigraph.server.models import AssistantCreate

    return AssistantCreate(graph_id="double")


def _run_create(assistant_id: str, input: dict | None = None):
    from lingxigraph.server.models import RunCreate

    return RunCreate(
        assistant_id=assistant_id, input=input if input is not None else {"value": 1}
    )


MAX_STEERABLE_TICKS = 15


def make_registry() -> GraphRegistry:
    """A graph that loops draining steering until told to stop.

    Each tick sleeps briefly so a concurrently-running test has a real
    wall-clock window to submit steering input over HTTP before the graph
    reaches its tick cap -- mirrors a real "keep working until steered"
    agent loop.
    """

    builder = StateGraph(State)

    async def wait_for_stop(state: State, runtime: Runtime[Any]) -> dict[str, Any]:
        await asyncio.sleep(0.05)
        drained = [event.payload.get("message", "") for event in runtime.drain_steering()]
        merged = list(state.get("drained", [])) + drained
        return {"ticks": state.get("ticks", 0) + 1, "drained": merged}

    def should_continue(state: State) -> str:
        if state.get("drained") and "stop" in state["drained"]:
            return END
        if state.get("ticks", 0) >= MAX_STEERABLE_TICKS:
            return END
        return "wait_for_stop"

    builder.add_node("wait_for_stop", wait_for_stop)
    builder.add_edge(START, "wait_for_stop")
    builder.add_conditional_edges("wait_for_stop", should_continue)
    return GraphRegistry({"steerable": builder.compile()})


def make_registry_double() -> GraphRegistry:
    """A trivial, near-instant single-node graph registered as ``double``.

    Used by the round 6 finalization-fencing/gate tests, which need the
    graph to reach its terminal outcome (and the worker to enter its
    finalization window) almost immediately, without any steering to
    drain along the way.
    """

    builder = StateGraph(State)

    async def double(state: State, runtime: Runtime[Any]) -> dict[str, Any]:
        return {"ticks": state.get("ticks", 0) + 1}

    builder.add_node("double", double)
    builder.add_edge(START, "double")
    builder.add_edge("double", END)
    return GraphRegistry({"double": builder.compile()})


class ServerSteeringTests(unittest.TestCase):
    """End-to-end: HTTP accept -> durable inbox -> worker -> graph safe point."""

    def test_running_run_receives_steer_and_consumes_it(self) -> None:
        app = create_app(
            registry=make_registry(),
            authenticator=Authenticator.insecure_dev(),
            embedded_worker=True,
        )
        # Fast heartbeat so mid-run steering syncs promptly for the test;
        # production defaults to a longer interval.
        app.state.worker.heartbeat_seconds = 0.02
        headers = {"x-tenant-id": "acme"}
        with TestClient(app) as client:
            assistant = client.post(
                "/v1/assistants", headers=headers, json={"graph_id": "steerable"}
            ).json()
            run = client.post(
                "/v1/runs",
                headers=headers,
                json={"assistant_id": assistant["id"], "input": {"ticks": 0, "drained": []}},
            ).json()
            run_id = run["id"]

            # Wait until the worker has actually claimed and started the run.
            wait_for_status(client, run_id, headers, {"running"}, attempts=200)

            accepted = client.post(
                f"/v1/runs/{run_id}/steer",
                headers=headers,
                json={"kind": "user_input", "payload": {"message": "keep-going"}},
            )
            self.assertEqual(accepted.status_code, 202, accepted.text)
            self.assertEqual(accepted.json()["sequence"], 1)
            self.assertIn(accepted.json()["status"], {"pending", "delivered"})

            stop = client.post(
                f"/v1/runs/{run_id}/steer",
                headers=headers,
                json={"kind": "user_input", "payload": {"message": "stop"}},
            )
            self.assertEqual(stop.status_code, 202)

            result = wait_for_status(client, run_id, headers, {"succeeded", "failed"})
            self.assertEqual(result.json()["status"], "succeeded", result.text)
            self.assertIn("keep-going", result.json()["output"]["drained"])
            self.assertIn("stop", result.json()["output"]["drained"])

            events = asyncio.run(app.state.repository.list_events("acme", run_id))
            kinds = [event.kind for event in events]
            self.assertIn("run.steer.accepted", kinds)
            self.assertIn("run.steer.consumed", kinds)
            # accepted must precede consumed for the lifecycle to make sense.
            self.assertLess(
                kinds.index("run.steer.accepted"), kinds.index("run.steer.consumed")
            )

    def test_duplicate_idempotency_key_does_not_duplicate(self) -> None:
        app = create_app(registry=make_registry(), authenticator=Authenticator.insecure_dev())
        headers = {"x-tenant-id": "acme"}
        with TestClient(app) as client:
            assistant = client.post(
                "/v1/assistants", headers=headers, json={"graph_id": "steerable"}
            ).json()
            run = client.post(
                "/v1/runs",
                headers=headers,
                json={"assistant_id": assistant["id"], "input": {"ticks": 0, "drained": []}},
            ).json()
            run_id = run["id"]
            first = client.post(
                f"/v1/runs/{run_id}/steer",
                headers={**headers, "Idempotency-Key": "msg-123"},
                json={"kind": "user_input", "payload": {"message": "hi"}},
            ).json()
            second = client.post(
                f"/v1/runs/{run_id}/steer",
                headers={**headers, "Idempotency-Key": "msg-123"},
                json={"kind": "user_input", "payload": {"message": "hi-again"}},
            ).json()
            self.assertEqual(first["id"], second["id"])
            self.assertEqual(first["sequence"], second["sequence"])
            pending = asyncio.run(app.state.repository.list_pending_steering("acme", run_id))
            self.assertEqual(len(pending), 1)

    def test_terminal_run_returns_conflict_not_silent_event(self) -> None:
        app = create_app(registry=make_registry(), authenticator=Authenticator.insecure_dev())
        headers = {"x-tenant-id": "acme"}
        with TestClient(app) as client:
            assistant = client.post(
                "/v1/assistants", headers=headers, json={"graph_id": "steerable"}
            ).json()
            run = client.post(
                "/v1/runs",
                headers=headers,
                json={"assistant_id": assistant["id"], "input": {"ticks": 0, "drained": []}},
            ).json()
            run_id = run["id"]
            cancelled = client.post(f"/v1/runs/{run_id}/cancel", headers=headers)
            self.assertEqual(cancelled.json()["status"], "cancelled")
            rejected = client.post(
                f"/v1/runs/{run_id}/steer",
                headers=headers,
                json={"kind": "user_input", "payload": {}},
            )
            self.assertEqual(rejected.status_code, 409)
            self.assertEqual(rejected.json()["code"], "run_terminal")

    def test_missing_run_returns_404(self) -> None:
        app = create_app(registry=make_registry(), authenticator=Authenticator.insecure_dev())
        headers = {"x-tenant-id": "acme"}
        with TestClient(app) as client:
            response = client.post(
                "/v1/runs/does-not-exist/steer",
                headers=headers,
                json={"kind": "user_input", "payload": {}},
            )
            self.assertEqual(response.status_code, 404)

    def test_payload_too_large_rejected(self) -> None:
        app = create_app(registry=make_registry(), authenticator=Authenticator.insecure_dev())
        headers = {"x-tenant-id": "acme"}
        with TestClient(app) as client:
            assistant = client.post(
                "/v1/assistants", headers=headers, json={"graph_id": "steerable"}
            ).json()
            run = client.post(
                "/v1/runs",
                headers=headers,
                json={"assistant_id": assistant["id"], "input": {"ticks": 0, "drained": []}},
            ).json()
            response = client.post(
                f"/v1/runs/{run['id']}/steer",
                headers=headers,
                json={"kind": "user_input", "payload": {"message": "x" * 500_000}},
            )
            self.assertEqual(response.status_code, 413)

    def test_payload_between_32kb_and_generic_event_cap_still_rejected(self) -> None:
        """Issue #16 PR #17 review round 5, point 3 (CONTRACT): the public
        docs and ``steering.MAX_STEERING_PAYLOAD_BYTES`` both promise a
        ~32KB cap, but the HTTP endpoint used to pass
        ``repository.limits.max_event_bytes`` (262,144 bytes) as the
        payload ceiling instead, silently accepting payloads up to 8x the
        documented limit. A ~500KB payload (comfortably over *both*
        numbers) couldn't have caught this ~40KB-sized contract mismatch;
        this uses a payload sized specifically between the two limits."""

        from lingxigraph.steering import MAX_STEERING_PAYLOAD_BYTES

        app = create_app(registry=make_registry(), authenticator=Authenticator.insecure_dev())
        headers = {"x-tenant-id": "acme"}
        with TestClient(app) as client:
            assistant = client.post(
                "/v1/assistants", headers=headers, json={"graph_id": "steerable"}
            ).json()
            run = client.post(
                "/v1/runs",
                headers=headers,
                json={"assistant_id": assistant["id"], "input": {"ticks": 0, "drained": []}},
            ).json()
            oversized_message = "x" * (MAX_STEERING_PAYLOAD_BYTES + 8_000)
            self.assertLess(len(oversized_message), 262_144)
            response = client.post(
                f"/v1/runs/{run['id']}/steer",
                headers=headers,
                json={"kind": "user_input", "payload": {"message": oversized_message}},
            )
            self.assertEqual(response.status_code, 413)

    def test_queued_run_can_receive_steer_before_worker_claims_it(self) -> None:
        app = create_app(registry=make_registry(), authenticator=Authenticator.insecure_dev())
        headers = {"x-tenant-id": "acme"}
        with TestClient(app) as client:
            assistant = client.post(
                "/v1/assistants", headers=headers, json={"graph_id": "steerable"}
            ).json()
            run = client.post(
                "/v1/runs",
                headers=headers,
                json={"assistant_id": assistant["id"], "input": {"ticks": 0, "drained": []}},
            ).json()
            self.assertEqual(
                client.get(f"/v1/runs/{run['id']}", headers=headers).json()["status"], "pending"
            )
            accepted = client.post(
                f"/v1/runs/{run['id']}/steer",
                headers=headers,
                json={"kind": "user_input", "payload": {"message": "stop"}},
            )
            self.assertEqual(accepted.status_code, 202)

            worker = app.state.worker
            asyncio.run(worker.run_once())
            asyncio.run(worker.run_once())
            result = wait_for_status(client, run["id"], headers, {"succeeded", "failed"})
            self.assertEqual(result.json()["status"], "succeeded", result.text)
            self.assertIn("stop", result.json()["output"]["drained"])

    def test_worker_restart_recovers_pending_steering_from_postgres_shaped_repo(self) -> None:
        """Simulates a worker crash: a fresh Worker resumes from the repository."""

        from lingxigraph.server.worker import Worker

        async def scenario() -> None:
            registry = make_registry()
            repository = InMemoryRepository()
            assistant = await repository.create_assistant(
                "acme", _assistant_create_named("steerable"), graph_version="1"
            )
            run = await repository.create_run(
                "acme", None, assistant, _run_create(assistant.id, {"ticks": 0, "drained": []})
            )
            # Durable accept happens *before* any worker exists yet.
            await repository.submit_steering(
                "acme", run.id, kind="user_input", payload={"message": "stop"}
            )

            crashed_worker = Worker(registry, repository, heartbeat_seconds=0.05)
            claimed = await crashed_worker.run_once()
            self.assertTrue(claimed)
            finished = await repository.get_run("acme", run.id)
            self.assertEqual(
                finished.status.value if hasattr(finished.status, "value") else finished.status,
                "succeeded",
            )
            self.assertIn("stop", finished.output["drained"])

        asyncio.run(scenario())

    def test_redis_notify_loss_does_not_lose_steering(self) -> None:
        """The worker heartbeat re-polls PostgreSQL regardless of eventbus."""

        from lingxigraph.server.eventbus import EventBus
        from lingxigraph.server.worker import Worker

        class BlackHoleEventBus(EventBus):
            async def publish(self, tenant_id: str, run_id: str, sequence: int) -> None:
                return None

            async def wait(self, tenant_id: str, run_id: str, *, timeout: float = 15.0) -> None:
                return None

        async def scenario() -> None:
            registry = make_registry()
            repository = InMemoryRepository()
            assistant = await repository.create_assistant(
                "acme", _assistant_create_named("steerable"), graph_version="1"
            )
            run = await repository.create_run(
                "acme", None, assistant, _run_create(assistant.id, {"ticks": 0, "drained": []})
            )
            worker = Worker(
                registry, repository, heartbeat_seconds=0.05, event_bus=BlackHoleEventBus()
            )
            task = asyncio.create_task(worker.run_once())
            await asyncio.sleep(0.05)
            await repository.submit_steering(
                "acme", run.id, kind="user_input", payload={"message": "stop"}
            )
            await task
            finished = await repository.get_run("acme", run.id)
            status = (
                finished.status.value if hasattr(finished.status, "value") else finished.status
            )
            self.assertEqual(status, "succeeded")
            self.assertIn("stop", finished.output["drained"])

        asyncio.run(scenario())

    def test_cancel_takes_priority_over_steer(self) -> None:
        """Steering must never undo or delay a cancel request.

        Exercised on a still-queued run (no worker claims it): the durable
        steer is accepted, and the subsequent cancel still takes effect
        immediately and unconditionally -- a prior steer never blocks it.
        """

        app = create_app(registry=make_registry(), authenticator=Authenticator.insecure_dev())
        headers = {"x-tenant-id": "acme"}
        with TestClient(app) as client:
            assistant = client.post(
                "/v1/assistants", headers=headers, json={"graph_id": "steerable"}
            ).json()
            run = client.post(
                "/v1/runs",
                headers=headers,
                json={"assistant_id": assistant["id"], "input": {"ticks": 0, "drained": []}},
            ).json()
            run_id = run["id"]
            accepted = client.post(
                f"/v1/runs/{run_id}/steer",
                headers=headers,
                json={"kind": "user_input", "payload": {"message": "keep-going"}},
            )
            self.assertEqual(accepted.status_code, 202)
            cancel = client.post(f"/v1/runs/{run_id}/cancel", headers=headers)
            self.assertEqual(cancel.status_code, 200)
            self.assertEqual(cancel.json()["status"], "cancelled")
            # The steer stays durably recorded (never rewound by cancel) but
            # will simply never be delivered since the run never executes.
            pending = asyncio.run(app.state.repository.list_pending_steering("acme", run_id))
            self.assertEqual(len(pending), 1)

    def test_paused_run_steer_is_transferred_and_consumed_after_resume(self) -> None:
        """Documented choice (option B): steer during pause is durably
        accepted under the *paused* run_id, but ``/resume`` creates a brand
        new Run row -- so at resume time any still-pending steering is
        atomically transferred onto the new run_id and delivered/consumed
        by the graph there. This is the end-to-end regression for the
        blocking review point: the old test only asserted resume succeeded,
        never that the paused steer was actually drained/consumed."""

        from lingxigraph import interrupt

        paused_builder = StateGraph(State)

        def approval(state: State, runtime: Runtime[Any]) -> dict[str, Any]:
            drained_before = [event.payload for event in runtime.peek_steering()]
            value = interrupt({"question": "approve?", "seen_before_resume": drained_before})
            return {"ticks": int(value)}

        def after_approval(state: State, runtime: Runtime[Any]) -> dict[str, Any]:
            # Proves the transferred event is actually delivered to *this*
            # (new) run's safe point, not merely accepted somewhere.
            drained = [event.payload["message"] for event in runtime.drain_steering()]
            return {"drained": drained}

        paused_builder.add_node("approval", approval)
        paused_builder.add_node("after_approval", after_approval)
        paused_builder.add_edge(START, "approval").add_edge("approval", "after_approval")
        paused_builder.add_edge("after_approval", END)
        registry = GraphRegistry({"approval": paused_builder.compile()})

        app = create_app(
            registry=registry, authenticator=Authenticator.insecure_dev(), embedded_worker=True
        )
        headers = {"x-tenant-id": "acme"}
        with TestClient(app) as client:
            assistant = client.post(
                "/v1/assistants", headers=headers, json={"graph_id": "approval"}
            ).json()
            thread = client.post("/v1/threads", headers=headers, json={}).json()
            run = client.post(
                f"/v1/threads/{thread['id']}/runs",
                headers=headers,
                json={"assistant_id": assistant["id"], "input": {"ticks": 0}},
            ).json()
            run_id = run["id"]
            paused = wait_for_status(client, run_id, headers, {"paused"})
            self.assertEqual(paused.json()["status"], "paused")

            accepted = client.post(
                f"/v1/runs/{run_id}/steer",
                headers=headers,
                json={"kind": "user_input", "payload": {"message": "while-paused"}},
            )
            self.assertEqual(accepted.status_code, 202)
            pending = asyncio.run(app.state.repository.list_pending_steering("acme", run_id))
            self.assertEqual(len(pending), 1)

            resumed = client.post(
                f"/v1/runs/{run_id}/resume", headers=headers, json={"resume": 9}
            ).json()
            new_run_id = resumed["id"]
            self.assertNotEqual(new_run_id, run_id)
            result = wait_for_status(client, new_run_id, headers, {"succeeded", "failed"})
            self.assertEqual(result.json()["status"], "succeeded", result.text)

            # The graph actually saw and drained the while-paused event
            # under the *new* run.
            self.assertEqual(result.json()["output"]["drained"], ["while-paused"])

            # The old run's steering row is terminal ("superseded"), not
            # stuck "pending" forever -- and the new run's inbox shows it
            # went pending -> consumed there.
            old_events = asyncio.run(app.state.repository.list_steering("acme", run_id))
            self.assertEqual([event.status for event in old_events], ["superseded"])
            new_events = asyncio.run(app.state.repository.list_steering("acme", new_run_id))
            self.assertEqual(len(new_events), 1)
            self.assertEqual(new_events[0].status, "consumed")
            self.assertEqual(new_events[0].payload, {"message": "while-paused"})

            new_run_events = asyncio.run(app.state.repository.list_events("acme", new_run_id))
            kinds = [event.kind for event in new_run_events]
            self.assertIn("run.steer.accepted", kinds)
            self.assertIn("run.steer.consumed", kinds)
            consumed_event = next(
                event for event in new_run_events if event.kind == "run.steer.consumed"
            )
            self.assertEqual(consumed_event.data["node"], "after_approval")
            self.assertIn("queue_latency_seconds", consumed_event.data)
            self.assertGreaterEqual(consumed_event.data["queue_latency_seconds"], 0)

            # Further steer attempts against the now-superseded old run_id
            # are rejected loudly instead of silently pending again.
            stale = client.post(
                f"/v1/runs/{run_id}/steer",
                headers=headers,
                json={"kind": "user_input", "payload": {"message": "too-late"}},
            )
            self.assertEqual(stale.status_code, 409)
            self.assertEqual(stale.json()["code"], "run_superseded")

    def test_resume_with_no_pending_steering_still_marks_old_run_superseded(self) -> None:
        """Even with nothing to transfer, resume closes the old run_id off
        from future steering so it cannot silently pend forever a second
        time (see RunSupersededError)."""

        from lingxigraph import interrupt

        builder = StateGraph(State)

        def approval(state: State, runtime: Runtime[Any]) -> dict[str, Any]:
            value = interrupt({"question": "approve?"})
            return {"ticks": int(value)}

        builder.add_node("approval", approval)
        builder.add_edge(START, "approval").add_edge("approval", END)
        registry = GraphRegistry({"approval": builder.compile()})

        app = create_app(
            registry=registry, authenticator=Authenticator.insecure_dev(), embedded_worker=True
        )
        headers = {"x-tenant-id": "acme"}
        with TestClient(app) as client:
            assistant = client.post(
                "/v1/assistants", headers=headers, json={"graph_id": "approval"}
            ).json()
            thread = client.post("/v1/threads", headers=headers, json={}).json()
            run = client.post(
                f"/v1/threads/{thread['id']}/runs",
                headers=headers,
                json={"assistant_id": assistant["id"], "input": {"ticks": 0}},
            ).json()
            run_id = run["id"]
            wait_for_status(client, run_id, headers, {"paused"})

            resumed = client.post(
                f"/v1/runs/{run_id}/resume", headers=headers, json={"resume": 1}
            ).json()
            wait_for_status(client, resumed["id"], headers, {"succeeded", "failed"})

            stale = client.post(
                f"/v1/runs/{run_id}/steer",
                headers=headers,
                json={"kind": "user_input", "payload": {}},
            )
            self.assertEqual(stale.status_code, 409)
            self.assertEqual(stale.json()["code"], "run_superseded")

    def test_paused_steer_identity_and_latency_survive_resume_transfer(self) -> None:
        """Issue #16 PR #17 review point 3: a paused-run transfer must
        preserve the *original* event's identity and acceptance time.

        Proves both halves of the fix end to end:
        * The id a client's paused ``/steer`` call got back
          (``accepted_id``) is traceable, via ``source_event_id``, all the
          way through the resumed run's ``run.steer.accepted`` and
          ``run.steer.consumed`` events -- never silently replaced by an
          unrelated new id.
        * ``queue_latency_seconds`` on the eventual ``consumed`` event is
          computed from the *original* acceptance time, so it includes the
          time the event spent waiting while the run was paused, not just
          the time since the resume/transfer moment.
        """

        from lingxigraph import interrupt

        paused_builder = StateGraph(State)

        def approval(state: State, runtime: Runtime[Any]) -> dict[str, Any]:
            value = interrupt({"question": "approve?"})
            return {"ticks": int(value)}

        def after_approval(state: State, runtime: Runtime[Any]) -> dict[str, Any]:
            drained = [event.payload["message"] for event in runtime.drain_steering()]
            return {"drained": drained}

        paused_builder.add_node("approval", approval)
        paused_builder.add_node("after_approval", after_approval)
        paused_builder.add_edge(START, "approval").add_edge("approval", "after_approval")
        paused_builder.add_edge("after_approval", END)
        registry = GraphRegistry({"approval": paused_builder.compile()})

        app = create_app(
            registry=registry, authenticator=Authenticator.insecure_dev(), embedded_worker=True
        )
        headers = {"x-tenant-id": "acme"}
        with TestClient(app) as client:
            assistant = client.post(
                "/v1/assistants", headers=headers, json={"graph_id": "approval"}
            ).json()
            thread = client.post("/v1/threads", headers=headers, json={}).json()
            run = client.post(
                f"/v1/threads/{thread['id']}/runs",
                headers=headers,
                json={"assistant_id": assistant["id"], "input": {"ticks": 0}},
            ).json()
            run_id = run["id"]
            wait_for_status(client, run_id, headers, {"paused"})

            accepted = client.post(
                f"/v1/runs/{run_id}/steer",
                headers=headers,
                json={"kind": "user_input", "payload": {"message": "while-paused"}},
            ).json()
            accepted_id = accepted["id"]

            # A real pause-wait window: the queue latency computed after
            # resume must include (at least) this much time.
            pause_wait_seconds = 0.3
            time.sleep(pause_wait_seconds)

            resumed = client.post(
                f"/v1/runs/{run_id}/resume", headers=headers, json={"resume": 9}
            ).json()
            new_run_id = resumed["id"]
            result = wait_for_status(client, new_run_id, headers, {"succeeded", "failed"})
            self.assertEqual(result.json()["status"], "succeeded", result.text)

            new_events = asyncio.run(app.state.repository.list_events("acme", new_run_id))
            accepted_transfer = next(
                event for event in new_events if event.kind == "run.steer.accepted"
            )
            # The transferred event's id changed (it is a new durable row),
            # but its source_event_id must point back at the exact id the
            # client received from its paused-run /steer call.
            self.assertNotEqual(accepted_transfer.data["steering_event_id"], accepted_id)
            self.assertEqual(accepted_transfer.data["source_event_id"], accepted_id)

            consumed = next(event for event in new_events if event.kind == "run.steer.consumed")
            self.assertEqual(consumed.data["source_event_id"], accepted_id)
            self.assertGreaterEqual(
                consumed.data["queue_latency_seconds"], pause_wait_seconds * 0.9
            )

            steering_rows = asyncio.run(app.state.repository.list_steering("acme", new_run_id))
            self.assertEqual(steering_rows[0].source_event_id, accepted_id)

    def test_double_pause_resume_preserves_root_source_event_id(self) -> None:
        """Issue #16 PR #17 review round 3, point 2: a *second* pause and
        resume (A -> B -> C) must not overwrite the root ``source_event_id``
        with the intermediate hop's id -- C's migrated event must still
        correlate back to A, not B."""

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create(), graph_version="1"
            )
            from lingxigraph.server.models import RunCreate

            run_a = await repo.create_run("acme", None, assistant, _run_create(assistant.id))
            event_a, _ = await repo.submit_steering(
                "acme", run_a.id, kind="user_input", payload={"m": 1}
            )
            self.assertIsNone(event_a.source_event_id)
            await repo.finish_run("acme", run_a.id, "paused", output={})

            run_b, transferred_b = await repo.resume_run_with_pending_steering(
                "acme",
                run_a.thread_id,
                assistant,
                RunCreate(assistant_id=assistant.id, resume=1),
                run_a.id,
            )
            self.assertEqual(len(transferred_b), 1)
            event_b = transferred_b[0]
            self.assertEqual(event_b.source_event_id, event_a.id)
            self.assertEqual(event_b.created_at, event_a.created_at)
            await repo.finish_run("acme", run_b.id, "paused", output={})

            run_c, transferred_c = await repo.resume_run_with_pending_steering(
                "acme",
                run_b.thread_id,
                assistant,
                RunCreate(assistant_id=assistant.id, resume=1),
                run_b.id,
            )
            self.assertEqual(len(transferred_c), 1)
            event_c = transferred_c[0]
            # Root identity A must survive, never be replaced by B's id.
            self.assertEqual(event_c.source_event_id, event_a.id)
            self.assertEqual(event_c.created_at, event_a.created_at)

            from lingxigraph.steering import SteeringConsumption, SteeringEvent

            steering_event = SteeringEvent(
                id=event_c.id,
                run_id=run_c.id,
                sequence=event_c.sequence,
                kind=event_c.kind,
                payload=event_c.payload,
                metadata=event_c.metadata,
                created_at=event_c.created_at,
                source_event_id=event_c.source_event_id,
            )
            consumption = SteeringConsumption(
                event=steering_event,
                consumed_at=steering_event.created_at,
                node="n",
                namespace=(),
                task_id="t",
            )
            stored = await repo.commit_steering_consumptions("acme", run_c.id, [consumption])
            self.assertEqual(len(stored), 1)
            self.assertEqual(stored[0].data["source_event_id"], event_a.id)

        asyncio.run(scenario())

    def test_commit_steering_consumptions_idempotent_on_retry(self) -> None:
        """Issue #16 PR #17 review round 3, point 3: calling
        ``commit_steering_consumptions`` twice with the same batch (a
        worker retrying after an ack it never observed) must produce only
        one ``run.steer.consumed`` lifecycle event, not two."""

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create(), graph_version="1"
            )
            run = await repo.create_run("acme", None, assistant, _run_create(assistant.id))
            event, _ = await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"m": 1}
            )

            from lingxigraph.steering import SteeringConsumption, SteeringEvent

            steering_event = SteeringEvent(
                id=event.id,
                run_id=run.id,
                sequence=event.sequence,
                kind=event.kind,
                payload=event.payload,
                metadata=event.metadata,
                created_at=event.created_at,
            )
            consumption = SteeringConsumption(
                event=steering_event,
                consumed_at=steering_event.created_at,
                node="n",
                namespace=(),
                task_id="t",
            )
            first = await repo.commit_steering_consumptions("acme", run.id, [consumption])
            self.assertEqual(len(first), 1)
            second = await repo.commit_steering_consumptions("acme", run.id, [consumption])
            self.assertEqual(second, [])

            all_events = await repo.list_events("acme", run.id)
            consumed = [e for e in all_events if e.kind == "run.steer.consumed"]
            self.assertEqual(len(consumed), 1)

        asyncio.run(scenario())


class WorkerFencedWriteLeaseLostBranchTests(unittest.TestCase):
    """Coverage for the "lease no longer owned" branches of every fenced
    write the Worker performs during finalization -- these can only be
    reached by simulating a lease loss between the Worker deciding what to
    do and the repository actually committing it, so they're exercised
    here by monkeypatching the fenced repository methods to return
    ``None`` (exactly what a losing CAS/fence looks like from the
    caller's perspective), rather than by orchestrating a real multi-
    worker race (already covered elsewhere against real Postgres)."""

    def test_retry_or_dead_letter_abandons_when_retry_lease_lost(self) -> None:
        from lingxigraph.server.worker import Worker

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create_named("double"), graph_version="1"
            )
            await repo.create_run(
                "acme", None, assistant, _run_create(assistant.id, {"ticks": 0})
            )
            claimed = await repo.claim_run("worker-a", lease_seconds=30)
            assert claimed is not None

            worker = Worker(make_registry_double(), repo, worker_id="worker-a")

            async def lost_lease(*args: Any, **kwargs: Any) -> None:
                return None

            repo.retry_run_with_event_if_owned = lost_lease  # type: ignore[method-assign]
            await worker._retry_or_dead_letter(
                claimed, {"code": "delivery_retry", "message": "transient"}
            )
            # No exception, no crash, and (since the fenced write never
            # touched anything) the run's real status is whatever it was
            # before -- nothing durable to assert beyond "this returned".

        asyncio.run(scenario())

    def test_retry_or_dead_letter_abandons_when_dead_letter_lease_lost(self) -> None:
        from lingxigraph.server.worker import Worker

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create_named("double"), graph_version="1"
            )
            await repo.create_run(
                "acme", None, assistant, _run_create(assistant.id, {"ticks": 0})
            )
            claimed = await repo.claim_run("worker-a", lease_seconds=30)
            assert claimed is not None
            claimed.attempt = 999  # force the max-attempts (dead-letter) branch

            worker = Worker(make_registry_double(), repo, worker_id="worker-a", max_delivery_attempts=1)

            async def lost_lease(*args: Any, **kwargs: Any) -> None:
                return None

            repo.finish_run_if_owned = lost_lease  # type: ignore[method-assign]
            await worker._retry_or_dead_letter(
                claimed, {"code": "delivery_retry", "message": "transient"}
            )

        asyncio.run(scenario())

    def test_commit_intent_finalize_abandons_when_lease_lost(self) -> None:
        from lingxigraph.server.worker import Worker

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create_named("double"), graph_version="1"
            )
            await repo.create_run(
                "acme", None, assistant, _run_create(assistant.id, {"ticks": 0})
            )
            claimed = await repo.claim_run("worker-a", lease_seconds=30)
            assert claimed is not None

            worker = Worker(make_registry_double(), repo, worker_id="worker-a")

            async def lost_lease(*args: Any, **kwargs: Any) -> None:
                return None

            repo.finish_run_if_owned = lost_lease  # type: ignore[method-assign]
            intent = Worker._Intent(kind="finish", status=RunStatus.SUCCEEDED, output={})
            committed = await worker._commit_intent(claimed, intent)
            self.assertFalse(committed)

        asyncio.run(scenario())

    def test_final_steering_flush_returns_false_when_heartbeat_confirms_lease_lost(self) -> None:
        from lingxigraph.server.worker import Worker

        async def scenario() -> None:
            repo = InMemoryRepository()
            registry = make_registry()
            assistant = await repo.create_assistant(
                "acme", _assistant_create_named("steerable"), graph_version="1"
            )
            run = await repo.create_run(
                "acme",
                None,
                assistant,
                _run_create(assistant.id, {"ticks": 0, "drained": []}),
            )
            await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"message": "stop"}
            )
            claimed = await repo.claim_run("worker-a", lease_seconds=30)
            assert claimed is not None

            graph = registry.get("steerable").with_runtime()
            channel = graph.get_steering_channel(run.id)
            worker = Worker(make_registry(), repo, worker_id="worker-a")
            await worker._sync_steering_in(claimed, graph)
            channel.drain()

            async def always_fails(*args: Any, **kwargs: Any) -> None:
                raise ConnectionError("transient")

            async def lease_gone(*args: Any, **kwargs: Any) -> bool:
                return False

            repo.commit_steering_consumptions_if_owned = always_fails  # type: ignore[method-assign]
            repo.heartbeat = lease_gone  # type: ignore[method-assign]
            result = await worker._final_steering_flush(claimed, graph, base_delay=0.001, max_delay=0.001)
            self.assertFalse(result)

        asyncio.run(scenario())

    def test_final_steering_flush_returns_false_when_commit_reports_lease_lost(self) -> None:
        from lingxigraph.server.worker import Worker

        async def scenario() -> None:
            repo = InMemoryRepository()
            registry = make_registry()
            assistant = await repo.create_assistant(
                "acme", _assistant_create_named("steerable"), graph_version="1"
            )
            run = await repo.create_run(
                "acme",
                None,
                assistant,
                _run_create(assistant.id, {"ticks": 0, "drained": []}),
            )
            await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"message": "stop"}
            )
            claimed = await repo.claim_run("worker-a", lease_seconds=30)
            assert claimed is not None

            graph = registry.get("steerable").with_runtime()
            channel = graph.get_steering_channel(run.id)
            worker = Worker(make_registry(), repo, worker_id="worker-a")
            await worker._sync_steering_in(claimed, graph)
            channel.drain()

            async def lost_lease(*args: Any, **kwargs: Any) -> None:
                return None

            repo.commit_steering_consumptions_if_owned = lost_lease  # type: ignore[method-assign]
            result = await worker._final_steering_flush(claimed, graph)
            self.assertFalse(result)

        asyncio.run(scenario())

    def test_sync_steering_out_skips_ack_when_commit_fails_transiently(self) -> None:
        from lingxigraph.server.worker import Worker

        async def scenario() -> None:
            repo = InMemoryRepository()
            registry = make_registry()
            assistant = await repo.create_assistant(
                "acme", _assistant_create_named("steerable"), graph_version="1"
            )
            run = await repo.create_run(
                "acme",
                None,
                assistant,
                _run_create(assistant.id, {"ticks": 0, "drained": []}),
            )
            await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"message": "stop"}
            )
            claimed = await repo.claim_run("worker-a", lease_seconds=30)
            assert claimed is not None

            graph = registry.get("steerable").with_runtime()
            channel = graph.get_steering_channel(run.id)
            worker = Worker(make_registry(), repo, worker_id="worker-a")
            await worker._sync_steering_in(claimed, graph)
            channel.drain()

            async def always_fails(*args: Any, **kwargs: Any) -> None:
                raise ConnectionError("transient")

            repo.commit_steering_consumptions_if_owned = always_fails  # type: ignore[method-assign]
            await worker._sync_steering_out(claimed, graph)
            self.assertEqual(len(channel.peek_consumed()), 1)

        asyncio.run(scenario())

    def test_sync_steering_out_skips_ack_when_lease_lost(self) -> None:
        from lingxigraph.server.worker import Worker

        async def scenario() -> None:
            repo = InMemoryRepository()
            registry = make_registry()
            assistant = await repo.create_assistant(
                "acme", _assistant_create_named("steerable"), graph_version="1"
            )
            run = await repo.create_run(
                "acme",
                None,
                assistant,
                _run_create(assistant.id, {"ticks": 0, "drained": []}),
            )
            await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"message": "stop"}
            )
            claimed = await repo.claim_run("worker-a", lease_seconds=30)
            assert claimed is not None

            graph = registry.get("steerable").with_runtime()
            channel = graph.get_steering_channel(run.id)
            worker = Worker(make_registry(), repo, worker_id="worker-a")
            await worker._sync_steering_in(claimed, graph)
            channel.drain()

            async def lost_lease(*args: Any, **kwargs: Any) -> None:
                return None

            repo.commit_steering_consumptions_if_owned = lost_lease  # type: ignore[method-assign]
            await worker._sync_steering_out(claimed, graph)
            self.assertEqual(len(channel.peek_consumed()), 1)

        asyncio.run(scenario())

    def test_delivery_retry_recovers_steering_not_yet_drained(self) -> None:
        """Issue #16 PR #17 review round 9, point 1 (BLOCKER).

        A REAL Worker-level delivery-retry regression -- not a graph-level
        ``RetryPolicy`` test (a node-level retry never leaves the Worker's
        ``_execute`` at all, so it can never exercise the
        ``closing_steering``/``intent.kind == "retry"`` scoping bug this
        covers). Steering E is durably pending; attempt 1's graph node
        raises a retryable exception *before* calling ``drain_steering()``
        at all, so the exception unwinds all the way out of
        ``graph.astream`` and into ``Worker._execute``'s ``except
        Exception`` branch, producing a ``kind="retry"`` intent. Before the
        round 9 fix, ``closing_steering`` was true for *any* non-paused
        intent (including retry), so this sequence would close steering
        admission and supersede E -- permanently losing it -- the instant
        before the Run was sent back to ``pending``. Assert E survives: it
        is still ``pending``/``delivered`` (never ``superseded``) after
        attempt 1, and attempt 2 (a fresh ``claim_run()``, mirroring a real
        redelivery) re-ingests and drains it successfully.
        """

        from lingxigraph.server.worker import Worker

        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create_named("flaky_before_drain"), graph_version="1"
            )
            run = await repo.create_run(
                "acme", None, assistant, _run_create(assistant.id, {"ticks": 0, "drained": []})
            )
            accepted, created = await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"message": "hello"}
            )
            self.assertTrue(created)

            registry, attempts, drained_log = make_registry_flaky_before_drain()
            worker = Worker(registry, repo, worker_id="worker-a", max_delivery_attempts=5)

            # Attempt 1: the node raises a retryable exception before ever
            # calling drain_steering() -- the run must go back to pending,
            # and E must remain fully recoverable, never superseded.
            claimed1 = await worker.run_once()
            self.assertTrue(claimed1)
            self.assertEqual(len(attempts), 1)

            after_attempt_1 = await repo.get_run("acme", run.id)
            assert after_attempt_1 is not None
            status_1 = (
                after_attempt_1.status.value
                if hasattr(after_attempt_1.status, "value")
                else after_attempt_1.status
            )
            self.assertEqual(status_1, "pending")

            still_pending = await repo.list_pending_steering("acme", run.id)
            self.assertEqual([event.id for event in still_pending], [accepted.id])
            all_steering = await repo.list_steering("acme", run.id)
            self.assertEqual(len(all_steering), 1)
            self.assertIn(all_steering[0].status, ("pending", "delivered"))

            events_after_1 = await repo.list_events("acme", run.id)
            self.assertFalse(
                any(event.kind == "run.steer.superseded" for event in events_after_1)
            )

            # Attempt 2: a fresh delivery attempt re-ingests E via the
            # ordinary claim -> _sync_steering_in path and drains it.
            claimed2 = await worker.run_once()
            self.assertTrue(claimed2)
            self.assertEqual(len(attempts), 2)
            self.assertEqual(drained_log, [["hello"]])

            final = await repo.get_run("acme", run.id)
            assert final is not None
            final_status = (
                final.status.value if hasattr(final.status, "value") else final.status
            )
            self.assertEqual(final_status, "succeeded")

            final_steering = await repo.list_steering("acme", run.id)
            self.assertEqual(len(final_steering), 1)
            self.assertEqual(final_steering[0].status, "consumed")

        asyncio.run(scenario())


def make_registry_flaky_before_drain() -> tuple[GraphRegistry, list[int], list[list[str]]]:
    """A single-node graph whose node raises a retryable ``ConnectionError``
    on its first *delivery* attempt, before ever calling
    ``drain_steering()``, and succeeds (draining whatever is pending) on
    the second. No node-level ``RetryPolicy`` is attached -- the exception
    is meant to propagate all the way out of ``graph.astream`` into the
    Worker's own delivery-retry handling, not be swallowed by a
    node-internal retry.
    """

    attempts: list[int] = []
    drained_log: list[list[str]] = []

    builder = StateGraph(State)

    async def flaky(state: State, runtime: Runtime[Any]) -> dict[str, Any]:
        attempts.append(1)
        if len(attempts) < 2:
            raise ConnectionError("transient, before draining anything")
        drained = [event.payload["message"] for event in runtime.drain_steering()]
        drained_log.append(drained)
        return {"ticks": state.get("ticks", 0) + 1}

    builder.add_node("flaky", flaky)
    builder.add_edge(START, "flaky")
    builder.add_edge("flaky", END)
    return GraphRegistry({"flaky_before_drain": builder.compile()}), attempts, drained_log


class InMemoryRepositoryRound9BranchCoverageTests(unittest.TestCase):
    """Direct unit coverage of the InMemory-reachable branches added/kept
    across issue #16 PR #17 review round 9: the restored unfenced
    ``retry_run()``, ``close_steering()``'s already-closed no-op, the
    standalone ``supersede_pending_steering_if_owned()`` (both its
    nothing-to-supersede and something-to-supersede paths), and
    ``finalize_run_with_steering_disposition_if_owned(supersede_pending=False)``."""

    def test_retry_run_unfenced_admin_path(self) -> None:
        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create(), graph_version="1"
            )
            run = await repo.create_run("acme", None, assistant, _run_create(assistant.id))
            await repo.claim_run("worker-a", lease_seconds=30)
            updated = await repo.retry_run("acme", run.id, error={"code": "x"})
            self.assertEqual(updated.status, "pending")
            self.assertIsNone(updated.lease_owner)

        asyncio.run(scenario())

    def test_close_steering_is_a_no_op_when_already_closed(self) -> None:
        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create(), graph_version="1"
            )
            run = await repo.create_run("acme", None, assistant, _run_create(assistant.id))
            claimed = await repo.claim_run("worker-a", lease_seconds=30)
            assert claimed is not None
            first = await repo.close_steering("acme", run.id, "worker-a", claimed.attempt)
            self.assertTrue(first)
            second = await repo.close_steering("acme", run.id, "worker-a", claimed.attempt)
            self.assertTrue(second)

        asyncio.run(scenario())

    def test_supersede_pending_steering_if_owned_standalone_no_pending(self) -> None:
        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create(), graph_version="1"
            )
            run = await repo.create_run("acme", None, assistant, _run_create(assistant.id))
            claimed = await repo.claim_run("worker-a", lease_seconds=30)
            assert claimed is not None
            result = await repo.supersede_pending_steering_if_owned(
                "acme", run.id, "worker-a", claimed.attempt
            )
            self.assertEqual(result, [])

        asyncio.run(scenario())

    def test_supersede_pending_steering_if_owned_standalone_supersedes(self) -> None:
        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create(), graph_version="1"
            )
            run = await repo.create_run("acme", None, assistant, _run_create(assistant.id))
            claimed = await repo.claim_run("worker-a", lease_seconds=30)
            assert claimed is not None
            await repo.submit_steering("acme", run.id, kind="user_input", payload={"m": "x"})
            result = await repo.supersede_pending_steering_if_owned(
                "acme", run.id, "worker-a", claimed.attempt
            )
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0].kind, "run.steer.superseded")
            steering = await repo.list_steering("acme", run.id)
            self.assertEqual(steering[0].status, "superseded")

        asyncio.run(scenario())

    def test_finalize_run_with_steering_disposition_can_skip_supersede(self) -> None:
        async def scenario() -> None:
            repo = InMemoryRepository()
            assistant = await repo.create_assistant(
                "acme", _assistant_create(), graph_version="1"
            )
            run = await repo.create_run("acme", None, assistant, _run_create(assistant.id))
            claimed = await repo.claim_run("worker-a", lease_seconds=30)
            assert claimed is not None
            await repo.submit_steering("acme", run.id, kind="user_input", payload={"m": "x"})
            result = await repo.finalize_run_with_steering_disposition_if_owned(
                "acme",
                run.id,
                "worker-a",
                claimed.attempt,
                RunStatus.PAUSED,
                output=None,
                error=None,
                supersede_pending=False,
                supersede_reason="n/a",
            )
            self.assertIsNotNone(result)
            updated, superseded_events = result
            self.assertEqual(updated.status, "paused")
            self.assertEqual(superseded_events, [])
            steering = await repo.list_steering("acme", run.id)
            self.assertEqual(steering[0].status, "pending")

        asyncio.run(scenario())


def _assistant_create_named(graph_id: str):
    from lingxigraph.server.models import AssistantCreate

    return AssistantCreate(graph_id=graph_id)


if __name__ == "__main__":
    unittest.main()
