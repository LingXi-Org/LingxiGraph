"""Tests for durable mid-run steering (issue #16)."""

from __future__ import annotations

import asyncio
import operator
import time
import unittest
from typing import Annotated, Any, TypedDict

from fastapi.testclient import TestClient

from lingxigraph import END, START, RetryPolicy, Runtime, Send, StateGraph
from lingxigraph.errors import RunTerminalError
from lingxigraph.server import GraphRegistry, create_app
from lingxigraph.server.repository import InMemoryRepository
from lingxigraph.server.security import Authenticator
from lingxigraph.steering import (
    MAX_STEERING_PAYLOAD_BYTES,
    SteeringChannel,
    SteeringConsumption,
    SteeringPayloadTooLarge,
    validate_steering_payload,
)


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
            kind="user_input", payload={"message": "duplicate-should-be-ignored"},
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
        _, created = channel.submit(
            kind="user_input", payload={}, idempotency_key="msg-1"
        )
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
                id="db-1", run_id="run-1", sequence=5, kind="user_input",
                payload={}, metadata={}, created_at=datetime.now(UTC),
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
                "acme", run.id, kind="user_input", payload={"i": 2},
                idempotency_key="dup",
            )
            e3b, c3b = await repo.submit_steering(
                "acme", run.id, kind="user_input", payload={"different": True},
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
                await repo.submit_steering(
                    "acme", run.id, kind="user_input", payload={}
                )

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


def _assistant_create():
    from lingxigraph.server.models import AssistantCreate

    return AssistantCreate(graph_id="double")


def _run_create(assistant_id: str, input: dict | None = None):
    from lingxigraph.server.models import RunCreate

    return RunCreate(assistant_id=assistant_id, input=input if input is not None else {"value": 1})


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
            self.assertLess(kinds.index("run.steer.accepted"), kinds.index("run.steer.consumed"))

    def test_duplicate_idempotency_key_does_not_duplicate(self) -> None:
        app = create_app(
            registry=make_registry(), authenticator=Authenticator.insecure_dev()
        )
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
            pending = asyncio.run(
                app.state.repository.list_pending_steering("acme", run_id)
            )
            self.assertEqual(len(pending), 1)

    def test_terminal_run_returns_conflict_not_silent_event(self) -> None:
        app = create_app(
            registry=make_registry(), authenticator=Authenticator.insecure_dev()
        )
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
        app = create_app(
            registry=make_registry(), authenticator=Authenticator.insecure_dev()
        )
        headers = {"x-tenant-id": "acme"}
        with TestClient(app) as client:
            response = client.post(
                "/v1/runs/does-not-exist/steer",
                headers=headers,
                json={"kind": "user_input", "payload": {}},
            )
            self.assertEqual(response.status_code, 404)

    def test_payload_too_large_rejected(self) -> None:
        app = create_app(
            registry=make_registry(), authenticator=Authenticator.insecure_dev()
        )
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

    def test_queued_run_can_receive_steer_before_worker_claims_it(self) -> None:
        app = create_app(
            registry=make_registry(), authenticator=Authenticator.insecure_dev()
        )
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
            self.assertEqual(client.get(f"/v1/runs/{run['id']}", headers=headers).json()["status"], "pending")
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
            self.assertEqual(finished.status.value if hasattr(finished.status, "value") else finished.status, "succeeded")
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
            status = finished.status.value if hasattr(finished.status, "value") else finished.status
            self.assertEqual(status, "succeeded")
            self.assertIn("stop", finished.output["drained"])

        asyncio.run(scenario())

    def test_cancel_takes_priority_over_steer(self) -> None:
        """Steering must never undo or delay a cancel request.

        Exercised on a still-queued run (no worker claims it): the durable
        steer is accepted, and the subsequent cancel still takes effect
        immediately and unconditionally -- a prior steer never blocks it.
        """

        app = create_app(
            registry=make_registry(), authenticator=Authenticator.insecure_dev()
        )
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
            pending = asyncio.run(
                app.state.repository.list_pending_steering("acme", run_id)
            )
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
            pending = asyncio.run(
                app.state.repository.list_pending_steering("acme", run_id)
            )
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
            new_events = asyncio.run(
                app.state.repository.list_steering("acme", new_run_id)
            )
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


def _assistant_create_named(graph_id: str):
    from lingxigraph.server.models import AssistantCreate

    return AssistantCreate(graph_id=graph_id)


if __name__ == "__main__":
    unittest.main()
