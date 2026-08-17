"""Tests for durable mid-run steering (issue #16)."""

from __future__ import annotations

import asyncio
import time
import unittest
from typing import Any, TypedDict

from fastapi.testclient import TestClient

from lingxigraph import END, START, Runtime, StateGraph
from lingxigraph.errors import RunTerminalError
from lingxigraph.server import GraphRegistry, create_app
from lingxigraph.server.repository import InMemoryRepository
from lingxigraph.server.security import Authenticator
from lingxigraph.steering import (
    MAX_STEERING_PAYLOAD_BYTES,
    SteeringChannel,
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

    def test_paused_run_steer_is_durably_accepted_not_immediately_consumed(self) -> None:
        """Documented choice: steer during pause is durably accepted, and only
        delivered to the graph at resume time (resume remains the API for
        actually unpausing -- steer never substitutes for it)."""

        from lingxigraph import interrupt

        paused_builder = StateGraph(State)

        def approval(state: State, runtime: Runtime[Any]) -> dict[str, Any]:
            drained_before = [event.payload for event in runtime.peek_steering()]
            value = interrupt({"question": "approve?", "seen_before_resume": drained_before})
            return {"ticks": int(value)}

        paused_builder.add_node("approval", approval)
        paused_builder.add_edge(START, "approval").add_edge("approval", END)
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
            result = wait_for_status(client, resumed["id"], headers, {"succeeded", "failed"})
            self.assertEqual(result.json()["status"], "succeeded", result.text)


def _assistant_create_named(graph_id: str):
    from lingxigraph.server.models import AssistantCreate

    return AssistantCreate(graph_id=graph_id)


if __name__ == "__main__":
    unittest.main()
