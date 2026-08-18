import asyncio
import time
import unittest
from typing import TypedDict

from fastapi.testclient import TestClient

from lingxigraph import END, START, StateGraph, interrupt
from lingxigraph.server import GraphRegistry, create_app
from lingxigraph.server.security import Authenticator


class ServerState(TypedDict):
    value: int


def make_registry() -> GraphRegistry:
    builder = StateGraph(ServerState, name="server-test", version="1.0.0")
    builder.add_node("double", lambda state: {"value": state["value"] * 2})
    builder.add_edge(START, "double")
    builder.add_edge("double", END)

    paused = StateGraph(ServerState, name="interrupt-test", version="1.0.0")

    def approval(_state):
        return {"value": int(interrupt({"question": "new value?"}))}

    paused.add_node("approval", approval)
    paused.add_edge(START, "approval")
    paused.add_edge("approval", END)
    return GraphRegistry({"double": builder.compile(), "approval": paused.compile()})


def wait_for_status(client, run_id, headers, expected):
    value = None
    for _ in range(100):
        value = client.get(f"/v1/runs/{run_id}", headers=headers)
        if value.json()["status"] in expected:
            return value
        time.sleep(0.01)
    return value


class AgentServerTests(unittest.TestCase):
    def test_threaded_run_worker_events_state_fork_sse_and_tenant_isolation(self) -> None:
        app = create_app(
            registry=make_registry(),
            authenticator=Authenticator.insecure_dev(),
            embedded_worker=True,
        )
        acme = {"x-tenant-id": "acme"}
        other = {"x-tenant-id": "other"}

        with TestClient(app) as client:
            studio = client.get("/studio/")
            self.assertEqual(studio.status_code, 200)
            self.assertIn("LingxiGraph Studio", studio.text)
            structure = client.get("/v1/graphs/double/structure", headers=acme)
            self.assertEqual(structure.status_code, 200, structure.text)
            self.assertEqual(
                [node["id"] for node in structure.json()["nodes"]],
                ["__start__", "double", "__end__"],
            )
            assistant = client.post("/v1/assistants", headers=acme, json={"graph_id": "double"})
            self.assertEqual(assistant.status_code, 201, assistant.text)
            thread = client.post("/v1/threads", headers=acme, json={})
            self.assertEqual(thread.status_code, 201, thread.text)
            run = client.post(
                f"/v1/threads/{thread.json()['id']}/runs",
                headers=acme,
                json={"assistant_id": assistant.json()["id"], "input": {"value": 3}},
            )
            self.assertEqual(run.status_code, 202, run.text)
            run_id = run.json()["id"]
            result = wait_for_status(client, run_id, acme, {"succeeded", "failed"})

            self.assertEqual(result.json()["status"], "succeeded", result.text)
            self.assertEqual(result.json()["output"], {"value": 6})
            self.assertEqual(client.get(f"/v1/runs/{run_id}", headers=other).status_code, 404)
            events = asyncio.run(app.state.repository.list_events("acme", run_id))
            self.assertEqual(events[0].kind, "run_started")
            self.assertEqual(events[-1].kind, "run_completed")
            self.assertEqual(
                [event.sequence for event in events], list(range(1, len(events) + 1))
            )
            state = client.get(f"/v1/threads/{thread.json()['id']}/state", headers=acme)
            self.assertEqual(state.json()["values"], {"value": 6})
            history = client.get(f"/v1/threads/{thread.json()['id']}/history", headers=acme)
            self.assertGreaterEqual(len(history.json()), 1)
            forked = client.post(
                f"/v1/threads/{thread.json()['id']}/fork",
                headers=acme,
                json={"values": {"value": 9}, "as_node": "double"},
            )
            self.assertEqual(forked.status_code, 200, forked.text)
            streamed = client.get(f"/v1/runs/{run_id}/stream", headers=acme)
            self.assertIn("event: run_completed", streamed.text)
            resumed_stream = client.get(
                f"/v1/runs/{run_id}/stream",
                headers={**acme, "Last-Event-ID": "2"},
            )
            self.assertNotIn("id: 1\n", resumed_stream.text)

    def test_rbac_rejects_viewer_mutation(self) -> None:
        app = create_app(registry=make_registry(), authenticator=Authenticator.insecure_dev())
        with TestClient(app) as client:
            response = client.post(
                "/v1/assistants",
                headers={"x-roles": "viewer"},
                json={"graph_id": "double"},
            )
            self.assertEqual(response.status_code, 403)

    def test_resource_store_schedule_and_cancel_lifecycle(self) -> None:
        app = create_app(registry=make_registry(), authenticator=Authenticator.insecure_dev())
        headers = {"x-tenant-id": "acme", "x-request-id": "request-123"}
        with TestClient(app) as client:
            self.assertEqual(client.get("/health").json()["status"], "ok")
            self.assertEqual(client.get("/ready").json()["graphs"], 2)
            self.assertEqual(client.get("/v1/graphs", headers=headers).status_code, 200)
            self.assertEqual(
                client.get("/v1/graphs/double", headers=headers).json()["id"], "double"
            )
            self.assertEqual(client.get("/v1/graphs/missing", headers=headers).status_code, 404)
            missing = client.get("/v1/graphs/missing", headers=headers)
            self.assertEqual(missing.headers["content-type"], "application/problem+json")
            self.assertEqual(missing.json()["code"], "not_found")
            self.assertEqual(missing.json()["request_id"], "request-123")
            invalid = client.post(
                "/v1/assistants", headers=headers, json={"graph_id": "double", "extra": 1}
            )
            self.assertEqual(invalid.status_code, 422)
            self.assertEqual(invalid.json()["code"], "validation_error")

            assistant = client.post(
                "/v1/assistants",
                headers=headers,
                json={"graph_id": "double", "name": "original"},
            ).json()
            self.assertEqual(
                client.get(f"/v1/assistants/{assistant['id']}", headers=headers).status_code,
                200,
            )
            self.assertEqual(len(client.get("/v1/assistants", headers=headers).json()), 1)
            updated = client.patch(
                f"/v1/assistants/{assistant['id']}",
                headers=headers,
                json={"name": "updated"},
            )
            self.assertEqual(updated.json()["name"], "updated")
            self.assertEqual(
                client.get("/v1/assistants/missing", headers=headers).status_code, 404
            )
            self.assertEqual(
                client.patch(
                    "/v1/assistants/missing", headers=headers, json={"name": "missing"}
                ).status_code,
                404,
            )
            self.assertEqual(
                client.delete("/v1/assistants/missing", headers=headers).status_code,
                404,
            )

            thread = client.post("/v1/threads", headers=headers, json={}).json()
            self.assertEqual(len(client.get("/v1/threads", headers=headers).json()), 1)
            self.assertEqual(
                client.get(f"/v1/threads/{thread['id']}", headers=headers).status_code,
                200,
            )
            updated_thread = client.patch(
                f"/v1/threads/{thread['id']}",
                headers=headers,
                json={"metadata": {"team": "platform"}},
            )
            self.assertEqual(updated_thread.json()["metadata"]["team"], "platform")
            self.assertEqual(
                client.get("/v1/threads/missing", headers=headers).status_code, 404
            )
            self.assertEqual(
                client.patch(
                    "/v1/threads/missing", headers=headers, json={"metadata": {}}
                ).status_code,
                404,
            )
            self.assertEqual(
                client.get(f"/v1/threads/{thread['id']}/state", headers=headers).status_code,
                404,
            )
            self.assertEqual(
                client.get(
                    "/v1/runs/missing/join", headers=headers, params={"timeout": 1}
                ).status_code,
                404,
            )
            invalid_join = client.get(
                "/v1/runs/missing/join", headers=headers, params={"timeout": 0}
            )
            self.assertEqual(invalid_join.status_code, 400)
            self.assertEqual(invalid_join.json()["code"], "invalid_request")
            run = client.post(
                f"/v1/threads/{thread['id']}/runs",
                headers=headers,
                json={"assistant_id": assistant["id"], "input": {"value": 1}},
            ).json()
            self.assertEqual(
                len(client.get(f"/v1/threads/{thread['id']}/runs", headers=headers).json()),
                1,
            )
            cancelled = client.post(f"/v1/runs/{run['id']}/cancel", headers=headers)
            self.assertEqual(cancelled.json()["status"], "cancelled")
            joined = client.get(
                f"/v1/runs/{run['id']}/join", headers=headers, params={"timeout": 1}
            )
            self.assertEqual(joined.json()["status"], "cancelled")
            self.assertEqual(
                client.post(f"/v1/runs/{run['id']}/cancel", headers=headers).status_code,
                409,
            )

            batch = client.post(
                "/v1/store/batch",
                headers=headers,
                json={
                    "operations": [
                        {
                            "kind": "put",
                            "namespace": ["users"],
                            "key": "alice",
                            "value": {"name": "Alice"},
                        },
                        {"kind": "get", "namespace": ["users"], "key": "alice"},
                    ]
                },
            )
            self.assertEqual(batch.status_code, 200, batch.text)
            self.assertEqual(batch.json()["results"][1]["value"]["name"], "Alice")
            searched = client.get(
                "/v1/store/search",
                headers=headers,
                params={"namespace": "users", "query": "Alice"},
            )
            self.assertEqual(len(searched.json()["items"]), 1)

            schedule = client.post(
                "/v1/schedules",
                headers=headers,
                json={"assistant_id": assistant["id"], "cron": "* * * * *"},
            ).json()
            self.assertEqual(len(client.get("/v1/schedules", headers=headers).json()), 1)
            updated_schedule = client.patch(
                f"/v1/schedules/{schedule['id']}",
                headers=headers,
                json={"enabled": False, "timezone": "Asia/Shanghai"},
            )
            self.assertFalse(updated_schedule.json()["enabled"])
            self.assertEqual(updated_schedule.json()["timezone"], "Asia/Shanghai")
            self.assertEqual(
                client.patch(
                    "/v1/schedules/missing", headers=headers, json={"enabled": False}
                ).status_code,
                404,
            )
            self.assertEqual(
                client.delete("/v1/schedules/missing", headers=headers).status_code,
                404,
            )
            self.assertEqual(
                client.delete(f"/v1/schedules/{schedule['id']}", headers=headers).status_code,
                204,
            )
            metrics = client.get("/metrics", headers=headers)
            self.assertIn("lingxigraph_graphs 2", metrics.text)
            self.assertIn('lingxigraph_runs{status="cancelled"} 1', metrics.text)
            self.assertIn("lingxigraph_queue_depth 0", metrics.text)
            self.assertIn("lingxigraph_active_runs 0", metrics.text)
            self.assertIn("lingxigraph_run_events 0", metrics.text)
            self.assertIn("lingxigraph_sse_clients 0", metrics.text)
            self.assertEqual(metrics.headers["x-request-id"], "request-123")
            self.assertEqual(
                client.delete(f"/v1/threads/{thread['id']}", headers=headers).status_code,
                204,
            )
            self.assertEqual(
                client.delete(f"/v1/assistants/{assistant['id']}", headers=headers).status_code,
                204,
            )

    def test_cancel_paused_run_terminates_immediately(self) -> None:
        """Issue #16 PR #17 review round 12 (BLOCKER): cancelling a ``paused``
        run must resolve to ``cancelled`` immediately instead of flipping to
        a lease-less ``cancelling`` that no recovery mechanism (including
        round 11's expired-lease recovery) can ever unstick, because a
        paused run never held a lease in the first place.
        """
        app = create_app(
            registry=make_registry(),
            authenticator=Authenticator.insecure_dev(),
            embedded_worker=True,
        )
        headers = {"x-tenant-id": "acme"}
        with TestClient(app) as client:
            assistant = client.post(
                "/v1/assistants",
                headers=headers,
                json={"graph_id": "approval", "name": "approval"},
            ).json()
            thread = client.post("/v1/threads", headers=headers, json={}).json()
            paused = client.post(
                f"/v1/threads/{thread['id']}/runs",
                headers=headers,
                json={"assistant_id": assistant["id"], "input": {"value": 0}},
            ).json()
            paused = wait_for_status(client, paused["id"], headers, {"paused"}).json()
            self.assertEqual(paused["status"], "paused")

            # Requirement 4: a paused run with pending steering must keep
            # that steering durable -- this fix must not implicitly
            # consume/replay/supersede it.
            steer = client.post(
                f"/v1/runs/{paused['id']}/steer",
                headers=headers,
                json={"kind": "note", "payload": {"text": "still relevant"}},
            )
            self.assertEqual(steer.status_code, 202)

            # Regression 1 (InMemory, direct repository call against a
            # genuinely paused run produced by the Worker, not a
            # hand-faked status).
            repository = app.state.repository
            self.assertIsNone(asyncio.run(repository.get_run("acme", paused["id"])).lease_owner)
            cancel_result = asyncio.run(repository.request_cancel("acme", paused["id"]))
            self.assertTrue(cancel_result)
            after_cancel = asyncio.run(repository.get_run("acme", paused["id"]))
            self.assertEqual(after_cancel.status, "cancelled")
            self.assertIsNotNone(after_cancel.finished_at)
            self.assertIsNone(after_cancel.lease_owner)
            self.assertIsNone(after_cancel.lease_expires_at)
            claimed = asyncio.run(repository.claim_run("worker", lease_seconds=30))
            self.assertIsNone(claimed)

            # Steering submitted while paused stays durable history -- not
            # implicitly dropped by the cancel-while-paused fix.
            events = asyncio.run(repository.list_events("acme", paused["id"]))
            self.assertTrue(any(event.kind == "run.steer.accepted" for event in events))

    def test_cancel_paused_run_http_e2e(self) -> None:
        """Regression 2 (HTTP E2E): the full public control-API surface
        must observe the paused-run cancel as an immediate terminal state,
        with resume and a new-idempotency-key steer both rejected 409.
        """
        app = create_app(
            registry=make_registry(),
            authenticator=Authenticator.insecure_dev(),
            embedded_worker=True,
        )
        headers = {"x-tenant-id": "acme"}
        with TestClient(app) as client:
            assistant = client.post(
                "/v1/assistants",
                headers=headers,
                json={"graph_id": "approval", "name": "approval"},
            ).json()
            thread = client.post("/v1/threads", headers=headers, json={}).json()
            paused = client.post(
                f"/v1/threads/{thread['id']}/runs",
                headers=headers,
                json={"assistant_id": assistant["id"], "input": {"value": 0}},
            ).json()
            paused = wait_for_status(client, paused["id"], headers, {"paused"}).json()
            self.assertEqual(paused["status"], "paused")

            cancelled = client.post(f"/v1/runs/{paused['id']}/cancel", headers=headers)
            self.assertEqual(cancelled.status_code, 200, cancelled.text)
            self.assertEqual(cancelled.json()["status"], "cancelled")

            joined = client.get(f"/v1/runs/{paused['id']}/join", headers=headers)
            self.assertEqual(joined.status_code, 200, joined.text)
            self.assertEqual(joined.json()["status"], "cancelled")

            resume_after_cancel = client.post(
                f"/v1/runs/{paused['id']}/resume",
                headers=headers,
                json={"resume": 7},
            )
            self.assertEqual(resume_after_cancel.status_code, 409)

            steer_after_cancel = client.post(
                f"/v1/runs/{paused['id']}/steer",
                headers={**headers, "Idempotency-Key": "post-cancel-key"},
                json={"kind": "note", "payload": {"text": "too late"}},
            )
            self.assertEqual(steer_after_cancel.status_code, 409)
            self.assertEqual(steer_after_cancel.json()["code"], "run_terminal")

    def test_cancel_of_a_resumed_run_returns_409_and_the_new_run_is_still_cancellable(
        self,
    ) -> None:
        """Issue #16 PR #17 review round 13 (BLOCKER), HTTP E2E: resuming
        ``A`` into ``B`` and then calling ``/cancel`` on the now-stale
        ``A`` must return ``409 run_superseded`` -- not a false-success 200
        -- and ``B`` (the run actually still executing) must remain
        genuinely cancellable afterwards.
        """
        app = create_app(
            registry=make_registry(),
            authenticator=Authenticator.insecure_dev(),
            embedded_worker=True,
        )
        headers = {"x-tenant-id": "acme"}
        with TestClient(app) as client:
            assistant = client.post(
                "/v1/assistants",
                headers=headers,
                json={"graph_id": "approval", "name": "approval"},
            ).json()
            thread = client.post("/v1/threads", headers=headers, json={}).json()
            paused = client.post(
                f"/v1/threads/{thread['id']}/runs",
                headers=headers,
                json={"assistant_id": assistant["id"], "input": {"value": 0}},
            ).json()
            paused = wait_for_status(client, paused["id"], headers, {"paused"}).json()
            self.assertEqual(paused["status"], "paused")

            resumed = client.post(
                f"/v1/runs/{paused['id']}/resume",
                headers=headers,
                json={"resume": 7},
            )
            self.assertEqual(resumed.status_code, 202, resumed.text)
            new_run_id = resumed.json()["id"]
            self.assertNotEqual(new_run_id, paused["id"])

            # Cancelling the stale, resumed-away run must be rejected as
            # superseded -- never a false-success 200 -- and the caller
            # must be told exactly which run_id to target instead.
            cancel_old = client.post(f"/v1/runs/{paused['id']}/cancel", headers=headers)
            self.assertEqual(cancel_old.status_code, 409, cancel_old.text)
            self.assertEqual(cancel_old.json()["code"], "run_superseded")
            self.assertIn(new_run_id, cancel_old.json()["detail"])

            # The old run must not have been mutated to a terminal state
            # by the rejected cancel attempt.
            old_after = client.get(f"/v1/runs/{paused['id']}", headers=headers).json()
            self.assertEqual(old_after["status"], "paused")

            # Cancelling the actual resumed descendant genuinely works
            # (unless the tiny "approval" graph already raced to a
            # terminal success before the cancel landed -- either way,
            # it must never be the superseded 409 the old run gets).
            new_run = client.get(f"/v1/runs/{new_run_id}", headers=headers).json()
            self.assertNotEqual(new_run["status"], "cancelled")
            cancel_new = client.post(f"/v1/runs/{new_run_id}/cancel", headers=headers)
            # It must never be rejected as *superseded* -- that would mean
            # this endpoint's own descendant is (wrongly) being treated as
            # stale. A plain 409 "conflict" is acceptable if the tiny
            # "approval" graph already raced to a terminal success before
            # the cancel landed.
            self.assertNotEqual(
                cancel_new.json().get("code"), "run_superseded", cancel_new.text
            )
            if cancel_new.status_code == 200:
                joined_new = wait_for_status(
                    client, new_run_id, headers, {"cancelled", "succeeded"}
                ).json()
                self.assertIn(joined_new["status"], {"cancelled", "succeeded"})

    def test_interrupt_resume_and_protocol_gateways(self) -> None:
        app = create_app(
            registry=make_registry(),
            authenticator=Authenticator.insecure_dev(),
            embedded_worker=True,
        )
        headers = {"x-tenant-id": "acme"}
        with TestClient(app) as client:
            assistant = client.post(
                "/v1/assistants",
                headers=headers,
                json={"graph_id": "approval", "name": "approval"},
            ).json()
            thread = client.post("/v1/threads", headers=headers, json={}).json()
            paused = client.post(
                f"/v1/threads/{thread['id']}/runs",
                headers=headers,
                json={"assistant_id": assistant["id"], "input": {"value": 0}},
            ).json()
            paused = wait_for_status(client, paused["id"], headers, {"paused"}).json()
            self.assertEqual(paused["status"], "paused")
            resumed = client.post(
                f"/v1/runs/{paused['id']}/resume",
                headers=headers,
                json={"resume": 7},
            ).json()
            resumed = wait_for_status(client, resumed["id"], headers, {"succeeded"}).json()
            self.assertEqual(resumed["output"]["value"], 7)

        gateway_app = create_app(
            registry=make_registry(), authenticator=Authenticator.insecure_dev()
        )
        with TestClient(gateway_app) as client:
            remote = client.post(
                "/v1/assistants",
                headers=headers,
                json={
                    "graph_id": "double",
                    "name": "remote-double",
                    "metadata": {"mcp_expose": True, "mcp_tool_name": "double"},
                },
            ).json()
            card = client.get(
                f"/a2a/{remote['id']}/.well-known/agent-card.json", headers=headers
            )
            self.assertEqual(card.json()["name"], "remote-double")
            sent = client.post(
                f"/a2a/{remote['id']}",
                headers=headers,
                json={
                    "jsonrpc": "2.0",
                    "id": "one",
                    "method": "message/send",
                    "params": {"message": {"value": 4}},
                },
            ).json()["result"]
            fetched = client.post(
                f"/a2a/{remote['id']}",
                headers=headers,
                json={
                    "jsonrpc": "2.0",
                    "id": "two",
                    "method": "tasks/get",
                    "params": {"id": sent["id"]},
                },
            )
            self.assertEqual(fetched.json()["result"]["id"], sent["id"])
            cancelled = client.post(
                f"/a2a/{remote['id']}",
                headers=headers,
                json={
                    "jsonrpc": "2.0",
                    "id": "three",
                    "method": "tasks/cancel",
                    "params": {"id": sent["id"]},
                },
            )
            self.assertEqual(cancelled.json()["result"]["status"], "cancelled")
            initialized = client.post(
                "/mcp",
                headers=headers,
                json={"jsonrpc": "2.0", "id": 1, "method": "initialize"},
            )
            self.assertEqual(initialized.json()["result"]["serverInfo"]["name"], "LingxiGraph")
            self.assertEqual(initialized.json()["result"]["serverInfo"]["version"], "2.2.0")
            tools = client.post(
                "/mcp",
                headers=headers,
                json={"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
            )
            self.assertEqual(tools.json()["result"]["tools"][0]["name"], "double")
            called = client.post(
                "/mcp",
                headers=headers,
                json={
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "tools/call",
                    "params": {"name": "double", "arguments": {"value": 5}},
                },
            )
            self.assertFalse(called.json()["result"]["isError"])


if __name__ == "__main__":
    unittest.main()
