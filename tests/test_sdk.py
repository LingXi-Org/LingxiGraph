import asyncio
import json
import unittest

import httpx

from lingxigraph.sdk import (
    AsyncLingxiGraphClient,
    LingxiGraphAPIError,
    LingxiGraphClient,
)


def handler(request: httpx.Request) -> httpx.Response:
    if request.url.path == "/problem":
        return httpx.Response(
            429,
            json={
                "code": "quota_exceeded",
                "detail": "full",
                "request_id": "request-1",
                "retryable": True,
            },
        )
    if request.url.path.endswith("/stream"):
        payload = json.dumps({"sequence": 3, "kind": "run_completed"})
        return httpx.Response(
            200,
            text=f": heartbeat\n\nid: 3\nevent: run_completed\ndata: {payload}\n\n",
            headers={"content-type": "text/event-stream"},
        )
    if request.method == "DELETE":
        return httpx.Response(204)
    path = request.url.path
    status = "succeeded" if "/runs/" in path else None
    value = {"method": request.method, "path": path}
    if status:
        value["status"] = status
    if path.endswith("/history") or path in {
        "/v1/graphs",
        "/v1/assistants",
        "/v1/threads",
        "/v1/schedules",
    }:
        return httpx.Response(200, json=[value])
    return httpx.Response(200, json=value)


class SyncSDKTests(unittest.TestCase):
    def test_all_resource_facades_and_sse(self) -> None:
        transport = httpx.MockTransport(handler)
        with LingxiGraphClient(
            "https://agents.example", token="token", transport=transport
        ) as client:
            client.graphs.list()
            client.graphs.get("graph")
            client.assistants.create(graph_id="graph")
            client.assistants.list()
            client.assistants.get("assistant")
            client.assistants.update("assistant", name="updated")
            client.assistants.delete("assistant")
            client.threads.create(metadata={"team": "platform"})
            client.threads.list()
            client.threads.get("thread")
            client.threads.update("thread", metadata={"team": "platform"})
            client.threads.state("thread", checkpoint_id="checkpoint")
            client.threads.history("thread")
            client.threads.fork("thread", values={"value": 2})
            client.threads.delete("thread")
            client.runs.create("assistant", input={"value": 1})
            client.runs.create("assistant", thread_id="thread", input={"value": 1})
            client.runs.get("run")
            client.runs.list("thread")
            client.runs.cancel("run")
            client.runs.resume("run", True, goto="next")
            self.assertEqual(client.runs.join("run", poll_interval=0)["status"], "succeeded")
            self.assertEqual(list(client.runs.stream("run", after=2))[0]["sequence"], 3)
            client.store.batch([{"kind": "get", "namespace": ["users"], "key": "1"}])
            client.store.search("users", query="Alice")
            client.schedules.create(assistant_id="assistant", cron="* * * * *")
            client.schedules.list()
            client.schedules.update("schedule", enabled=False)
            client.schedules.delete("schedule")

    def test_problem_details_and_join_timeout(self) -> None:
        client = LingxiGraphClient(
            "https://agents.example", api_key="key", transport=httpx.MockTransport(handler)
        )
        with self.assertRaises(LingxiGraphAPIError) as raised:
            client.request("GET", "/problem")
        self.assertEqual(raised.exception.code, "quota_exceeded")
        self.assertTrue(raised.exception.retryable)
        self.assertEqual(raised.exception.request_id, "request-1")

        def running(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"status": "running"})

        waiting = LingxiGraphClient(
            "https://agents.example", transport=httpx.MockTransport(running)
        )
        with self.assertRaises(TimeoutError):
            waiting.runs.join("run", poll_interval=0, timeout=0)
        waiting.close()
        client.close()


class AsyncSDKTests(unittest.TestCase):
    def test_async_resources_sse_and_timeout(self) -> None:
        async def scenario() -> None:
            async with AsyncLingxiGraphClient(
                "https://agents.example", transport=httpx.MockTransport(handler)
            ) as client:
                await client.graphs.list()
                await client.graphs.get("graph")
                await client.assistants.create(graph_id="graph")
                await client.assistants.list()
                await client.assistants.get("assistant")
                await client.assistants.update("assistant", name="updated")
                await client.assistants.delete("assistant")
                await client.threads.create()
                await client.threads.list()
                await client.threads.get("thread")
                await client.threads.update("thread", metadata={"team": "platform"})
                await client.threads.state("thread")
                await client.threads.history("thread")
                await client.threads.fork("thread")
                await client.threads.delete("thread")
                await client.runs.create("assistant")
                await client.runs.get("run")
                await client.runs.list("thread")
                await client.runs.cancel("run")
                await client.runs.resume("run", True)
                self.assertEqual((await client.runs.join("run", poll_interval=0))["status"], "succeeded")
                events = [event async for event in client.runs.stream("run")]
                self.assertEqual(events[0]["kind"], "run_completed")
                await client.store.batch([])
                await client.store.search("users")
                await client.schedules.create(assistant_id="assistant", cron="* * * * *")
                await client.schedules.list()
                await client.schedules.update("schedule", enabled=False)
                await client.schedules.delete("schedule")

            async def running(_request: httpx.Request) -> httpx.Response:
                return httpx.Response(200, json={"status": "running"})

            waiting = AsyncLingxiGraphClient(
                "https://agents.example", transport=httpx.MockTransport(running)
            )
            with self.assertRaises(TimeoutError):
                await waiting.runs.join("run", poll_interval=0, timeout=0)
            await waiting.close()

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
