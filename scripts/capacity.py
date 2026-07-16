"""Department-scale REST/SSE acceptance harness for a deployed LingxiGraph stack."""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import time
from collections.abc import Awaitable, Callable
from typing import Any

import httpx


def percentile(values: list[float], quantile: float = 0.95) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * quantile))]


async def timed(call: Callable[[], Awaitable[Any]]) -> tuple[Any, float]:
    started = time.perf_counter()
    return await call(), (time.perf_counter() - started) * 1000


async def wait_started(
    client: httpx.AsyncClient, run_id: str, created_at: float, deadline: float
) -> float:
    while time.perf_counter() < deadline:
        response = await client.get(f"/v1/runs/{run_id}")
        response.raise_for_status()
        if response.json()["status"] != "pending":
            return (time.perf_counter() - created_at) * 1000
        await asyncio.sleep(0.02)
    raise TimeoutError(f"run {run_id} did not start")


async def consume_stream(client: httpx.AsyncClient, run_id: str) -> float:
    started = time.perf_counter()
    async with client.stream("GET", f"/v1/runs/{run_id}/stream") as response:
        response.raise_for_status()
        async for line in response.aiter_lines():
            if line.startswith("data:"):
                return (time.perf_counter() - started) * 1000
    return (time.perf_counter() - started) * 1000


async def run(args: argparse.Namespace) -> None:
    headers = {"x-tenant-id": args.tenant, "x-roles": "tenant-admin"}
    limits = httpx.Limits(
        max_connections=max(args.concurrent_runs, args.sse_clients) + 20,
        max_keepalive_connections=max(args.concurrent_runs, 100),
    )
    async with httpx.AsyncClient(
        base_url=args.base_url,
        headers=headers,
        timeout=args.timeout,
        limits=limits,
    ) as client:
        assistant_response = await client.post(
            "/v1/assistants",
            json={"graph_id": args.graph_id, "name": "capacity-acceptance"},
        )
        assistant_response.raise_for_status()
        assistant_id = assistant_response.json()["id"]

        created: list[tuple[str, float]] = []

        async def create_run() -> float:
            created_at = time.perf_counter()
            response, latency = await timed(
                lambda: client.post(
                    "/v1/runs",
                    json={"assistant_id": assistant_id, "input": args.input},
                )
            )
            response.raise_for_status()
            created.append((response.json()["id"], created_at))
            return latency

        crud_latencies = await asyncio.gather(
            *(create_run() for _ in range(args.concurrent_runs))
        )
        deadline = time.perf_counter() + args.timeout
        start_latencies = await asyncio.gather(
            *(
                wait_started(client, run_id, created_at, deadline)
                for run_id, created_at in created
            )
        )

        stream_runs = [created[index % len(created)] for index in range(args.sse_clients)]
        event_latencies = await asyncio.gather(
            *(consume_stream(client, run_id) for run_id, _ in stream_runs)
        )

        thread = (await client.post("/v1/threads", json={})).json()
        queue_responses = await asyncio.gather(
            *(
                client.post(
                    f"/v1/threads/{thread['id']}/runs",
                    json={"assistant_id": assistant_id, "input": args.input},
                )
                for _ in range(args.queued_runs)
            )
        )
        accepted = sum(response.status_code == 202 for response in queue_responses)

    measurements = {
        "crud_p95_ms": percentile(list(crud_latencies)),
        "queue_start_p95_ms": percentile(list(start_latencies)),
        "event_p95_ms": percentile(list(event_latencies)),
        "crud_mean_ms": statistics.fmean(crud_latencies),
        "queued_accepted": accepted,
    }
    for name, value in measurements.items():
        print(f"{name}={value:.2f}" if isinstance(value, float) else f"{name}={value}")

    failures = []
    if measurements["crud_p95_ms"] >= args.crud_p95_ms:
        failures.append("CRUD p95")
    if measurements["queue_start_p95_ms"] >= args.queue_start_p95_ms:
        failures.append("queue start p95")
    if measurements["event_p95_ms"] >= args.event_p95_ms:
        failures.append("event p95")
    if accepted != args.queued_runs:
        failures.append("queued run acceptance")
    if failures:
        raise SystemExit("capacity thresholds failed: " + ", ".join(failures))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8124")
    parser.add_argument("--graph-id", default="production-support")
    parser.add_argument("--tenant", default="capacity-test")
    parser.add_argument("--concurrent-runs", type=int, default=100)
    parser.add_argument("--queued-runs", type=int, default=1000)
    parser.add_argument("--sse-clients", type=int, default=200)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--crud-p95-ms", type=float, default=250)
    parser.add_argument("--queue-start-p95-ms", type=float, default=2000)
    parser.add_argument("--event-p95-ms", type=float, default=500)
    parser.add_argument(
        "--input",
        type=json.loads,
        default={"request": "capacity acceptance", "result": ""},
        help="JSON object accepted by the deployed graph",
    )
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
