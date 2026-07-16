"""Typed Python client mirroring Agent Server's versioned REST resources."""

from __future__ import annotations

import asyncio
import builtins
import json
import time
from collections.abc import AsyncIterator, Iterator, Mapping
from typing import Any


class LingxiGraphAPIError(RuntimeError):
    def __init__(
        self,
        status_code: int,
        code: str,
        detail: str,
        *,
        request_id: str | None = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(f"{code}: {detail}")
        self.status_code = status_code
        self.code = code
        self.detail = detail
        self.request_id = request_id
        self.retryable = retryable


def _raise_for_problem(response: Any) -> None:
    if response.is_success:
        return
    try:
        problem = response.json()
    except Exception:
        problem = {}
    raise LingxiGraphAPIError(
        response.status_code,
        str(problem.get("code") or f"http_{response.status_code}"),
        str(problem.get("detail") or response.text),
        request_id=problem.get("request_id") or response.headers.get("x-request-id"),
        retryable=bool(problem.get("retryable", False)),
    )


class _AsyncResource:
    def __init__(self, client: AsyncLingxiGraphClient) -> None:
        self._client = client


class _AsyncGraphs(_AsyncResource):
    async def list(self) -> list[dict[str, Any]]:
        return await self._client.request("GET", "/v1/graphs")

    async def get(self, graph_id: str) -> dict[str, Any]:
        return await self._client.request("GET", f"/v1/graphs/{graph_id}")


class _AsyncAssistants(_AsyncResource):
    async def create(self, **values: Any) -> dict[str, Any]:
        return await self._client.request("POST", "/v1/assistants", json=values)

    async def list(self) -> list[dict[str, Any]]:
        return await self._client.request("GET", "/v1/assistants")

    async def get(self, assistant_id: str) -> dict[str, Any]:
        return await self._client.request("GET", f"/v1/assistants/{assistant_id}")

    async def update(self, assistant_id: str, **values: Any) -> dict[str, Any]:
        return await self._client.request(
            "PATCH", f"/v1/assistants/{assistant_id}", json=values
        )

    async def delete(self, assistant_id: str) -> None:
        await self._client.request("DELETE", f"/v1/assistants/{assistant_id}")


class _AsyncThreads(_AsyncResource):
    async def create(
        self, *, metadata: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        return await self._client.request(
            "POST", "/v1/threads", json={"metadata": dict(metadata or {})}
        )

    async def list(self) -> list[dict[str, Any]]:
        return await self._client.request("GET", "/v1/threads")

    async def get(self, thread_id: str) -> dict[str, Any]:
        return await self._client.request("GET", f"/v1/threads/{thread_id}")

    async def update(
        self, thread_id: str, *, metadata: Mapping[str, Any]
    ) -> dict[str, Any]:
        return await self._client.request(
            "PATCH", f"/v1/threads/{thread_id}", json={"metadata": dict(metadata)}
        )

    async def state(
        self, thread_id: str, *, checkpoint_id: str | None = None
    ) -> dict[str, Any]:
        params = {"checkpoint_id": checkpoint_id} if checkpoint_id else None
        return await self._client.request(
            "GET", f"/v1/threads/{thread_id}/state", params=params
        )

    async def history(self, thread_id: str) -> builtins.list[dict[str, Any]]:
        return await self._client.request("GET", f"/v1/threads/{thread_id}/history")

    async def fork(self, thread_id: str, **values: Any) -> dict[str, Any]:
        return await self._client.request(
            "POST", f"/v1/threads/{thread_id}/fork", json=values
        )

    async def delete(self, thread_id: str) -> None:
        await self._client.request("DELETE", f"/v1/threads/{thread_id}")


class _AsyncRuns(_AsyncResource):
    async def create(
        self,
        assistant_id: str,
        *,
        input: Mapping[str, Any] | None = None,
        thread_id: str | None = None,
        **options: Any,
    ) -> dict[str, Any]:
        path = f"/v1/threads/{thread_id}/runs" if thread_id else "/v1/runs"
        return await self._client.request(
            "POST", path, json={"assistant_id": assistant_id, "input": input, **options}
        )

    async def get(self, run_id: str) -> dict[str, Any]:
        return await self._client.request("GET", f"/v1/runs/{run_id}")

    async def list(self, thread_id: str) -> list[dict[str, Any]]:
        return await self._client.request("GET", f"/v1/threads/{thread_id}/runs")

    async def cancel(self, run_id: str) -> dict[str, Any]:
        return await self._client.request("POST", f"/v1/runs/{run_id}/cancel")

    async def resume(self, run_id: str, resume: Any, **values: Any) -> dict[str, Any]:
        return await self._client.request(
            "POST", f"/v1/runs/{run_id}/resume", json={"resume": resume, **values}
        )

    async def join(
        self, run_id: str, *, poll_interval: float = 0.25, timeout: float | None = None
    ) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout if timeout is not None else None
        if timeout is not None and timeout <= 0:
            raise TimeoutError(f"run {run_id} did not finish before timeout")
        while True:
            remaining = max(0.0, deadline - loop.time()) if deadline is not None else 30.0
            server_timeout = min(30.0, remaining) if deadline is not None else 30.0
            try:
                return await self._client.request(
                    "GET",
                    f"/v1/runs/{run_id}/join",
                    params={"timeout": server_timeout},
                    timeout=server_timeout + 5.0,
                )
            except LingxiGraphAPIError as exc:
                if exc.code != "join_timeout":
                    raise
                if deadline is not None and loop.time() >= deadline:
                    raise TimeoutError(
                        f"run {run_id} did not finish before timeout"
                    ) from exc
                await asyncio.sleep(poll_interval)

    async def stream(
        self, run_id: str, *, after: int = 0
    ) -> AsyncIterator[dict[str, Any]]:
        headers = {"Last-Event-ID": str(after)} if after else None
        async with self._client._client.stream(
            "GET", f"/v1/runs/{run_id}/stream", headers=headers
        ) as response:
            _raise_for_problem(response)
            event: dict[str, str] = {}
            async for line in response.aiter_lines():
                if not line:
                    if "data" in event:
                        yield json.loads(event["data"])
                    event = {}
                    continue
                if line.startswith(":"):
                    continue
                key, _, value = line.partition(":")
                event[key] = value.lstrip()


class _AsyncStore(_AsyncResource):
    async def batch(self, operations: list[dict[str, Any]]) -> dict[str, Any]:
        return await self._client.request(
            "POST", "/v1/store/batch", json={"operations": operations}
        )

    async def search(
        self,
        namespace: str,
        *,
        query: str | None = None,
        limit: int = 10,
        offset: int = 0,
    ) -> dict[str, Any]:
        return await self._client.request(
            "GET",
            "/v1/store/search",
            params={
                "namespace": namespace,
                "query": query,
                "limit": limit,
                "offset": offset,
            },
        )


class _AsyncSchedules(_AsyncResource):
    async def create(self, **values: Any) -> dict[str, Any]:
        return await self._client.request("POST", "/v1/schedules", json=values)

    async def list(self) -> list[dict[str, Any]]:
        return await self._client.request("GET", "/v1/schedules")

    async def update(self, schedule_id: str, **values: Any) -> dict[str, Any]:
        return await self._client.request(
            "PATCH", f"/v1/schedules/{schedule_id}", json=values
        )

    async def delete(self, schedule_id: str) -> None:
        await self._client.request("DELETE", f"/v1/schedules/{schedule_id}")


class AsyncLingxiGraphClient:
    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        api_key: str | None = None,
        timeout: float = 30.0,
        transport: Any | None = None,
    ) -> None:
        try:
            import httpx
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("install lingxigraph[sdk] to use the HTTP client") from exc
        headers = {"accept": "application/json"}
        if token:
            headers["authorization"] = f"Bearer {token}"
        if api_key:
            headers["x-api-key"] = api_key
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers=headers,
            timeout=timeout,
            transport=transport,
        )
        self.graphs = _AsyncGraphs(self)
        self.assistants = _AsyncAssistants(self)
        self.threads = _AsyncThreads(self)
        self.runs = _AsyncRuns(self)
        self.store = _AsyncStore(self)
        self.schedules = _AsyncSchedules(self)

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> AsyncLingxiGraphClient:
        return self

    async def __aexit__(self, *_exc_info: Any) -> None:
        await self.close()

    async def request(self, method: str, path: str, **kwargs: Any) -> Any:
        response = await self._client.request(method, path, **kwargs)
        _raise_for_problem(response)
        return None if response.status_code == 204 else response.json()


class _SyncResource:
    def __init__(self, client: LingxiGraphClient) -> None:
        self._client = client


class _SyncGraphs(_SyncResource):
    def list(self) -> list[dict[str, Any]]:
        return self._client.request("GET", "/v1/graphs")

    def get(self, graph_id: str) -> dict[str, Any]:
        return self._client.request("GET", f"/v1/graphs/{graph_id}")


class _SyncAssistants(_SyncResource):
    def create(self, **values: Any) -> dict[str, Any]:
        return self._client.request("POST", "/v1/assistants", json=values)

    def list(self) -> list[dict[str, Any]]:
        return self._client.request("GET", "/v1/assistants")

    def get(self, assistant_id: str) -> dict[str, Any]:
        return self._client.request("GET", f"/v1/assistants/{assistant_id}")

    def update(self, assistant_id: str, **values: Any) -> dict[str, Any]:
        return self._client.request(
            "PATCH", f"/v1/assistants/{assistant_id}", json=values
        )

    def delete(self, assistant_id: str) -> None:
        self._client.request("DELETE", f"/v1/assistants/{assistant_id}")


class _SyncThreads(_SyncResource):
    def create(self, *, metadata: Mapping[str, Any] | None = None) -> dict[str, Any]:
        return self._client.request(
            "POST", "/v1/threads", json={"metadata": dict(metadata or {})}
        )

    def list(self) -> list[dict[str, Any]]:
        return self._client.request("GET", "/v1/threads")

    def get(self, thread_id: str) -> dict[str, Any]:
        return self._client.request("GET", f"/v1/threads/{thread_id}")

    def update(
        self, thread_id: str, *, metadata: Mapping[str, Any]
    ) -> dict[str, Any]:
        return self._client.request(
            "PATCH", f"/v1/threads/{thread_id}", json={"metadata": dict(metadata)}
        )

    def state(
        self, thread_id: str, *, checkpoint_id: str | None = None
    ) -> dict[str, Any]:
        params = {"checkpoint_id": checkpoint_id} if checkpoint_id else None
        return self._client.request(
            "GET", f"/v1/threads/{thread_id}/state", params=params
        )

    def history(self, thread_id: str) -> builtins.list[dict[str, Any]]:
        return self._client.request("GET", f"/v1/threads/{thread_id}/history")

    def fork(self, thread_id: str, **values: Any) -> dict[str, Any]:
        return self._client.request(
            "POST", f"/v1/threads/{thread_id}/fork", json=values
        )

    def delete(self, thread_id: str) -> None:
        self._client.request("DELETE", f"/v1/threads/{thread_id}")


class _SyncRuns(_SyncResource):
    def create(
        self,
        assistant_id: str,
        *,
        input: Mapping[str, Any] | None = None,
        thread_id: str | None = None,
        **options: Any,
    ) -> dict[str, Any]:
        path = f"/v1/threads/{thread_id}/runs" if thread_id else "/v1/runs"
        return self._client.request(
            "POST", path, json={"assistant_id": assistant_id, "input": input, **options}
        )

    def get(self, run_id: str) -> dict[str, Any]:
        return self._client.request("GET", f"/v1/runs/{run_id}")

    def list(self, thread_id: str) -> list[dict[str, Any]]:
        return self._client.request("GET", f"/v1/threads/{thread_id}/runs")

    def cancel(self, run_id: str) -> dict[str, Any]:
        return self._client.request("POST", f"/v1/runs/{run_id}/cancel")

    def resume(self, run_id: str, resume: Any, **values: Any) -> dict[str, Any]:
        return self._client.request(
            "POST", f"/v1/runs/{run_id}/resume", json={"resume": resume, **values}
        )

    def join(
        self, run_id: str, *, poll_interval: float = 0.25, timeout: float | None = None
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout if timeout is not None else None
        if timeout is not None and timeout <= 0:
            raise TimeoutError(f"run {run_id} did not finish before timeout")
        while True:
            remaining = (
                max(0.0, deadline - time.monotonic()) if deadline is not None else 30.0
            )
            server_timeout = min(30.0, remaining) if deadline is not None else 30.0
            try:
                return self._client.request(
                    "GET",
                    f"/v1/runs/{run_id}/join",
                    params={"timeout": server_timeout},
                    timeout=server_timeout + 5.0,
                )
            except LingxiGraphAPIError as exc:
                if exc.code != "join_timeout":
                    raise
                if deadline is not None and time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"run {run_id} did not finish before timeout"
                    ) from exc
                time.sleep(poll_interval)

    def stream(self, run_id: str, *, after: int = 0) -> Iterator[dict[str, Any]]:
        headers = {"Last-Event-ID": str(after)} if after else None
        with self._client._client.stream(
            "GET", f"/v1/runs/{run_id}/stream", headers=headers
        ) as response:
            _raise_for_problem(response)
            event: dict[str, str] = {}
            for line in response.iter_lines():
                if not line:
                    if "data" in event:
                        yield json.loads(event["data"])
                    event = {}
                    continue
                if line.startswith(":"):
                    continue
                key, _, value = line.partition(":")
                event[key] = value.lstrip()


class _SyncStore(_SyncResource):
    def batch(self, operations: list[dict[str, Any]]) -> dict[str, Any]:
        return self._client.request(
            "POST", "/v1/store/batch", json={"operations": operations}
        )

    def search(
        self,
        namespace: str,
        *,
        query: str | None = None,
        limit: int = 10,
        offset: int = 0,
    ) -> dict[str, Any]:
        return self._client.request(
            "GET",
            "/v1/store/search",
            params={
                "namespace": namespace,
                "query": query,
                "limit": limit,
                "offset": offset,
            },
        )


class _SyncSchedules(_SyncResource):
    def create(self, **values: Any) -> dict[str, Any]:
        return self._client.request("POST", "/v1/schedules", json=values)

    def list(self) -> list[dict[str, Any]]:
        return self._client.request("GET", "/v1/schedules")

    def update(self, schedule_id: str, **values: Any) -> dict[str, Any]:
        return self._client.request(
            "PATCH", f"/v1/schedules/{schedule_id}", json=values
        )

    def delete(self, schedule_id: str) -> None:
        self._client.request("DELETE", f"/v1/schedules/{schedule_id}")


class LingxiGraphClient:
    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        api_key: str | None = None,
        timeout: float = 30.0,
        transport: Any | None = None,
    ) -> None:
        try:
            import httpx
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("install lingxigraph[sdk] to use the HTTP client") from exc
        headers = {"accept": "application/json"}
        if token:
            headers["authorization"] = f"Bearer {token}"
        if api_key:
            headers["x-api-key"] = api_key
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"),
            headers=headers,
            timeout=timeout,
            transport=transport,
        )
        self.graphs = _SyncGraphs(self)
        self.assistants = _SyncAssistants(self)
        self.threads = _SyncThreads(self)
        self.runs = _SyncRuns(self)
        self.store = _SyncStore(self)
        self.schedules = _SyncSchedules(self)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> LingxiGraphClient:
        return self

    def __exit__(self, *_exc_info: Any) -> None:
        self.close()

    def request(self, method: str, path: str, **kwargs: Any) -> Any:
        response = self._client.request(method, path, **kwargs)
        _raise_for_problem(response)
        return None if response.status_code == 204 else response.json()


__all__ = [
    "AsyncLingxiGraphClient",
    "LingxiGraphAPIError",
    "LingxiGraphClient",
]
