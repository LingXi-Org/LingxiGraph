"""Agent2Agent protocol gateway and remote-agent node."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from ..runtime import Runtime
from ..server.models import RunCreate, ThreadCreate, enum_value

SecretResolver = Callable[[str], str]


@dataclass(slots=True)
class RemoteAgentNode:
    """Invoke a remote A2A agent as a provider-neutral graph node."""

    url: str
    input_mapper: Callable[[Mapping[str, Any]], Any] = lambda state: dict(state)
    output_key: str = "remote_agent"
    secret_ref: str | None = None
    secret_resolver: SecretResolver | None = None
    timeout: float = 60.0
    poll_interval: float = 0.5

    async def __call__(self, state: Mapping[str, Any], runtime: Runtime[Any]):
        import httpx

        headers: dict[str, str] = {
            "content-type": "application/json",
            "traceparent": str(runtime.config.get("traceparent", "")),
        }
        if self.secret_ref:
            if self.secret_resolver is None:
                raise RuntimeError("A2A secret_ref requires a secret_resolver")
            headers["authorization"] = f"Bearer {self.secret_resolver(self.secret_ref)}"
        payload = {
            "jsonrpc": "2.0",
            "id": str(uuid4()),
            "method": "message/send",
            "params": {
                "message": self.input_mapper(state),
                "metadata": {
                    "idempotency_key": runtime.idempotency_key,
                    "run_id": runtime.run_id,
                },
            },
        }
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(self.url, json=payload, headers=headers)
            response.raise_for_status()
            result = response.json().get("result", {})
            task_id = result.get("id")
            while task_id and result.get("status") in {"pending", "running"}:
                runtime.raise_if_cancelled()
                await asyncio.sleep(self.poll_interval)
                response = await client.post(
                    self.url,
                    json={
                        "jsonrpc": "2.0",
                        "id": str(uuid4()),
                        "method": "tasks/get",
                        "params": {"id": task_id},
                    },
                    headers=headers,
                )
                response.raise_for_status()
                result = response.json().get("result", {})
        return {self.output_key: result}


class A2AGateway:
    """Map A2A JSON-RPC tasks to assistants, threads, and durable runs."""

    def __init__(self, repository, registry) -> None:
        self.repository = repository
        self.registry = registry

    async def agent_card(self, tenant_id: str, assistant_id: str) -> dict[str, Any]:
        assistant = await self.repository.get_assistant(tenant_id, assistant_id)
        if assistant is None:
            raise KeyError("assistant not found")
        graph = self.registry.info(assistant.graph_id)
        return {
            "name": assistant.name or assistant.id,
            "description": assistant.metadata.get("description", "LingxiGraph assistant"),
            "url": f"/a2a/{assistant.id}",
            "version": assistant.graph_version,
            "capabilities": {"streaming": True, "pushNotifications": False},
            "defaultInputModes": ["application/json", "text/plain"],
            "defaultOutputModes": ["application/json", "text/plain"],
            "skills": assistant.metadata.get("skills", []),
            "metadata": {"schema_hash": graph.schema_hash},
        }

    async def handle(
        self,
        tenant_id: str,
        assistant_id: str,
        request: Mapping[str, Any],
    ) -> dict[str, Any]:
        request_id = request.get("id")
        method = request.get("method")
        params = dict(request.get("params") or {})
        try:
            if method == "message/send":
                result = await self._send(tenant_id, assistant_id, params)
            elif method == "tasks/get":
                run = await self.repository.get_run(tenant_id, str(params.get("id")))
                if run is None:
                    raise KeyError("task not found")
                result = self._task(run)
            elif method == "tasks/cancel":
                await self.repository.request_cancel(tenant_id, str(params.get("id")))
                run = await self.repository.get_run(tenant_id, str(params.get("id")))
                result = self._task(run) if run else None
            else:
                return self._error(request_id, -32601, "method not found")
            return {"jsonrpc": "2.0", "id": request_id, "result": result}
        except KeyError as exc:
            return self._error(request_id, -32001, str(exc))
        except Exception as exc:
            return self._error(request_id, -32000, str(exc))

    async def _send(self, tenant_id: str, assistant_id: str, params: dict[str, Any]):
        assistant = await self.repository.get_assistant(tenant_id, assistant_id)
        if assistant is None:
            raise KeyError("assistant not found")
        thread_id = params.get("contextId")
        if thread_id is None:
            thread = await self.repository.create_thread(tenant_id, ThreadCreate())
            thread_id = thread.id
        message = params.get("message")
        input_value = message if isinstance(message, dict) else {"messages": [message]}
        run = await self.repository.create_run(
            tenant_id,
            str(thread_id),
            assistant,
            RunCreate(assistant_id=assistant_id, input=input_value),
        )
        return self._task(run)

    @staticmethod
    def _task(run) -> dict[str, Any]:
        return {
            "id": run.id,
            "contextId": run.thread_id,
            "status": enum_value(run.status),
            "artifacts": ([{"data": run.output}] if run.output is not None else []),
            "metadata": run.metadata,
        }

    @staticmethod
    def _error(request_id: Any, code: int, message: str) -> dict[str, Any]:
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": code, "message": message},
        }


__all__ = ["A2AGateway", "RemoteAgentNode"]
