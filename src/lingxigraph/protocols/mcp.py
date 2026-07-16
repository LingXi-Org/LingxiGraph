"""Model Context Protocol tool clients and assistant gateway."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from ..runtime import Runtime
from ..server.models import RunCreate, enum_value


@dataclass(slots=True)
class MCPToolNode:
    server_url: str
    tool_name: str
    arguments: Callable[[Mapping[str, Any]], Mapping[str, Any]] = lambda state: state
    output_key: str = "tool_result"
    secret_ref: str | None = None
    secret_resolver: Callable[[str], str] | None = None
    timeout: float = 30.0

    async def __call__(self, state: Mapping[str, Any], runtime: Runtime[Any]):
        import httpx

        runtime.raise_if_cancelled()
        headers = {"content-type": "application/json"}
        if self.secret_ref:
            if self.secret_resolver is None:
                raise RuntimeError("MCP secret_ref requires a secret_resolver")
            headers["authorization"] = f"Bearer {self.secret_resolver(self.secret_ref)}"
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                self.server_url,
                headers=headers,
                json={
                    "jsonrpc": "2.0",
                    "id": str(uuid4()),
                    "method": "tools/call",
                    "params": {
                        "name": self.tool_name,
                        "arguments": dict(self.arguments(state)),
                        "_meta": {"idempotencyKey": runtime.idempotency_key},
                    },
                },
            )
            response.raise_for_status()
            body = response.json()
            if "error" in body:
                raise RuntimeError(body["error"].get("message", "MCP tool failed"))
            return {self.output_key: body.get("result")}


class MCPToolset:
    def __init__(
        self,
        server_url: str,
        *,
        secret_ref: str | None = None,
        secret_resolver: Callable[[str], str] | None = None,
    ) -> None:
        self.server_url = server_url
        self.secret_ref = secret_ref
        self.secret_resolver = secret_resolver

    async def list_tools(self) -> list[dict[str, Any]]:
        import httpx

        headers: dict[str, str] = {}
        if self.secret_ref:
            if self.secret_resolver is None:
                raise RuntimeError("MCP secret_ref requires a secret_resolver")
            headers["authorization"] = f"Bearer {self.secret_resolver(self.secret_ref)}"
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.server_url,
                headers=headers,
                json={"jsonrpc": "2.0", "id": str(uuid4()), "method": "tools/list"},
            )
            response.raise_for_status()
            return list(response.json().get("result", {}).get("tools", ()))

    def node(self, name: str, **kwargs: Any) -> MCPToolNode:
        return MCPToolNode(
            self.server_url,
            name,
            secret_ref=self.secret_ref,
            secret_resolver=self.secret_resolver,
            **kwargs,
        )


class MCPGateway:
    """Expose selected assistants as asynchronous MCP tools."""

    def __init__(self, repository, assistants: Mapping[str, str]) -> None:
        self.repository = repository
        self.assistants = dict(assistants)

    async def handle(self, tenant_id: str, request: Mapping[str, Any]) -> dict[str, Any]:
        request_id = request.get("id")
        method = request.get("method")
        try:
            result: dict[str, Any]
            if method == "initialize":
                result = {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {"name": "LingxiGraph", "version": "1.0.0"},
                }
            elif method == "tools/list":
                result = {
                    "tools": [
                        {
                            "name": name,
                            "description": f"Run LingxiGraph assistant {assistant_id}",
                            "inputSchema": {"type": "object", "additionalProperties": True},
                        }
                        for name, assistant_id in sorted(self.assistants.items())
                    ]
                }
            elif method == "tools/call":
                params = dict(request.get("params") or {})
                name = params.get("name")
                if name not in self.assistants:
                    raise KeyError(f"unknown MCP tool {name!r}")
                assistant_id = self.assistants[name]
                assistant = await self.repository.get_assistant(tenant_id, assistant_id)
                if assistant is None:
                    raise KeyError("assistant not found")
                run = await self.repository.create_run(
                    tenant_id,
                    None,
                    assistant,
                    RunCreate(
                        assistant_id=assistant_id,
                        input=dict(params.get("arguments") or {}),
                    ),
                )
                result = {
                    "content": [
                        {
                            "type": "text",
                            "text": f"LingxiGraph run accepted: {run.id}",
                        }
                    ],
                    "structuredContent": {
                        "run_id": run.id,
                        "status": enum_value(run.status),
                    },
                    "isError": False,
                }
            else:
                return self._error(request_id, -32601, "method not found")
            return {"jsonrpc": "2.0", "id": request_id, "result": result}
        except Exception as exc:
            return self._error(request_id, -32000, str(exc))

    @staticmethod
    def _error(request_id: Any, code: int, message: str) -> dict[str, Any]:
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": code, "message": message},
        }


__all__ = ["MCPGateway", "MCPToolNode", "MCPToolset"]
