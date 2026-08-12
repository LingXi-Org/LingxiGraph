"""Model Context Protocol tool clients and assistant gateway."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from ..cache_first import build_tool_catalog_fingerprint
from ..runtime import Runtime
from ..server.models import RunCreate, enum_value
from ..tools import ToolSpec
from ..version import __version__


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
        timeout: float = 30.0,
    ) -> None:
        self.server_url = server_url
        self.secret_ref = secret_ref
        self.secret_resolver = secret_resolver
        if timeout <= 0:
            raise ValueError("MCP timeout must be positive")
        self.timeout = timeout
        self._last_tools: tuple[dict[str, Any], ...] = ()
        self._last_catalog_fingerprint: str | None = None

    def _headers(self) -> dict[str, str]:
        headers: dict[str, str] = {"content-type": "application/json"}
        if self.secret_ref:
            if self.secret_resolver is None:
                raise RuntimeError("MCP secret_ref requires a secret_resolver")
            headers["authorization"] = f"Bearer {self.secret_resolver(self.secret_ref)}"
        return headers

    async def list_tools(self) -> list[dict[str, Any]]:
        import httpx

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                self.server_url,
                headers=self._headers(),
                json={"jsonrpc": "2.0", "id": str(uuid4()), "method": "tools/list"},
            )
            response.raise_for_status()
            tools = [
                dict(item)
                for item in response.json().get("result", {}).get("tools", ())
                if isinstance(item, Mapping) and item.get("name")
            ]
            tools.sort(key=lambda item: str(item["name"]))
            self._last_tools = tuple(tools)
            self._last_catalog_fingerprint = build_tool_catalog_fingerprint(tools).fingerprint
            return tools

    @property
    def catalog_fingerprint(self) -> str | None:
        return self._last_catalog_fingerprint

    async def call_tool(self, name: str, arguments: Mapping[str, Any] | None = None) -> Any:
        import httpx

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                self.server_url,
                headers=self._headers(),
                json={
                    "jsonrpc": "2.0",
                    "id": str(uuid4()),
                    "method": "tools/call",
                    "params": {"name": name, "arguments": dict(arguments or {})},
                },
            )
            response.raise_for_status()
            body = response.json()
            if "error" in body:
                raise RuntimeError(body["error"].get("message", "MCP tool failed"))
            return body.get("result")

    def progressive_tools(self) -> tuple[ToolSpec, ...]:
        """Return fixed discovery/call tools for a large remote catalog."""

        async def mcp_search(query: str) -> str:
            """Search the current MCP catalog by name or description."""

            needle = query.casefold().strip()
            tools = await self.list_tools()
            matches = [
                item
                for item in tools
                if not needle
                or needle in str(item.get("name", "")).casefold()
                or needle in str(item.get("description", "")).casefold()
            ]
            return json.dumps(matches[:20], ensure_ascii=False)

        async def mcp_describe(name: str) -> str:
            """Return one MCP tool schema by its exact name."""

            tools = await self.list_tools()
            match = next((item for item in tools if item.get("name") == name), None)
            if match is None:
                raise KeyError(f"unknown MCP tool {name!r}")
            return json.dumps(match, ensure_ascii=False)

        async def mcp_call(name: str, arguments: Mapping[str, Any]) -> str:
            """Call a discovered MCP tool with dynamic arguments."""

            result = await self.call_tool(name, arguments)
            return json.dumps(result, ensure_ascii=False, default=str)

        async def mcp_refresh_catalog() -> str:
            """Refresh the MCP catalog and return its fingerprint and names."""

            tools = await self.list_tools()
            return json.dumps(
                {
                    "fingerprint": self.catalog_fingerprint,
                    "tool_names": [str(item["name"]) for item in tools],
                },
                ensure_ascii=False,
            )

        object_schema = {"type": "object", "additionalProperties": True}
        return (
            ToolSpec(
                "mcp_search",
                "Search the MCP catalog without sending every remote schema.",
                {
                    "type": "object",
                    "properties": {"query": {"type": "string"}},
                    "required": ["query"],
                    "additionalProperties": False,
                },
                mcp_search,
                read_only=True,
            ),
            ToolSpec(
                "mcp_describe",
                "Describe one discovered MCP tool.",
                {
                    "type": "object",
                    "properties": {"name": {"type": "string"}},
                    "required": ["name"],
                    "additionalProperties": False,
                },
                mcp_describe,
                read_only=True,
            ),
            ToolSpec(
                "mcp_call",
                "Call a discovered MCP tool with dynamic arguments.",
                {
                    "type": "object",
                    "properties": {"name": {"type": "string"}, "arguments": object_schema},
                    "required": ["name", "arguments"],
                    "additionalProperties": False,
                },
                mcp_call,
            ),
            ToolSpec(
                "mcp_refresh_catalog",
                "Refresh the MCP catalog fingerprint.",
                {"type": "object", "properties": {}, "additionalProperties": False},
                mcp_refresh_catalog,
                read_only=True,
            ),
        )

    async def tools_for_prefix(self, *, max_tools: int = 64) -> tuple[Any, ...]:
        """Use full schemas below a limit and fixed discovery above it."""

        if max_tools <= 0:
            raise ValueError("max_tools must be positive")
        tools = await self.list_tools()
        if len(tools) > max_tools:
            return self.progressive_tools()
        return tuple(tools)

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
                    "serverInfo": {"name": "LingxiGraph", "version": __version__},
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
