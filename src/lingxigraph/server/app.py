"""FastAPI Agent Server with versioned REST and replayable SSE."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from ..cache import BaseCache
from ..checkpoint import Checkpointer, InMemorySaver
from ..errors import ConcurrentRunError, EmptyInputError, IdempotencyConflictError
from ..store import BaseStore, InMemoryStore, StoreOperation
from ..types import RunStatus
from ..version import __version__
from .eventbus import EventBus, InMemoryEventBus
from .models import (
    Assistant,
    AssistantCreate,
    AssistantPatch,
    AuditRecord,
    GraphInfo,
    Run,
    RunCreate,
    Schedule,
    ScheduleCreate,
    SchedulePatch,
    StoreBatchRequest,
    Thread,
    ThreadCreate,
    ThreadPatch,
    enum_value,
)
from .registry import GraphRegistry
from .repository import TERMINAL, InMemoryRepository
from .security import Authenticator, Principal
from .worker import Worker

logger = logging.getLogger("lingxigraph.server")


def create_app(
    *,
    registry: GraphRegistry | None = None,
    repository: InMemoryRepository | None = None,
    checkpointer: Checkpointer | None = None,
    store_factory: Callable[[str], BaseStore] | None = None,
    authenticator: Authenticator | None = None,
    event_bus: EventBus | None = None,
    cache: BaseCache | None = None,
    embedded_worker: bool = False,
) -> FastAPI:
    if registry is None:
        registry = (
            GraphRegistry.from_manifest("lingxigraph.json")
            if Path("lingxigraph.json").exists()
            else GraphRegistry()
        )
    repository = repository or InMemoryRepository()
    checkpointer = checkpointer or InMemorySaver()
    shared_store = InMemoryStore()
    store_factory = store_factory or (lambda _tenant: shared_store)
    authenticator = authenticator or Authenticator()
    event_bus = event_bus or InMemoryEventBus()
    worker = Worker(
        registry,
        repository,
        checkpointer=checkpointer,
        store_factory=store_factory,
        cache=cache,
        event_bus=event_bus,
    )

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        worker_task = (
            asyncio.create_task(worker.run_forever()) if embedded_worker else None
        )
        try:
            yield
        finally:
            worker.stop()
            if worker_task is not None:
                await asyncio.gather(worker_task, return_exceptions=True)

    app = FastAPI(
        title="LingxiGraph Agent Server",
        version=__version__,
        lifespan=lifespan,
    )
    app.state.registry = registry
    app.state.repository = repository
    app.state.checkpointer = checkpointer
    app.state.store_factory = store_factory
    app.state.authenticator = authenticator
    app.state.event_bus = event_bus
    app.state.worker = worker
    app.state.sse_counts = {}
    app.state.sse_lock = asyncio.Lock()
    app.state.rate_windows = {}
    app.state.rate_lock = asyncio.Lock()

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        request.state.request_id = request.headers.get("x-request-id") or str(uuid4())
        length = request.headers.get("content-length")
        try:
            declared_length = int(length) if length is not None else None
        except ValueError:
            return _problem(request, 400, "invalid_request", "Content-Length must be an integer")
        if declared_length is not None and declared_length < 0:
            return _problem(request, 400, "invalid_request", "Content-Length must not be negative")
        if declared_length is not None and declared_length > repository.limits.max_request_bytes:
            return _problem(
                request,
                413,
                "payload_too_large",
                f"request exceeds {repository.limits.max_request_bytes} bytes",
            )
        chunks: list[bytes] = []
        actual_length = 0
        async for chunk in request.stream():
            actual_length += len(chunk)
            if actual_length > repository.limits.max_request_bytes:
                return _problem(
                    request,
                    413,
                    "payload_too_large",
                    f"request exceeds {repository.limits.max_request_bytes} bytes",
                )
            chunks.append(chunk)
        request._body = b"".join(chunks)
        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception as exc:
            logger.exception(
                "request failed",
                extra={
                    "request_id": request.state.request_id,
                    "tenant_id": getattr(request.state, "tenant_id", None),
                    "duration_ms": round((time.perf_counter() - started) * 1000, 3),
                    "error_type": type(exc).__name__,
                },
            )
            raise
        response.headers["x-request-id"] = request.state.request_id
        response.headers["x-content-type-options"] = "nosniff"
        response.headers["cache-control"] = "no-store"
        logger.info(
            "request completed",
            extra={
                "request_id": request.state.request_id,
                "tenant_id": getattr(request.state, "tenant_id", None),
                "status": response.status_code,
                "duration_ms": round((time.perf_counter() - started) * 1000, 3),
            },
        )
        return response

    @app.exception_handler(ConcurrentRunError)
    async def concurrent_error(request: Request, exc: ConcurrentRunError):
        is_quota = "quota" in str(exc)
        return _problem(
            request,
            429 if is_quota else 409,
            "quota_exceeded" if is_quota else "concurrent_run",
            str(exc),
            retryable=True,
        )

    @app.exception_handler(IdempotencyConflictError)
    async def idempotency_error(request: Request, exc: IdempotencyConflictError):
        return _problem(request, 409, "idempotency_conflict", str(exc))

    @app.exception_handler(HTTPException)
    async def http_error(request: Request, exc: HTTPException):
        codes = {
            400: "invalid_request",
            401: "unauthorized",
            403: "forbidden",
            404: "not_found",
            408: "join_timeout",
            409: "conflict",
            429: "rate_limited",
        }
        return _problem(
            request,
            exc.status_code,
            codes.get(exc.status_code, "http_error"),
            str(exc.detail),
            retryable=exc.status_code in {408, 429, 502, 503, 504},
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError):
        detail = "; ".join(
            str(error.get("msg", "invalid value")) for error in exc.errors()
        )
        return _problem(request, 422, "validation_error", detail)

    async def principal(
        request: Request,
        authorization: str | None = Header(default=None),
        x_api_key: str | None = Header(default=None),
        x_tenant_id: str | None = Header(default=None),
        x_roles: str | None = Header(default=None),
    ) -> Principal:
        try:
            value = await request.app.state.authenticator.authenticate(
                authorization,
                api_key=x_api_key,
                dev_tenant=x_tenant_id,
                dev_roles=x_roles,
            )
            request.state.tenant_id = value.tenant_id
            await _consume_rate_limit(request.app, value.tenant_id)
            return value
        except PermissionError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc

    def require(*roles: str):
        async def dependency(user: Principal = Depends(principal)) -> Principal:
            try:
                user.require(*roles)
            except PermissionError as exc:
                raise HTTPException(status_code=403, detail=str(exc)) from exc
            return user

        return dependency

    async def audit(
        user: Principal,
        action: str,
        resource_type: str,
        resource_id: str | None = None,
    ) -> None:
        await repository.audit(
            AuditRecord(
                tenant_id=user.tenant_id,
                actor=user.subject,
                action=action,
                resource_type=resource_type,
                resource_id=resource_id,
            )
        )

    @app.get("/v1/graphs", response_model=list[GraphInfo])
    async def list_graphs(_user: Principal = Depends(require("viewer", "developer"))):
        return registry.list()

    @app.get("/v1/graphs/{graph_id}", response_model=GraphInfo)
    async def get_graph(
        graph_id: str,
        graph_version: str | None = None,
        _user: Principal = Depends(require("viewer", "developer")),
    ):
        try:
            return registry.info(graph_id, graph_version)
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.get("/v1/graphs/{graph_id}/structure")
    async def get_graph_structure(
        graph_id: str,
        graph_version: str | None = None,
        xray: bool = False,
        _user: Principal = Depends(require("viewer", "developer")),
    ):
        """Return the serializable topology consumed by the embedded Studio."""
        try:
            structure = registry.get(graph_id, graph_version).get_graph(xray=xray)
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc
        return {
            "nodes": [
                {
                    "id": node.id,
                    "metadata": node.metadata,
                    "is_subgraph": node.is_subgraph,
                }
                for node in structure.nodes
            ],
            "edges": [
                {
                    "source": edge.source,
                    "target": edge.target,
                    "conditional": edge.conditional,
                    "label": edge.label,
                }
                for edge in structure.edges
            ],
        }

    @app.post("/v1/assistants", response_model=Assistant, status_code=201)
    async def create_assistant(
        body: AssistantCreate, user: Principal = Depends(require("developer"))
    ):
        try:
            graph = registry.get(body.graph_id, body.graph_version)
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc
        value = await repository.create_assistant(
            user.tenant_id, body, graph.graph_version
        )
        await audit(user, "assistants.create", "assistant", value.id)
        return value

    @app.get("/v1/assistants", response_model=list[Assistant])
    async def list_assistants(user: Principal = Depends(require("viewer", "developer"))):
        return await repository.list_assistants(user.tenant_id)

    @app.get("/v1/assistants/{assistant_id}", response_model=Assistant)
    async def get_assistant(
        assistant_id: str, user: Principal = Depends(require("viewer", "developer"))
    ):
        value = await repository.get_assistant(user.tenant_id, assistant_id)
        if value is None:
            raise HTTPException(404, "assistant not found")
        return value

    @app.patch("/v1/assistants/{assistant_id}", response_model=Assistant)
    async def patch_assistant(
        assistant_id: str,
        body: AssistantPatch,
        user: Principal = Depends(require("developer")),
    ):
        value = await repository.patch_assistant(user.tenant_id, assistant_id, body)
        if value is None:
            raise HTTPException(404, "assistant not found")
        await audit(user, "assistants.update", "assistant", assistant_id)
        return value

    @app.delete("/v1/assistants/{assistant_id}", status_code=204)
    async def delete_assistant(
        assistant_id: str, user: Principal = Depends(require("developer"))
    ):
        if not await repository.delete_assistant(user.tenant_id, assistant_id):
            raise HTTPException(404, "assistant not found")
        await audit(user, "assistants.delete", "assistant", assistant_id)
        return Response(status_code=204)

    @app.post("/v1/threads", response_model=Thread, status_code=201)
    async def create_thread(
        body: ThreadCreate, user: Principal = Depends(require("operator"))
    ):
        value = await repository.create_thread(user.tenant_id, body)
        await audit(user, "threads.create", "thread", value.id)
        return value

    @app.get("/v1/threads", response_model=list[Thread])
    async def list_threads(user: Principal = Depends(require("viewer", "operator"))):
        return await repository.list_threads(user.tenant_id)

    @app.get("/v1/threads/{thread_id}", response_model=Thread)
    async def get_thread(
        thread_id: str, user: Principal = Depends(require("viewer", "operator"))
    ):
        value = await repository.get_thread(user.tenant_id, thread_id)
        if value is None:
            raise HTTPException(404, "thread not found")
        return value

    @app.patch("/v1/threads/{thread_id}", response_model=Thread)
    async def patch_thread(
        thread_id: str,
        body: ThreadPatch,
        user: Principal = Depends(require("operator")),
    ):
        value = await repository.patch_thread(user.tenant_id, thread_id, body)
        if value is None:
            raise HTTPException(404, "thread not found")
        await audit(user, "threads.update", "thread", thread_id)
        return value

    @app.delete("/v1/threads/{thread_id}", status_code=204)
    async def delete_thread(
        thread_id: str, user: Principal = Depends(require("operator"))
    ):
        if not await repository.delete_thread(user.tenant_id, thread_id):
            raise HTTPException(404, "thread not found")
        try:
            checkpointer.delete_thread(
                {"configurable": {"tenant_id": user.tenant_id, "thread_id": thread_id}}
            )
        except AttributeError:
            pass
        await audit(user, "threads.delete", "thread", thread_id)
        return Response(status_code=204)

    @app.get("/v1/threads/{thread_id}/state")
    async def get_thread_state(
        thread_id: str,
        checkpoint_id: str | None = None,
        user: Principal = Depends(require("viewer", "operator")),
    ):
        graph = await _thread_graph(repository, registry, user.tenant_id, thread_id)
        config: dict[str, Any] = {
            "configurable": {"tenant_id": user.tenant_id, "thread_id": thread_id}
        }
        if checkpoint_id:
            config["configurable"]["checkpoint_id"] = checkpoint_id
        try:
            snapshot = graph.with_runtime(checkpointer=checkpointer).get_state(config)
        except EmptyInputError as exc:
            raise HTTPException(404, str(exc)) from exc
        return _snapshot_json(snapshot)

    @app.get("/v1/threads/{thread_id}/history")
    async def get_thread_history(
        thread_id: str,
        user: Principal = Depends(require("viewer", "operator")),
    ):
        graph = await _thread_graph(repository, registry, user.tenant_id, thread_id)
        config = {
            "configurable": {"tenant_id": user.tenant_id, "thread_id": thread_id}
        }
        return [
            _snapshot_json(item)
            for item in graph.with_runtime(checkpointer=checkpointer).get_state_history(config)
        ]

    @app.post("/v1/threads/{thread_id}/fork")
    async def fork_thread_state(
        thread_id: str,
        body: dict[str, Any],
        user: Principal = Depends(require("operator")),
    ):
        graph = await _thread_graph(repository, registry, user.tenant_id, thread_id)
        config: dict[str, Any] = {
            "configurable": {"tenant_id": user.tenant_id, "thread_id": thread_id}
        }
        if body.get("checkpoint_id"):
            config["configurable"]["checkpoint_id"] = body["checkpoint_id"]
        fork_config = graph.with_runtime(checkpointer=checkpointer).fork(
            config,
            body.get("values", {}),
            as_node=body.get("as_node"),
        )
        await audit(user, "threads.fork", "thread", thread_id)
        return fork_config

    @app.post("/v1/threads/{thread_id}/runs", response_model=Run, status_code=202)
    async def create_thread_run(
        thread_id: str,
        body: RunCreate,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
        user: Principal = Depends(require("operator")),
    ):
        if await repository.get_thread(user.tenant_id, thread_id) is None:
            raise HTTPException(404, "thread not found")
        assistant = await repository.get_assistant(user.tenant_id, body.assistant_id)
        if assistant is None:
            raise HTTPException(404, "assistant not found")
        _validate_idempotency_key(idempotency_key)
        value = await repository.create_run(
            user.tenant_id,
            thread_id,
            assistant,
            body,
            idempotency_key=idempotency_key,
            request_digest=_run_digest(thread_id, assistant, body),
        )
        await audit(user, "runs.create", "run", value.id)
        return value

    @app.post("/v1/runs", response_model=Run, status_code=202)
    async def create_stateless_run(
        body: RunCreate,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
        user: Principal = Depends(require("operator")),
    ):
        assistant = await repository.get_assistant(user.tenant_id, body.assistant_id)
        if assistant is None:
            raise HTTPException(404, "assistant not found")
        _validate_idempotency_key(idempotency_key)
        value = await repository.create_run(
            user.tenant_id,
            None,
            assistant,
            body,
            idempotency_key=idempotency_key,
            request_digest=_run_digest(None, assistant, body),
        )
        await audit(user, "runs.create_stateless", "run", value.id)
        return value

    @app.get("/v1/threads/{thread_id}/runs", response_model=list[Run])
    async def list_thread_runs(
        thread_id: str, user: Principal = Depends(require("viewer", "operator"))
    ):
        return await repository.list_runs(user.tenant_id, thread_id=thread_id)

    @app.get("/v1/runs/{run_id}", response_model=Run)
    async def get_run(
        run_id: str, user: Principal = Depends(require("viewer", "operator"))
    ):
        value = await repository.get_run(user.tenant_id, run_id)
        if value is None:
            raise HTTPException(404, "run not found")
        return value

    @app.get("/v1/runs/{run_id}/join", response_model=Run)
    async def join_run(
        run_id: str,
        timeout: float = 30.0,
        user: Principal = Depends(require("viewer", "operator")),
    ):
        if timeout <= 0 or timeout > 300:
            raise HTTPException(400, "timeout must be greater than 0 and at most 300")
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            value = await repository.get_run(user.tenant_id, run_id)
            if value is None:
                raise HTTPException(404, "run not found")
            if enum_value(value.status) in TERMINAL | {RunStatus.PAUSED.value}:
                return value
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise HTTPException(408, "run did not complete before join timeout")
            waiter = getattr(repository, "wait_for_change", None)
            if waiter is None:
                await asyncio.sleep(min(0.25, remaining))
            else:
                await waiter(min(1.0, remaining))

    @app.post("/v1/runs/{run_id}/cancel", response_model=Run)
    async def cancel_run(run_id: str, user: Principal = Depends(require("operator"))):
        if not await repository.request_cancel(user.tenant_id, run_id):
            raise HTTPException(409, "run cannot be cancelled")
        await audit(user, "runs.cancel", "run", run_id)
        value = await repository.get_run(user.tenant_id, run_id)
        assert value is not None
        return value

    @app.post("/v1/runs/{run_id}/resume", response_model=Run, status_code=202)
    async def resume_run(
        run_id: str,
        body: dict[str, Any],
        user: Principal = Depends(require("operator")),
    ):
        previous = await repository.get_run(user.tenant_id, run_id)
        if previous is None or enum_value(previous.status) != RunStatus.PAUSED.value:
            raise HTTPException(409, "only paused runs can be resumed")
        assistant = await repository.get_assistant(user.tenant_id, previous.assistant_id)
        assert assistant is not None
        # Resume the immutable execution contract captured by the paused run,
        # even if the mutable assistant has been edited or redeployed since.
        assistant = assistant.model_copy(
            update={
                "graph_id": previous.graph_id,
                "graph_version": previous.graph_version,
                "context": previous.context,
                "config": previous.config,
            },
            deep=True,
        )
        request = RunCreate(
            assistant_id=assistant.id,
            resume=body.get("resume"),
            update=body.get("update"),
            goto=body.get("goto"),
            durability=previous.durability,
            metadata={"resumed_from_run_id": previous.id},
        )
        value = await repository.create_run(
            user.tenant_id, previous.thread_id, assistant, request
        )
        await audit(user, "runs.resume", "run", value.id)
        return value

    @app.post("/v1/runs/{run_id}/redrive", response_model=Run, status_code=202)
    async def redrive_run(
        run_id: str, user: Principal = Depends(require("operator"))
    ):
        value = await repository.redrive_run(user.tenant_id, run_id)
        if value is None:
            raise HTTPException(409, "only failed or dead-letter runs can be redriven")
        await audit(user, "runs.redrive", "run", run_id)
        return value

    @app.get("/v1/runs/{run_id}/stream")
    async def stream_run(
        run_id: str,
        request: Request,
        last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
        user: Principal = Depends(require("viewer", "operator")),
    ):
        if await repository.get_run(user.tenant_id, run_id) is None:
            raise HTTPException(404, "run not found")
        async with app.state.sse_lock:
            current = app.state.sse_counts.get(user.tenant_id, 0)
            if current >= repository.limits.max_sse_connections:
                raise HTTPException(429, "tenant SSE connection quota exceeded")
            app.state.sse_counts[user.tenant_id] = current + 1

        async def generate() -> AsyncIterator[str]:
            sequence = int(last_event_id or 0)
            try:
                while not await request.is_disconnected():
                    events = await repository.list_events(
                        user.tenant_id, run_id, after=sequence
                    )
                    for event in events:
                        sequence = event.sequence
                        payload = json.dumps(
                            event.model_dump(mode="json"),
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )
                        yield f"id: {sequence}\nevent: {event.kind}\ndata: {payload}\n\n"
                    run = await repository.get_run(user.tenant_id, run_id)
                    if run is None or enum_value(run.status) in TERMINAL | {
                        RunStatus.PAUSED.value
                    }:
                        return
                    if not events:
                        yield ": heartbeat\n\n"
                    await event_bus.wait(user.tenant_id, run_id, timeout=15.0)
            finally:
                async with app.state.sse_lock:
                    app.state.sse_counts[user.tenant_id] = max(
                        0, app.state.sse_counts.get(user.tenant_id, 1) - 1
                    )

        return StreamingResponse(generate(), media_type="text/event-stream")

    @app.post("/v1/store/batch")
    async def store_batch(
        body: StoreBatchRequest, user: Principal = Depends(require("operator"))
    ):
        operations = [StoreOperation(**operation) for operation in body.operations]
        store = store_factory(user.tenant_id)
        batch = getattr(store, "abatch", None)
        values = await batch(operations) if batch else store.batch(operations)
        return {"results": [_jsonable(value) for value in values]}

    @app.get("/v1/store/search")
    async def search_store(
        namespace: str,
        query: str | None = None,
        limit: int = 10,
        offset: int = 0,
        user: Principal = Depends(require("viewer", "operator")),
    ):
        store = store_factory(user.tenant_id)
        prefix = tuple(part for part in namespace.split("/") if part)
        search = getattr(store, "asearch", None)
        values = (
            await search(prefix, query=query, limit=limit, offset=offset)
            if search
            else store.search(prefix, query=query, limit=limit, offset=offset)
        )
        return {"items": [_jsonable(value) for value in values]}

    @app.post("/v1/schedules", response_model=Schedule, status_code=201)
    async def create_schedule(
        body: ScheduleCreate, user: Principal = Depends(require("operator"))
    ):
        if await repository.get_assistant(user.tenant_id, body.assistant_id) is None:
            raise HTTPException(404, "assistant not found")
        value = await repository.create_schedule(user.tenant_id, body)
        await audit(user, "schedules.create", "schedule", value.id)
        return value

    @app.get("/v1/schedules", response_model=list[Schedule])
    async def list_schedules(user: Principal = Depends(require("viewer", "operator"))):
        return await repository.list_schedules(user.tenant_id)

    @app.patch("/v1/schedules/{schedule_id}", response_model=Schedule)
    async def patch_schedule(
        schedule_id: str,
        body: SchedulePatch,
        user: Principal = Depends(require("operator")),
    ):
        value = await repository.patch_schedule(user.tenant_id, schedule_id, body)
        if value is None:
            raise HTTPException(404, "schedule not found")
        await audit(user, "schedules.update", "schedule", schedule_id)
        return value

    @app.delete("/v1/schedules/{schedule_id}", status_code=204)
    async def delete_schedule(
        schedule_id: str, user: Principal = Depends(require("operator"))
    ):
        if not await repository.delete_schedule(user.tenant_id, schedule_id):
            raise HTTPException(404, "schedule not found")
        await audit(user, "schedules.delete", "schedule", schedule_id)
        return Response(status_code=204)

    @app.get("/a2a/{assistant_id}/.well-known/agent-card.json")
    async def a2a_agent_card(
        assistant_id: str, user: Principal = Depends(require("viewer", "operator"))
    ):
        from ..protocols.a2a import A2AGateway

        try:
            return await A2AGateway(repository, registry).agent_card(
                user.tenant_id, assistant_id
            )
        except KeyError as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.post("/a2a/{assistant_id}")
    async def a2a_jsonrpc(
        assistant_id: str,
        body: dict[str, Any],
        user: Principal = Depends(require("operator")),
    ):
        from ..protocols.a2a import A2AGateway

        return await A2AGateway(repository, registry).handle(
            user.tenant_id, assistant_id, body
        )

    @app.post("/mcp")
    async def mcp_jsonrpc(
        body: dict[str, Any], user: Principal = Depends(require("operator"))
    ):
        from ..protocols.mcp import MCPGateway

        assistants = await repository.list_assistants(user.tenant_id)
        exposed = {
            str(item.metadata.get("mcp_tool_name") or item.name or item.id): item.id
            for item in assistants
            if item.metadata.get("mcp_expose", False)
        }
        return await MCPGateway(repository, exposed).handle(user.tenant_id, body)

    @app.get("/health")
    async def health():
        return {"status": "ok", "version": app.version}

    @app.get("/ready")
    async def ready():
        graph_count = len(registry.list())
        repository_ready = await repository.healthcheck()
        worker_ready = worker.ready if embedded_worker else True
        if not repository_ready or not worker_ready or graph_count == 0:
            raise HTTPException(
                503,
                {
                    "status": "not_ready",
                    "repository": repository_ready,
                    "worker": worker_ready,
                    "graphs": graph_count,
                },
            )
        return {
            "status": "ready",
            "repository": repository_ready,
            "worker": worker_ready,
            "graphs": graph_count,
        }

    @app.get("/metrics", response_class=Response)
    async def metrics(user: Principal = Depends(require("viewer"))):
        stats = await repository.stats(user.tenant_id)
        lines = [
            "# HELP lingxigraph_graphs Loaded graph definitions",
            "# TYPE lingxigraph_graphs gauge",
            f"lingxigraph_graphs {len(registry.list())}",
            "# HELP lingxigraph_runs Runs by lifecycle status for the authenticated tenant",
            "# TYPE lingxigraph_runs gauge",
            *(
                f'lingxigraph_runs{{status="{status}"}} {count}'
                for status, count in stats["runs"].items()
            ),
            "# HELP lingxigraph_queue_depth Pending runs for the authenticated tenant",
            "# TYPE lingxigraph_queue_depth gauge",
            f"lingxigraph_queue_depth {stats['runs']['pending']}",
            "# HELP lingxigraph_active_runs Active runs for the authenticated tenant",
            "# TYPE lingxigraph_active_runs gauge",
            f"lingxigraph_active_runs {stats['runs']['running'] + stats['runs']['cancelling']}",
            "# HELP lingxigraph_run_events Persisted run events for the authenticated tenant",
            "# TYPE lingxigraph_run_events gauge",
            f"lingxigraph_run_events {stats['events']}",
            "# HELP lingxigraph_sse_clients Active SSE clients on this API replica",
            "# TYPE lingxigraph_sse_clients gauge",
            f"lingxigraph_sse_clients {app.state.sse_counts.get(user.tenant_id, 0)}",
        ]
        return Response("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")

    if embedded_worker:
        studio_dir = Path(__file__).resolve().parent.parent / "studio"
        if studio_dir.is_dir():
            app.mount("/studio", StaticFiles(directory=studio_dir, html=True), name="studio")

    return app


async def _thread_graph(repository, registry, tenant_id: str, thread_id: str):
    runs = await repository.list_runs(tenant_id, thread_id=thread_id)
    if not runs:
        raise HTTPException(404, "thread has no graph state")
    return registry.get(runs[0].graph_id, runs[0].graph_version)


def _problem(
    request: Request,
    status_code: int,
    code: str,
    detail: str,
    *,
    retryable: bool = False,
) -> JSONResponse:
    return JSONResponse(
        {
            "type": "about:blank",
            "title": code.replace("_", " ").title(),
            "status": status_code,
            "detail": detail,
            "code": code,
            "request_id": getattr(request.state, "request_id", "unknown"),
            "retryable": retryable,
        },
        status_code=status_code,
        media_type="application/problem+json",
    )


def _validate_idempotency_key(value: str | None) -> None:
    if value is not None and (not value.strip() or len(value) > 255):
        raise HTTPException(400, "Idempotency-Key must contain 1 to 255 characters")


def _run_digest(thread_id: str | None, assistant: Assistant, body: RunCreate) -> str:
    payload = {
        "thread_id": thread_id,
        "assistant_id": assistant.id,
        "graph_id": assistant.graph_id,
        "graph_version": assistant.graph_version,
        "request": body.model_dump(mode="json"),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


async def _consume_rate_limit(app: FastAPI, tenant_id: str) -> None:
    now = time.monotonic()
    limit = app.state.repository.limits.max_requests_per_minute
    async with app.state.rate_lock:
        started, count = app.state.rate_windows.get(tenant_id, (now, 0))
        if now - started >= 60:
            started, count = now, 0
        if count >= limit:
            raise HTTPException(429, "tenant request-rate quota exceeded")
        app.state.rate_windows[tenant_id] = (started, count + 1)


def _snapshot_json(snapshot) -> dict[str, Any]:
    import dataclasses

    return _jsonable(dataclasses.asdict(snapshot))


def _jsonable(value: Any) -> Any:
    import dataclasses
    from datetime import date, datetime
    from enum import Enum

    if dataclasses.is_dataclass(value):
        return _jsonable(dataclasses.asdict(value))  # type: ignore[arg-type]
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    return value


__all__ = ["create_app"]
