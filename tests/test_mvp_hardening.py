import asyncio
import json
import sys
import tempfile
import types
import unittest
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, Literal, TypedDict

import httpx
from fastapi.testclient import TestClient
from pydantic import BaseModel

from lingxigraph import (
    END,
    START,
    AIMessage,
    AIMessageChunk,
    BudgetExceededError,
    HumanMessage,
    InMemorySaver,
    InMemoryStore,
    Runtime,
    StateGraph,
    ToolCall,
    add_messages,
    get_stream_writer,
    task,
    tool,
)
from lingxigraph.errors import GraphValidationError, PersistenceError
from lingxigraph.events import EventKind
from lingxigraph.integrations._http import retry_delay, sleep_before_retry
from lingxigraph.integrations.coze import CozeAgentNode
from lingxigraph.integrations.openai_compat import OpenAICompatChatModel
from lingxigraph.messages import ToolMessage
from lingxigraph.observability import JsonFormatter, configure_logging, redact, start_span
from lingxigraph.patterns import build_group_chat, create_handoff_tool
from lingxigraph.prebuilt import create_agent
from lingxigraph.runtime import ExecutionBudget
from lingxigraph.schema import SchemaAdapter
from lingxigraph.server import GraphRegistry, create_app
from lingxigraph.server.models import AssistantCreate, RunCreate
from lingxigraph.server.repository import InMemoryRepository, RepositoryLimits
from lingxigraph.server.security import Authenticator
from lingxigraph.server.worker import Worker
from lingxigraph.tools import ToolNode
from lingxigraph.types import RunStatus


class ValueState(TypedDict):
    value: int


class MessageState(TypedDict):
    messages: Annotated[list[Any], add_messages]


@dataclass
class NestedRecord:
    name: str
    scores: list[int]


class ComplexState(TypedDict):
    text: str
    enabled: bool
    count: int
    ratio: float
    choice: Literal["a", "b"]
    values: list[int]
    fixed: tuple[str, int]
    repeated: tuple[int, ...]
    labels: set[str]
    lookup: dict[str, float]
    optional: int | None
    nested: NestedRecord


def value_graph(version: str, increment: int):
    builder = StateGraph(ValueState, name="versioned", version=version)
    builder.add_node("update", lambda state: {"value": state["value"] + increment})
    builder.add_edge(START, "update").add_edge("update", END)
    return builder.compile()


class ToolAndTaskHardeningTests(unittest.TestCase):
    def test_tool_validation_injection_permissions_secrets_timeout_and_handoff(self) -> None:
        seen: dict[str, Any] = {}

        @tool(permissions=("network",), secret_refs={"token": "service/token"})
        def protected(
            value: int,
            token: str,
            runtime: Runtime[Any],
            idempotency_key: str,
            tool_call: ToolCall,
        ) -> dict[str, Any]:
            seen.update(
                token=token,
                run_id=runtime.run_id,
                idempotency_key=idempotency_key,
                call_id=tool_call.id,
            )
            return {"value": value}

        node = ToolNode([protected], secret_resolver=lambda reference: f"secret:{reference}")
        builder = StateGraph(MessageState)
        builder.add_node("tools", node).add_edge(START, "tools").add_edge("tools", END)
        graph = builder.compile()
        allowed = graph.invoke(
            {
                "messages": [
                    AIMessage("", tool_calls=(ToolCall("protected", {"value": 7}, "call-7"),))
                ]
            },
            {"tool_permissions": "network"},
        )
        self.assertEqual(json.loads(allowed["messages"][-1].content), {"value": 7})
        self.assertEqual(seen["token"], "secret:service/token")
        self.assertEqual(seen["call_id"], "call-7")
        self.assertTrue(seen["run_id"])
        self.assertTrue(seen["idempotency_key"])

        denied = graph.invoke(
            {
                "messages": [
                    AIMessage("", tool_calls=(ToolCall("protected", {"value": 1}, "denied"),))
                ]
            }
        )
        self.assertEqual(denied["messages"][-1].status, "error")
        self.assertIn("requires permission", denied["messages"][-1].content)

        invalid = graph.invoke(
            {
                "messages": [
                    AIMessage("", tool_calls=(ToolCall("protected", {"value": "bad"}, "bad"),))
                ]
            },
            {"tool_permissions": ["network"]},
        )
        self.assertIn("must be integer", invalid["messages"][-1].content)

        @tool(timeout=0.01)
        async def slow() -> str:
            await asyncio.sleep(0.1)
            return "late"

        timed = asyncio.run(
            ToolNode([slow])(
                {"messages": [AIMessage("", tool_calls=(ToolCall("slow", {}, "slow"),))]}
            )
        )
        self.assertEqual(timed["messages"][-1].status, "error")
        self.assertIn("TimeoutError", timed["messages"][-1].content)

        command = asyncio.run(
            ToolNode([create_handoff_tool("researcher")])(
                {
                    "messages": [
                        AIMessage(
                            "",
                            tool_calls=(
                                ToolCall("transfer_to_researcher", {}, "real-call-id"),
                            ),
                        )
                    ]
                }
            )
        )
        message = command.update["messages"][0]
        self.assertIsInstance(message, ToolMessage)
        self.assertEqual(message.tool_call_id, "real-call-id")

    def test_tool_budget_terminates_the_run(self) -> None:
        @tool
        def echo(value: int) -> int:
            return value

        builder = StateGraph(MessageState)
        builder.add_node("tools", ToolNode([echo]))
        builder.add_edge(START, "tools").add_edge("tools", END)
        with self.assertRaises(BudgetExceededError):
            builder.compile().invoke(
                {
                    "messages": [
                        AIMessage(
                            "",
                            tool_calls=(
                                ToolCall("echo", {"value": 1}, "one"),
                                ToolCall("echo", {"value": 2}, "two"),
                            ),
                        )
                    ]
                },
                {"max_tool_calls": 1},
            )

    def test_sync_and_async_tasks_reuse_durable_results(self) -> None:
        sync_calls = 0
        async_calls = 0
        keys: list[str] = []
        store = InMemoryStore()

        @task
        def sync_work(value: int, idempotency_key: str) -> int:
            nonlocal sync_calls
            sync_calls += 1
            keys.append(idempotency_key)
            return value + 1

        @task
        async def async_work(value: int) -> int:
            nonlocal async_calls
            async_calls += 1
            return value + 2

        async def work(state: ValueState) -> dict[str, int]:
            first = sync_work(state["value"])
            return {"value": await async_work(first)}

        builder = StateGraph(ValueState, name="durable-task", version="1")
        builder.add_node("work", work).add_edge(START, "work").add_edge("work", END)
        graph = builder.compile(store=store)
        self.assertEqual(graph.invoke({"value": 1})["value"], 4)
        self.assertEqual(graph.invoke({"value": 1})["value"], 4)
        self.assertEqual((sync_calls, async_calls), (1, 1))
        self.assertEqual(len(keys[0]), 64)


class SchemaBudgetAndObservabilityTests(unittest.TestCase):
    def test_complex_schema_validation_and_nested_models(self) -> None:
        adapter = SchemaAdapter(ComplexState)
        value = {
            "text": "ok",
            "enabled": True,
            "count": 2,
            "ratio": 1.5,
            "choice": "a",
            "values": [1, 2],
            "fixed": ("x", 3),
            "repeated": (1, 2),
            "labels": {"one"},
            "lookup": {"x": 1.0},
            "optional": None,
            "nested": {"name": "n", "scores": [4]},
        }
        self.assertEqual(adapter.validate(value)["nested"]["name"], "n")
        schema = adapter.json_schema()
        self.assertEqual(schema["properties"]["count"]["type"], "integer")
        self.assertEqual(schema["properties"]["nested"]["type"], "object")
        self.assertEqual(adapter.fingerprint(), adapter.fingerprint())

        invalid_values = [
            {**value, "count": True},
            {**value, "choice": "c"},
            {**value, "values": "not-an-array"},
            {**value, "values": ["bad"]},
            {**value, "fixed": ("short",)},
            {**value, "fixed": ["x", 1]},
            {**value, "repeated": (1, "bad")},
            {**value, "lookup": []},
            {**value, "lookup": {"x": "bad"}},
            {**value, "optional": []},
            {**value, "nested": {"name": "n", "scores": ["bad"]}},
        ]
        for invalid in invalid_values:
            with self.subTest(invalid=invalid), self.assertRaises(GraphValidationError):
                adapter.validate(invalid)
        with self.assertRaises(GraphValidationError):
            adapter.validate({key: item for key, item in value.items() if key != "text"})
        with self.assertRaises(GraphValidationError):
            adapter.validate({**value, "unknown": 1})
        with self.assertRaises(GraphValidationError):
            adapter.validate([])

        class Child(BaseModel):
            count: int

        class Parent(BaseModel):
            child: Child

        pydantic_adapter = SchemaAdapter(Parent)
        self.assertEqual(pydantic_adapter.validate({"child": {"count": 2}}), {"child": {"count": 2}})
        self.assertEqual(pydantic_adapter.validate_partial({"child": {"count": 3}})["child"]["count"], 3)
        with self.assertRaises(GraphValidationError):
            pydantic_adapter.validate({"child": {"count": "bad"}})
        with self.assertRaises(GraphValidationError):
            pydantic_adapter.validate_partial({"unknown": 1})
        with self.assertRaises(GraphValidationError):
            pydantic_adapter.validate_partial({"child": {"count": "bad"}})

    def test_budget_definition_http_backoff_and_structured_logging(self) -> None:
        budget = ExecutionBudget(max_model_calls=1, max_tool_calls=1, max_tokens=5, max_cost=1.0)
        budget.consume_model_call()
        budget.consume_model_usage({"total_token_count": 2, "total_cost": 0.25})
        budget.consume_tool_call("first")
        self.assertEqual(budget.snapshot()["tokens"], 2)
        with self.assertRaises(BudgetExceededError):
            budget.consume_model_call()
        with self.assertRaises(BudgetExceededError):
            budget.consume_tool_call("second")
        with self.assertRaises(BudgetExceededError):
            budget.consume_model_usage({"total_tokens": 6})
        with self.assertRaises(BudgetExceededError):
            ExecutionBudget(max_cost=0.1).consume_model_usage({"cost": 0.2})
        for invalid_usage in ({"total_tokens": -1}, {"cost": float("inf")}):
            with self.assertRaises(ValueError):
                ExecutionBudget().consume_model_usage(invalid_usage)

        self.assertEqual(retry_delay(1, {"retry-after": "0"}), 0)
        future = (datetime.now(UTC) + timedelta(seconds=1)).strftime("%a, %d %b %Y %H:%M:%S GMT")
        self.assertGreaterEqual(retry_delay(1, {"retry-after": future}), 0)
        self.assertGreaterEqual(retry_delay(2, {"retry-after": "invalid"}, base=0), 0)
        asyncio.run(sleep_before_retry(1, base=0))

        redacted = redact(
            {
                "authorization": "bearer",
                "nested": [{"api_token": "token"}, ("safe",)],
                "value": 1,
            }
        )
        self.assertEqual(redacted["authorization"], "[REDACTED]")
        self.assertEqual(redacted["nested"][0]["api_token"], "[REDACTED]")
        record = __import__("logging").LogRecord(
            "test", 20, __file__, 1, "hello %s", ("world",), None
        )
        record.run_id = "run"
        formatted = json.loads(JsonFormatter().format(record))
        self.assertEqual(formatted["message"], "hello world")
        self.assertEqual(formatted["run_id"], "run")
        root = __import__("logging").getLogger()
        handlers, level = list(root.handlers), root.level
        try:
            configure_logging(level="warning", json_output=False)
            self.assertEqual(root.level, 30)
            configure_logging(level="info", json_output=True)
            self.assertIsInstance(root.handlers[0].formatter, JsonFormatter)
        finally:
            root.handlers[:] = handlers
            root.setLevel(level)
        with start_span("test", {"safe": True}):
            pass

    def test_tool_definition_rejects_unsafe_metadata(self) -> None:
        def sample(value: str) -> str:
            return value

        with self.assertRaises(TypeError):
            tool(permissions="network")(sample)
        with self.assertRaises(ValueError):
            tool(timeout=0)(sample)
        with self.assertRaises(ValueError):
            tool(permissions=("",))(sample)
        with self.assertRaises(ValueError):
            tool(secret_refs={"missing": "ref"})(sample)
        with self.assertRaises(ValueError):
            tool(secret_refs={"value": ""})(sample)


class BudgetAndMultiAgentTests(unittest.TestCase):
    def test_model_call_budget_is_checked_before_the_second_provider_call(self) -> None:
        class Model:
            calls = 0

            async def agenerate(self, messages, *, tools=None, **kwargs):
                del messages, tools, kwargs
                self.calls += 1
                return AIMessage('{"value": 1}', usage={"total_tokens": 2})

        model = Model()
        graph = create_agent(
            model,
            response_format={
                "type": "object",
                "properties": {"value": {"type": "integer"}},
                "required": ["value"],
                "additionalProperties": False,
            },
        )
        with self.assertRaises(BudgetExceededError):
            graph.invoke({"messages": [HumanMessage("answer")]}, {"max_model_calls": 1})
        self.assertEqual(model.calls, 1)

    def test_structured_output_repairs_and_token_budget(self) -> None:
        class RepairModel:
            def __init__(self):
                self.responses = [
                    AIMessage("draft", usage={"total_tokens": 1}),
                    AIMessage("not-json", usage={"total_tokens": 1}),
                    AIMessage('{"value": 3}', usage={"total_tokens": 1}),
                ]

            async def agenerate(self, messages, *, tools=None, **kwargs):
                del messages, tools, kwargs
                return self.responses.pop(0)

        repaired = create_agent(
            RepairModel(),
            response_format={
                "type": "object",
                "properties": {"value": {"type": "integer"}},
                "required": ["value"],
                "additionalProperties": False,
            },
        ).invoke({"messages": [HumanMessage("answer")]})
        self.assertEqual(repaired["structured_response"], {"value": 3})

        class ExpensiveModel:
            async def agenerate(self, messages, *, tools=None, **kwargs):
                del messages, tools, kwargs
                return AIMessage("done", usage={"total_tokens": 11})

        with self.assertRaises(BudgetExceededError):
            create_agent(ExpensiveModel()).invoke(
                {"messages": [HumanMessage("answer")]}, {"max_tokens": 10}
            )

    def test_group_chat_has_a_deterministic_turn_cap(self) -> None:
        class GroupState(TypedDict, total=False):
            active_agent: str
            turn: int
            trace: list[str]

        def speaker(name: str):
            def speak(state):
                return {"trace": [*state.get("trace", []), name]}

            return speak

        graph = build_group_chat(
            GroupState,
            {"a": speaker("a"), "b": speaker("b")},
            max_turns=2,
        ).compile()
        result = graph.invoke({"active_agent": "a", "turn": 0, "trace": []})
        self.assertEqual(result["trace"], ["a", "b"])
        self.assertEqual(result["turn"], 2)


class RegistryServerAndWorkerTests(unittest.TestCase):
    def test_registry_and_worker_pin_exact_graph_version(self) -> None:
        async def run() -> None:
            registry = GraphRegistry({"calculator": value_graph("1", 1)})
            registry.register("calculator", value_graph("2", 10))
            self.assertEqual(registry.get("calculator").graph_version, "2")
            self.assertEqual(registry.get("calculator", "1").graph_version, "1")
            self.assertEqual(len(registry.list()), 2)

            repository = InMemoryRepository()
            assistant = await repository.create_assistant(
                "tenant",
                AssistantCreate(graph_id="calculator", graph_version="1"),
                "1",
            )
            queued = await repository.create_run(
                "tenant",
                None,
                assistant,
                RunCreate(assistant_id=assistant.id, input={"value": 1}),
            )
            await Worker(registry, repository).run_once()
            completed = await repository.get_run("tenant", queued.id)
            self.assertEqual(completed.status, RunStatus.SUCCEEDED)
            self.assertEqual(completed.output, {"value": 2})

        asyncio.run(run())

    def test_manifest_accepts_multiple_versions_and_rejects_mismatch(self) -> None:
        module_name = "_lingxigraph_test_versioned_manifest"
        module = types.ModuleType(module_name)
        module.v1 = value_graph("1", 1)
        module.v2 = value_graph("2", 2)
        sys.modules[module_name] = module
        try:
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "lingxigraph.json"
                path.write_text(
                    json.dumps(
                        {
                            "graphs": {
                                "calculator": [
                                    {"path": f"{module_name}:v1", "version": "1"},
                                    {"path": f"{module_name}:v2", "version": "2"},
                                ]
                            }
                        }
                    ),
                    encoding="utf-8",
                )
                registry = GraphRegistry.from_manifest(path)
                self.assertEqual(registry.get("calculator").graph_version, "2")
                path.write_text(
                    json.dumps(
                        {
                            "graphs": {
                                "calculator": {
                                    "path": f"{module_name}:v1",
                                    "version": "wrong",
                                }
                            }
                        }
                    ),
                    encoding="utf-8",
                )
                with self.assertRaisesRegex(ValueError, "does not match"):
                    GraphRegistry.from_manifest(path)
        finally:
            sys.modules.pop(module_name, None)

    def test_api_idempotency_request_limit_and_readiness(self) -> None:
        registry = GraphRegistry({"calculator": value_graph("1", 1)})
        repository = InMemoryRepository(limits=RepositoryLimits(max_request_bytes=512))
        app = create_app(
            registry=registry,
            repository=repository,
            authenticator=Authenticator.insecure_dev(),
        )
        headers = {"x-tenant-id": "tenant"}
        with TestClient(app) as client:
            assistant = client.post(
                "/v1/assistants", headers=headers, json={"graph_id": "calculator"}
            ).json()
            body = {"assistant_id": assistant["id"], "input": {"value": 1}}
            first = client.post(
                "/v1/runs", headers={**headers, "Idempotency-Key": "same"}, json=body
            )
            repeated = client.post(
                "/v1/runs", headers={**headers, "Idempotency-Key": "same"}, json=body
            )
            self.assertEqual(first.json()["id"], repeated.json()["id"])
            conflict = client.post(
                "/v1/runs",
                headers={**headers, "Idempotency-Key": "same"},
                json={**body, "input": {"value": 2}},
            )
            self.assertEqual(conflict.status_code, 409)
            self.assertEqual(conflict.json()["code"], "idempotency_conflict")
            oversized = client.post(
                "/v1/runs",
                headers={**headers, "content-length": "513"},
                content=b"{}",
            )
            self.assertEqual(oversized.status_code, 413)
            understated = client.post(
                "/v1/runs",
                headers={**headers, "content-length": "2"},
                content=b"x" * 513,
            )
            self.assertEqual(understated.status_code, 413)

        class UnhealthyRepository(InMemoryRepository):
            async def healthcheck(self) -> bool:
                return False

        unhealthy = create_app(
            registry=registry,
            repository=UnhealthyRepository(),
            authenticator=Authenticator.insecure_dev(),
        )
        with TestClient(unhealthy) as client:
            self.assertEqual(client.get("/ready").status_code, 503)

    def test_worker_retry_dead_letter_and_redrive(self) -> None:
        def fail(_state):
            raise RuntimeError("temporary")

        builder = StateGraph(ValueState, version="1")
        builder.add_node("fail", fail).add_edge(START, "fail").add_edge("fail", END)

        async def run() -> None:
            registry = GraphRegistry({"failure": builder.compile()})
            repository = InMemoryRepository()
            assistant = await repository.create_assistant(
                "tenant", AssistantCreate(graph_id="failure"), "1"
            )
            queued = await repository.create_run(
                "tenant",
                None,
                assistant,
                RunCreate(assistant_id=assistant.id, input={"value": 1}),
            )
            worker = Worker(registry, repository, max_delivery_attempts=2)
            await worker.run_once()
            retrying = await repository.get_run("tenant", queued.id)
            self.assertEqual(retrying.status, RunStatus.PENDING)
            self.assertEqual(retrying.attempt, 1)
            await worker.run_once()
            dead = await repository.get_run("tenant", queued.id)
            self.assertEqual(dead.status, RunStatus.DEAD_LETTER)
            redriven = await repository.redrive_run("tenant", queued.id)
            self.assertEqual(redriven.status, RunStatus.PENDING)
            self.assertEqual(redriven.attempt, 0)

        asyncio.run(run())

    def test_checkpoint_state_size_limit(self) -> None:
        builder = StateGraph(ValueState)
        builder.add_node("noop", lambda state: {})
        builder.add_edge(START, "noop").add_edge("noop", END)
        graph = builder.compile(checkpointer=InMemorySaver())
        with self.assertRaises(PersistenceError):
            graph.invoke(
                {"value": 1},
                {"configurable": {"thread_id": "limited"}, "max_state_bytes": 1},
            )


class ProviderReliabilityTests(unittest.TestCase):
    def test_standard_stream_writer_and_coze_tokens_are_delivered_while_node_runs(self) -> None:
        async def standard_writer() -> None:
            release = asyncio.Event()

            async def work(state: ValueState, runtime: Runtime[Any]) -> dict[str, int]:
                get_stream_writer()({"token": "A"})
                runtime.stream_writer({"token": "B"})
                runtime.emit("progress", {"percent": 50})
                runtime.emit_message(AIMessageChunk("not-a-custom-chunk"))
                await release.wait()
                return {"value": state["value"] + 1}

            builder = StateGraph(ValueState)
            builder.add_node("work", work).add_edge(START, "work").add_edge("work", END)
            iterator = builder.compile().astream({"value": 0}, stream_mode="custom")
            first = await asyncio.wait_for(anext(iterator), timeout=0.1)
            second = await asyncio.wait_for(anext(iterator), timeout=0.1)
            third = await asyncio.wait_for(anext(iterator), timeout=0.1)
            self.assertEqual(first, {"token": "A"})
            self.assertEqual(second, {"token": "B"})
            self.assertEqual(third, {"progress": {"percent": 50}})
            release.set()
            self.assertEqual([item async for item in iterator], [])

        async def coze_tokens() -> None:
            release = asyncio.Event()

            class FakeCozeClient:
                async def chat_stream(self, *args, **kwargs):
                    del args, kwargs
                    yield {
                        "event": "conversation.chat.created",
                        "data": {"id": "chat", "conversation_id": "conversation"},
                    }
                    yield {
                        "event": "conversation.message.delta",
                        "data": {"id": "chat", "content": "你"},
                    }
                    await release.wait()
                    yield {
                        "event": "conversation.message.delta",
                        "data": {"id": "chat", "content": "好"},
                    }

            class CozeState(TypedDict):
                messages: Annotated[list[Any], add_messages]
                coze_conversations: dict[str, str]

            builder = StateGraph(CozeState)
            builder.add_node(
                "coze",
                CozeAgentNode("bot", client=FakeCozeClient(), user_id="user"),
            )
            builder.add_edge(START, "coze").add_edge("coze", END)
            iterator = builder.compile().astream(
                {"messages": [HumanMessage("问候")], "coze_conversations": {}},
                stream_mode="events",
            )

            async def next_token():
                async for event in iterator:
                    if event.kind is EventKind.MESSAGE:
                        return event.data["value"][0]
                raise AssertionError("message event was not emitted")

            first = await asyncio.wait_for(next_token(), timeout=0.1)
            self.assertIsInstance(first, AIMessageChunk)
            self.assertEqual(first.content, "你")
            release.set()
            second = await asyncio.wait_for(next_token(), timeout=0.1)
            self.assertEqual(second.content, "好")
            await iterator.aclose()

        asyncio.run(standard_writer())
        asyncio.run(coze_tokens())

    def test_openai_retries_with_stable_idempotency_and_preserves_stream_usage(self) -> None:
        attempts = 0
        keys: list[str] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            keys.append(request.headers["idempotency-key"])
            payload = json.loads(request.content)
            if payload.get("stream"):
                body = (
                    'data: {"id":"s","model":"m","choices":[{"delta":{"content":"ok"},'
                    '"finish_reason":"stop"}]}\n\n'
                    'data: {"id":"s","model":"m","choices":[],"usage":{"total_tokens":4}}\n\n'
                    'data: [DONE]\n\n'
                )
                return httpx.Response(
                    200, text=body, headers={"content-type": "text/event-stream"}
                )
            attempts += 1
            if attempts == 1:
                return httpx.Response(429, headers={"Retry-After": "0"})
            return httpx.Response(
                200,
                json={
                    "id": "c",
                    "model": "m",
                    "choices": [
                        {"finish_reason": "stop", "message": {"content": "ok"}}
                    ],
                    "usage": {"total_tokens": 3},
                },
            )

        async def run() -> None:
            model = OpenAICompatChatModel(
                "m",
                base_url="https://example.test/v1",
                api_key="key",
                transport=httpx.MockTransport(handler),
                retry_base=0,
            )
            response = await model.agenerate([HumanMessage("hello")])
            self.assertEqual(response.content, "ok")
            chunks = [chunk async for chunk in model.astream([HumanMessage("hello")])]
            self.assertEqual(chunks[0].response_metadata["finish_reason"], "stop")
            self.assertEqual(chunks[-1].usage["total_tokens"], 4)
            await model.aclose()

        asyncio.run(run())
        self.assertEqual(attempts, 2)
        self.assertEqual(keys[0], keys[1])
        self.assertTrue(keys[0])


if __name__ == "__main__":
    unittest.main()
