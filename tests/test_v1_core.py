import asyncio
import operator
import time
import unittest
from dataclasses import dataclass
from datetime import date, datetime, timezone
from datetime import time as datetime_time
from enum import Enum
from pathlib import Path
from typing import Annotated, TypedDict
from uuid import uuid4

from lingxigraph import (
    END,
    START,
    CachePolicy,
    GraphTimeoutError,
    InMemoryCache,
    InMemorySaver,
    JsonSerializer,
    Runtime,
    SerializationError,
    StateGraph,
)


class DurableState(TypedDict):
    values: Annotated[list[str], operator.add]


class V1CoreTests(unittest.TestCase):
    def test_pending_writes_prevent_successful_sibling_reexecution(self) -> None:
        calls = {"successful": 0, "flaky": 0}

        def successful(_state):
            calls["successful"] += 1
            return {"values": ["successful"]}

        def flaky(_state):
            calls["flaky"] += 1
            if calls["flaky"] == 1:
                raise RuntimeError("injected failure")
            return {"values": ["flaky"]}

        builder = StateGraph(DurableState)
        builder.add_node("successful", successful)
        builder.add_node("flaky", flaky)
        builder.add_edge(START, "successful")
        builder.add_edge(START, "flaky")
        builder.add_edge(["successful", "flaky"], END)
        graph = builder.compile(checkpointer=InMemorySaver())
        config = {"configurable": {"thread_id": "pending-writes"}}

        with self.assertRaisesRegex(RuntimeError, "injected failure"):
            graph.invoke({"values": []}, config)
        result = graph.invoke({"values": []}, config)

        self.assertEqual(result["values"], ["successful", "flaky"])
        self.assertEqual(calls, {"successful": 1, "flaky": 2})

    def test_runtime_context_idempotency_and_custom_stream(self) -> None:
        class State(TypedDict):
            identity: str

        class Context(TypedDict):
            tenant: str

        def node(_state, runtime: Runtime[dict[str, str]]):
            runtime.emit("progress", {"tenant": runtime.context["tenant"]})
            return {"identity": runtime.idempotency_key}

        builder = StateGraph(State, context_schema=Context)
        builder.add_node("node", node)
        builder.add_edge(START, "node")
        builder.add_edge("node", END)
        graph = builder.compile()

        events = list(
            graph.stream(
                {"identity": ""},
                context={"tenant": "acme"},
                stream_mode="custom",
            )
        )
        first = graph.invoke({"identity": ""}, context={"tenant": "acme"})
        second = graph.invoke({"identity": ""}, context={"tenant": "acme"})

        self.assertEqual(events, [{"progress": {"tenant": "acme"}}])
        self.assertEqual(first["identity"], second["identity"])

    def test_node_cache_and_timeout(self) -> None:
        class State(TypedDict):
            value: int

        calls = 0

        def cached(state):
            nonlocal calls
            calls += 1
            return {"value": state["value"] + 1}

        builder = StateGraph(State)
        builder.add_node("cached", cached, cache_policy=CachePolicy(ttl=60))
        builder.add_edge(START, "cached")
        builder.add_edge("cached", END)
        graph = builder.compile(cache=InMemoryCache())

        self.assertEqual(graph.invoke({"value": 1})["value"], 2)
        self.assertEqual(graph.invoke({"value": 1})["value"], 2)
        self.assertEqual(calls, 1)

        async def slow(_state):
            await asyncio.sleep(0.05)
            return {"value": 2}

        timed = StateGraph(State)
        timed.add_node("slow", slow, timeout=0.001)
        timed.add_edge(START, "slow")
        timed.add_edge("slow", END)
        with self.assertRaises(GraphTimeoutError):
            timed.compile().invoke({"value": 1})

    def test_any_fan_in_activates_from_one_source(self) -> None:
        class State(TypedDict):
            route: str
            joined: bool

        builder = StateGraph(State)
        builder.add_node("a", lambda _state: {})
        builder.add_node("b", lambda _state: {})
        builder.add_node("join", lambda _state: {"joined": True})
        builder.add_conditional_edges(START, lambda state: state["route"], {"a": "a", "b": "b"})
        builder.add_edge(["a", "b"], "join", trigger="any")
        builder.add_edge("join", END)

        self.assertTrue(builder.compile().invoke({"route": "a", "joined": False})["joined"])

    def test_json_serializer_rejects_unsafe_objects(self) -> None:
        with self.assertRaises(SerializationError):
            JsonSerializer().dumps({"unsafe": object()})

    def test_json_serializer_roundtrips_safe_extensions_and_rejects_invalid_payloads(
        self,
    ) -> None:
        class Choice(Enum):
            YES = "yes"

        @dataclass
        class Value:
            name: str

        identifier = uuid4()
        moment = datetime.now(timezone.utc).replace(microsecond=0)
        serializer = JsonSerializer()
        result = serializer.loads(
            serializer.dumps(
                {
                    "bytes": b"value",
                    "tuple": (1, "two"),
                    "set": {"b", "a"},
                    "datetime": moment,
                    "date": date(2026, 7, 17),
                    "time": datetime_time(12, 30),
                    "uuid": identifier,
                    "path": Path("safe/path"),
                    "enum": Choice.YES,
                    "dataclass": Value("safe"),
                }
            )
        )
        self.assertEqual(result["bytes"], b"value")
        self.assertEqual(result["tuple"], (1, "two"))
        self.assertEqual(result["set"], {"a", "b"})
        self.assertEqual(result["datetime"], moment)
        self.assertEqual(result["uuid"], identifier)
        self.assertEqual(result["enum"], "yes")
        self.assertEqual(result["dataclass"], {"name": "safe"})
        with self.assertRaises(SerializationError):
            serializer.dumps({1: "not-a-string-key"})
        with self.assertRaises(SerializationError):
            serializer.loads(b"not-json")
        with self.assertRaises(SerializationError):
            serializer.loads(b'{"version":999,"value":null}')

    def test_cache_ttl_namespace_clear_async_and_failure_degradation(self) -> None:
        cache = InMemoryCache()
        cache.set("one:a", {"value": 1}, ttl=0.001)
        cache.set("two:b", {"value": 2})
        time.sleep(0.002)
        self.assertIsNone(cache.get("one:a"))
        cache.clear(namespace="two")
        self.assertIsNone(cache.get("two:b"))

        async def async_cache() -> None:
            await cache.aset("async", {"value": 3})
            self.assertEqual(await cache.aget("async"), {"value": 3})
            await cache.adelete("async")
            self.assertIsNone(await cache.aget("async"))

        asyncio.run(async_cache())

        class FailedCache:
            async def aget(self, _key):
                raise ConnectionError("redis unavailable")

            async def aset(self, _key, _value, *, ttl=None):
                raise ConnectionError("redis unavailable")

        class State(TypedDict):
            value: int

        builder = StateGraph(State)
        builder.add_node(
            "node", lambda state: {"value": state["value"] + 1}, cache_policy=CachePolicy()
        )
        builder.add_edge(START, "node")
        builder.add_edge("node", END)
        self.assertEqual(builder.compile(cache=FailedCache()).invoke({"value": 1})["value"], 2)


if __name__ == "__main__":
    unittest.main()
