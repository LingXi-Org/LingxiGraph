import asyncio
import json
import unittest

import httpx

from lingxigraph import (
    AIMessage,
    ExecutionBudget,
    HumanMessage,
    ImmutablePrefix,
    InMemorySaver,
    InMemoryUsageLedger,
    PrefixDriftError,
    SystemMessage,
    ToolCall,
    ToolMessage,
    ToolNode,
    ToolSpec,
    apply_history_hygiene,
    build_tool_catalog_fingerprint,
    compare_tool_catalogs,
    create_agent,
    normalize_usage,
    repair_model_history,
    tool,
)
from lingxigraph.cache_first import CacheFirstChatModel, CacheFirstConfig, ContextCompactor


class CacheFirstCoreTests(unittest.TestCase):
    def test_tool_and_schema_canonicalization_is_registration_order_independent(self) -> None:
        @tool(name="z_lookup")
        def lookup_z(query: str) -> str:
            return query

        @tool(name="a_lookup")
        def lookup_a(query: str) -> str:
            return query

        first = ImmutablePrefix.create(
            system_prompt="stable",
            tools=[lookup_z, lookup_a],
        )
        second = ImmutablePrefix.create(
            system_prompt="stable",
            tools=[lookup_a, lookup_z],
        )
        self.assertEqual(first.fingerprint, second.fingerprint)
        self.assertEqual(first.tool_catalog.tool_names, ("a_lookup", "z_lookup"))
        self.assertEqual(first.tools[0]["function"]["name"], "a_lookup")

    def test_prefix_revision_and_drift_are_explicit(self) -> None:
        original = ImmutablePrefix.create(
            system_prompt="stable", pinned_constraints=["never leak"]
        )
        changed = original.evolve(system_prompt="changed")
        self.assertEqual(changed.revision, original.revision + 1)
        diagnostic = original.drift_against(changed)
        self.assertEqual(diagnostic.first_changed_component, "system_prompt")
        self.assertIn("system_prompt", diagnostic.changed_sections)

        class Model:
            async def agenerate(self, messages, *, tools=None, **kwargs):
                del messages, tools, kwargs
                return AIMessage("ok", usage={"prompt_tokens": 3, "completion_tokens": 1})

        async def run() -> None:
            wrapped = CacheFirstChatModel(Model(), prefix=original)
            with self.assertRaises(PrefixDriftError):
                await wrapped.agenerate([HumanMessage("request")], tools=[{"name": "new"}])

        asyncio.run(run())

    def test_rendered_prefix_is_not_duplicated_when_passed_back_to_wrapper(self) -> None:
        seen: list[tuple[str, ...]] = []

        class Model:
            async def agenerate(self, messages, *, tools=None, **kwargs):
                del tools, kwargs
                seen.append(tuple(item.type for item in messages))
                return AIMessage("ok")

        async def run() -> None:
            prefix = ImmutablePrefix.create(system_prompt="stable")
            wrapped = CacheFirstChatModel(Model(), prefix=prefix)
            await wrapped.agenerate([*prefix.render_messages(), HumanMessage("request")])

        asyncio.run(run())
        self.assertEqual(seen, [("system", "human")])

    def test_tool_catalog_drift_categories(self) -> None:
        base = build_tool_catalog_fingerprint(
            [{"type": "function", "function": {"name": "one", "parameters": {}}}]
        )
        additive = build_tool_catalog_fingerprint(
            [
                {"type": "function", "function": {"name": "one", "parameters": {}}},
                {"type": "function", "function": {"name": "two", "parameters": {}}},
            ]
        )
        changed = build_tool_catalog_fingerprint(
            [
                {
                    "type": "function",
                    "function": {
                        "name": "one",
                        "parameters": {"type": "object", "properties": {"x": {}}},
                    },
                }
            ]
        )
        self.assertEqual(compare_tool_catalogs(base, additive).kind, "additive")
        self.assertEqual(compare_tool_catalogs(base, changed).kind, "schema_changed")

    def test_usage_normalizer_prioritizes_deepseek_and_does_not_fabricate_partial_rate(
        self,
    ) -> None:
        deepseek = normalize_usage(
            {
                "prompt_tokens": 100,
                "completion_tokens": 10,
                "total_tokens": 110,
                "prompt_cache_hit_tokens": 80,
                "prompt_cache_miss_tokens": 20,
                "cached_tokens": 1,
            }
        )
        self.assertEqual(deepseek.cache_hit_tokens, 80)
        self.assertEqual(deepseek.cache_miss_tokens, 20)
        self.assertEqual(deepseek.cache_hit_rate, 0.8)
        self.assertEqual(deepseek.token_savings, 80)

        openai = normalize_usage(
            {"prompt_tokens": 100, "prompt_tokens_details": {"cached_tokens": 70}}
        )
        self.assertEqual(openai.cache_hit_tokens, 70)
        self.assertEqual(openai.cache_miss_tokens, 30)
        self.assertEqual(openai.cache_hit_rate, 0.7)

        partial = normalize_usage({"prompt_tokens": 100, "prompt_cache_hit_tokens": 70})
        self.assertEqual(partial.cache_hit_tokens, 70)
        self.assertIsNone(partial.cache_hit_rate)
        self.assertIn("incomplete_deepseek_cache_pair", partial.diagnostics)

        anthropic_fixture = normalize_usage(
            {
                "input_tokens": 10,
                "cache_read_input_tokens": 80,
                "cache_creation_input_tokens": 10,
            }
        )
        self.assertEqual(anthropic_fixture.prompt_tokens, 100)
        self.assertEqual(anthropic_fixture.cache_hit_tokens, 80)
        self.assertEqual(anthropic_fixture.cache_miss_tokens, 10)
        self.assertEqual(anthropic_fixture.cache_write_tokens, 10)

    def test_history_repair_is_projection_only_and_reorders_results(self) -> None:
        calls = AIMessage(
            "",
            tool_calls=(ToolCall("first", {}, "c1"), ToolCall("second", {}, "c2")),
        )
        source = (
            HumanMessage("goal"),
            ToolMessage("orphan", tool_call_id="missing"),
            calls,
            ToolMessage("second result", tool_call_id="c2"),
            ToolMessage("duplicate", tool_call_id="c2"),
            ToolMessage("first result", tool_call_id="c1"),
        )
        repaired = repair_model_history(source)
        self.assertEqual(source[1].content, "orphan")
        self.assertEqual(
            [item.tool_call_id for item in repaired.messages if isinstance(item, ToolMessage)],
            ["c1", "c2"],
        )
        self.assertEqual(repaired.orphan_tool_results, 1)
        self.assertEqual(repaired.duplicate_tool_results, 1)
        self.assertEqual(repaired.reordered_tool_results, 1)

        missing = repair_model_history(
            [
                AIMessage(
                    "", tool_calls=(ToolCall("first", {}, "c1"), ToolCall("second", {}, "c2"))
                ),
                ToolMessage("one", "c1"),
            ]
        )
        self.assertEqual(missing.messages, ())
        self.assertEqual(missing.missing_tool_results, 1)

    def test_history_hygiene_bounds_encoded_content_and_tool_args(self) -> None:
        long_arg = "x" * 20_000
        encoded = "A" * 400
        messages = (
            AIMessage("", tool_calls=(ToolCall("run", {"arg": long_arg}, "call"),)),
            ToolMessage(f"\x1b[31m{encoded}\x1b[0m", tool_call_id="call"),
        )
        result = apply_history_hygiene(
            messages,
            CacheFirstConfig(max_tool_result_bytes=200, max_tool_result_lines=4),
        )
        self.assertLessEqual(len(str(result[-1].content).encode()), 200)
        self.assertLess(len(result[0].tool_calls[0].args["arg"]), len(long_arg))
        self.assertNotIn("\x1b", str(result[-1].content))

    def test_compaction_accounts_for_output_budget_without_changing_prefix(self) -> None:
        prefix = ImmutablePrefix.create(
            system_prompt="stable system",
            pinned_constraints=["keep this"],
            few_shots=(SystemMessage("few-shot"),),
        )
        before = prefix.fingerprint
        compactor = ContextCompactor(
            CacheFirstConfig(
                context_window_tokens=140,
                hard_threshold_tokens=140,
                max_output_tokens=50,
                keep_recent_messages=2,
            )
        )
        result = compactor.compact(
            prefix.render_messages(),
            tuple(HumanMessage("history " + ("x" * 40)) for _ in range(8)),
        )
        self.assertTrue(result.compacted)
        self.assertEqual(prefix.fingerprint, before)
        self.assertGreater(result.estimated_total_tokens, result.estimated_input_tokens - 1)
        self.assertLessEqual(result.estimated_total_tokens, 140)


class CacheFirstRuntimeTests(unittest.TestCase):
    def test_ledger_and_budget_restore_cumulative_counters(self) -> None:
        ledger = InMemoryUsageLedger()
        ledger.record(
            "thread|default|deepseek|deepseek-chat",
            {
                "prompt_tokens": 100,
                "completion_tokens": 5,
                "prompt_cache_hit_tokens": 90,
                "prompt_cache_miss_tokens": 10,
            },
        )
        snapshot = ledger.snapshot("thread|default|deepseek|deepseek-chat")
        restored = InMemoryUsageLedger()
        restored.restore("thread|default|deepseek|deepseek-chat", snapshot)
        self.assertEqual(
            restored.snapshot("thread|default|deepseek|deepseek-chat")["cache_hit_tokens"], 90
        )

        budget = ExecutionBudget()
        budget.consume_model_usage(
            {
                "prompt_tokens": 100,
                "completion_tokens": 5,
                "prompt_cache_hit_tokens": 90,
                "prompt_cache_miss_tokens": 10,
            }
        )
        budget2 = ExecutionBudget()
        budget2.restore_cache_snapshot(budget.cache_snapshot())
        self.assertEqual(budget2.cache_hit_tokens, 90)
        self.assertEqual(budget2.cache_miss_tokens, 10)

    def test_checkpoint_persists_only_cache_counters_and_hydrates_on_restart(self) -> None:
        class Model:
            async def agenerate(self, messages, *, tools=None, **kwargs):
                del messages, tools, kwargs
                return AIMessage(
                    "ok",
                    usage={
                        "prompt_tokens": 10,
                        "completion_tokens": 2,
                        "total_tokens": 12,
                        "prompt_cache_hit_tokens": 8,
                        "prompt_cache_miss_tokens": 2,
                    },
                )

        saver = InMemorySaver()
        config = {"configurable": {"thread_id": "cache-checkpoint"}}
        create_agent(Model(), checkpointer=saver).invoke(
            {"messages": [HumanMessage("first")]}, config
        )
        first = saver.get_tuple(config)
        assert first is not None
        first_telemetry = first.metadata["lingxigraph.cache_first"]
        self.assertEqual(first_telemetry["cache_hit_tokens"], 8)
        self.assertNotIn("messages", first.metadata.get("lingxigraph.cache_first", {}))

        create_agent(Model(), checkpointer=saver).invoke(
            {"messages": [HumanMessage("second")]}, config
        )
        second = saver.get_tuple(config)
        assert second is not None
        self.assertEqual(second.metadata["lingxigraph.cache_first"]["cache_hit_tokens"], 16)


class ReadOnlyToolTests(unittest.TestCase):
    def test_read_only_tools_run_bounded_and_return_call_order(self) -> None:
        order: list[str] = []
        active = 0
        maximum = 0
        lock = asyncio.Lock()

        async def read(value: str) -> str:
            nonlocal active, maximum
            async with lock:
                active += 1
                maximum = max(maximum, active)
            await asyncio.sleep(0.01 if value == "one" else 0.001)
            async with lock:
                active -= 1
            order.append(value)
            return value

        first = ToolSpec(
            "first",
            "",
            {
                "type": "object",
                "properties": {"value": {"type": "string"}},
                "required": ["value"],
            },
            read,
            read_only=True,
        )
        second = ToolSpec("second", "", first.parameters, read, read_only=True)
        node = ToolNode([first, second], read_only_concurrency=2, read_only_batch_size=2)

        async def run() -> dict:
            result = await node(
                {
                    "messages": [
                        AIMessage(
                            "",
                            tool_calls=(
                                ToolCall("first", {"value": "one"}, "1"),
                                ToolCall("second", {"value": "two"}, "2"),
                            ),
                        )
                    ]
                }
            )
            return result

        result = asyncio.run(run())
        self.assertLessEqual(maximum, 2)
        self.assertEqual([item.tool_call_id for item in result["messages"]], ["1", "2"])


class CacheFirstWrapperTests(unittest.TestCase):
    def test_stable_prefix_is_reused_across_warm_turns_and_usage_is_attached(self) -> None:
        seen: list[tuple[tuple[str, ...], str | None]] = []

        class Model:
            model = "deepseek-chat"
            provider_id = "deepseek"
            endpoint_format = "chat-completions"

            async def agenerate(self, messages, *, tools=None, **kwargs):
                del kwargs
                seen.append(
                    (
                        tuple(item.type for item in messages),
                        getattr(messages[0], "content", None),
                    )
                )
                return AIMessage(
                    "ok",
                    usage={
                        "prompt_tokens": 100,
                        "completion_tokens": 3,
                        "total_tokens": 103,
                        "prompt_cache_hit_tokens": 90 if len(seen) > 1 else 0,
                        "prompt_cache_miss_tokens": 10 if len(seen) > 1 else 100,
                    },
                )

        async def run() -> tuple[AIMessage, AIMessage, InMemoryUsageLedger]:
            ledger = InMemoryUsageLedger()
            prefix = ImmutablePrefix.create(system_prompt="stable", pinned_constraints=["rule"])
            wrapped = CacheFirstChatModel(
                Model(), prefix=prefix, usage_ledger=ledger, provider_id="deepseek"
            )
            first = await wrapped.agenerate([HumanMessage("one")])
            second = await wrapped.agenerate([HumanMessage("one"), first, HumanMessage("two")])
            return first, second, ledger

        first, second, ledger = asyncio.run(run())
        self.assertEqual(seen[0][1], seen[1][1])
        self.assertEqual(
            first.response_metadata["cache_request"]["prefix_fingerprint"],
            second.response_metadata["cache_request"]["prefix_fingerprint"],
        )
        self.assertEqual(second.usage["cache_hit_tokens"], 90)
        self.assertEqual(second.response_metadata["cache"]["cacheable_token_hit_rate"], 0.9)
        self.assertEqual(ledger.snapshot("process|deepseek|deepseek-chat")["requests"], 2)

    def test_openai_compat_adapter_attaches_native_cache_usage(self) -> None:
        from lingxigraph.integrations import OpenAICompatChatModel

        requests: list[dict] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            requests.append(json.loads(request.content))
            return httpx.Response(
                200,
                json={
                    "model": "deepseek-chat",
                    "choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}],
                    "usage": {
                        "prompt_tokens": 100,
                        "completion_tokens": 2,
                        "total_tokens": 102,
                        "prompt_cache_hit_tokens": 90,
                        "prompt_cache_miss_tokens": 10,
                    },
                },
            )

        async def run() -> AIMessage:
            model = OpenAICompatChatModel(
                "deepseek-chat",
                base_url="https://example.test/v1",
                api_key="key",
                transport=httpx.MockTransport(handler),
            )
            response = await model.agenerate([HumanMessage("hello")])
            await model.aclose()
            return response

        response = asyncio.run(run())
        self.assertEqual(response.usage["prompt_cache_hit_tokens"], 90)
        self.assertEqual(response.usage["cache_hit_tokens"], 90)
        self.assertEqual(response.usage["cache_miss_tokens"], 10)
        self.assertEqual(response.response_metadata["cache"]["cache_hit_rate"], 0.9)
        self.assertEqual(requests[0]["messages"][0]["role"], "user")

    def test_mcp_progressive_tools_have_fixed_names(self) -> None:
        from lingxigraph.protocols import MCPToolset

        tools = MCPToolset("https://mcp.example.test/rpc").progressive_tools()
        self.assertEqual(
            tuple(item.name for item in tools),
            ("mcp_search", "mcp_describe", "mcp_call", "mcp_refresh_catalog"),
        )
        self.assertEqual(
            ImmutablePrefix.create(tools=tools).fingerprint,
            ImmutablePrefix.create(tools=tuple(reversed(tools))).fingerprint,
        )


if __name__ == "__main__":
    unittest.main()
