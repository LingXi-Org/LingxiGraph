"""Live DeepSeek cache benchmark for baseline vs cache-first request assembly.

This script intentionally does not invent provider results.  It exits with a
clear error when DEEPSEEK_API_KEY is absent and records all run parameters in
the JSON report when a live run is performed.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from lingxigraph import (
    CacheFirstConfig,
    HumanMessage,
    ImmutablePrefix,
    merge_chunks,
    normalize_usage,
)
from lingxigraph.integrations import OpenAICompatChatModel

TOOLS = (
    {
        "type": "function",
        "function": {
            "name": "lookup_repository",
            "description": "Look up a repository fact when the user explicitly asks.",
            "parameters": {
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": ["query"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_file_excerpt",
            "description": "Read a small file excerpt when the user explicitly asks.",
            "parameters": {
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"],
                "additionalProperties": False,
            },
        },
    },
)


def load_pricing(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    return json.loads(Path(path).read_text(encoding="utf-8"))


def rate(hit: int, miss: int) -> float | None:
    return hit / (hit + miss) if hit + miss else None


def summarize(records: list[dict[str, Any]]) -> dict[str, Any]:
    hit = sum(int(item.get("cache_hit_tokens") or 0) for item in records)
    miss = sum(int(item.get("cache_miss_tokens") or 0) for item in records)
    prompt = sum(int(item.get("prompt_tokens") or 0) for item in records)
    completion = sum(int(item.get("completion_tokens") or 0) for item in records)
    total = sum(int(item.get("total_tokens") or 0) for item in records)
    costs = [
        item.get("estimated_cost") for item in records if item.get("estimated_cost") is not None
    ]
    savings = [
        item.get("estimated_cost_savings")
        for item in records
        if item.get("estimated_cost_savings") is not None
    ]
    token_savings = [
        item.get("token_savings") for item in records if item.get("token_savings") is not None
    ]
    return {
        "requests": len(records),
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": total,
        "cache_hit_tokens": hit,
        "cache_miss_tokens": miss,
        "cache_hit_rate": rate(hit, miss),
        "total_input_token_hit_rate": hit / prompt if prompt else None,
        "token_savings": sum(int(value) for value in token_savings) if token_savings else None,
        "estimated_cost": sum(float(value) for value in costs) if costs else None,
        "estimated_cost_savings": sum(float(value) for value in savings) if savings else None,
        "ttft_ms_mean": statistics.fmean(
            [float(item["ttft_ms"]) for item in records if item.get("ttft_ms") is not None]
        )
        if any(item.get("ttft_ms") is not None for item in records)
        else None,
        "full_latency_ms_mean": statistics.fmean(float(item["latency_ms"]) for item in records)
        if records
        else None,
    }


async def one_turn(
    model: OpenAICompatChatModel,
    messages: list[Any],
    tools: tuple[dict[str, Any], ...],
) -> tuple[Any, dict[str, Any]]:
    started = time.perf_counter()
    first_chunk_at: float | None = None
    chunks = []
    async for chunk in model.astream(messages, tools=tools):
        if first_chunk_at is None:
            first_chunk_at = time.perf_counter()
        chunks.append(chunk)
    response = merge_chunks(chunks)
    latency_ms = (time.perf_counter() - started) * 1000
    metadata = dict(response.response_metadata)
    cache = dict(metadata.get("cache") or {})
    usage = normalize_usage(
        response.usage, provider="deepseek", model=model.model, pricing=model.pricing
    )
    normalized = usage.as_dict()
    return response, {
        **normalized,
        "latency_ms": latency_ms,
        "ttft_ms": (first_chunk_at - started) * 1000 if first_chunk_at is not None else None,
        "cache": cache,
        "prefix_fingerprint": (metadata.get("cache_request") or {}).get("prefix_fingerprint"),
        "tool_catalog_fingerprint": (metadata.get("cache_request") or {}).get(
            "tool_catalog_fingerprint"
        ),
    }


async def run_series(
    args: argparse.Namespace, *, optimized: bool, pricing: dict[str, Any]
) -> dict[str, Any]:
    model_kwargs = {
        "model": args.model,
        "base_url": args.endpoint,
        "api_key": os.environ["DEEPSEEK_API_KEY"],
        "default_options": {
            "temperature": args.temperature,
            "max_tokens": args.max_output_tokens,
        },
        "pricing": pricing,
    }
    if optimized:
        prefix = ImmutablePrefix.create(
            system_prompt="You are a concise repository assistant. Answer from supplied evidence.",
            pinned_constraints=(
                "Do not invent repository facts.",
                "Keep volatile context after this prefix.",
            ),
            tools=TOOLS,
        )
        model_kwargs["immutable_prefix"] = prefix
        model_kwargs["cache_first"] = CacheFirstConfig(
            verify_mode="strict",
            context_window_tokens=args.context_window_tokens,
            hard_threshold_tokens=args.context_window_tokens,
            max_output_tokens=args.max_output_tokens,
        )
    else:
        model_kwargs["cache_first"] = False
        prefix = None
    model = OpenAICompatChatModel(**model_kwargs)
    history: list[Any] = []
    records: list[dict[str, Any]] = []
    labels = [
        "cold",
        "warm_up",
        *[f"steady_{index}" for index in range(1, args.steady_turns + 1)],
    ]
    try:
        for index, label in enumerate(labels):
            if optimized:
                user_content = (
                    f"Turn {index}: summarize the stable repository policy in one sentence."
                )
                request = [*history, HumanMessage(user_content)]
            else:
                # Deliberately unstable material appears before the history in
                # the baseline request, preventing the provider from reusing
                # the prior prompt prefix.
                request = [
                    HumanMessage(
                        f"SYSTEM policy snapshot at {datetime.now(UTC).isoformat()}: "
                        "Answer from supplied evidence."
                    ),
                    *history,
                    HumanMessage(
                        f"Turn {index}: summarize the stable repository policy in one sentence."
                    ),
                ]
            response, record = await one_turn(model, request, TOOLS)
            record["label"] = label
            record["prefix_fingerprint"] = record.get("prefix_fingerprint") or (
                prefix.fingerprint
                if prefix is not None
                else ImmutablePrefix.create(
                    system_prompt=str(request[0].content), tools=TOOLS
                ).fingerprint
            )
            records.append(record)
            history.extend(
                (
                    HumanMessage(
                        f"Turn {index}: summarize the stable repository policy in one sentence."
                    ),
                    response,
                )
            )
    finally:
        await model.aclose()
    return {
        "mode": "optimized" if optimized else "baseline",
        "summary": summarize(records),
        "records": records,
        "prefix_fingerprint": prefix.fingerprint if prefix is not None else None,
    }


async def async_main(args: argparse.Namespace) -> dict[str, Any]:
    key = os.getenv("DEEPSEEK_API_KEY")
    if not key:
        raise SystemExit("DEEPSEEK_API_KEY is required for the live benchmark")
    pricing = load_pricing(args.pricing_file)
    started = datetime.now(UTC)
    baseline = await run_series(args, optimized=False, pricing=pricing)
    optimized = await run_series(args, optimized=True, pricing=pricing)
    return {
        "benchmark": "lingxigraph-deepseek-cache-first",
        "started_at": started.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "model": args.model,
        "endpoint": args.endpoint,
        "temperature": args.temperature,
        "max_output_tokens": args.max_output_tokens,
        "steady_turns": args.steady_turns,
        "request_count_per_mode": args.steady_turns + 2,
        "pricing": pricing,
        "provider_cache_note": "DeepSeek context caching is best-effort; TTL/routing/model variance is not hidden.",
        "baseline": baseline,
        "optimized": optimized,
        "comparison": {
            "cold_inclusive_hit_rate": {
                "baseline": baseline["summary"]["cache_hit_rate"],
                "optimized": optimized["summary"]["cache_hit_rate"],
            },
            "steady_state_hit_rate": {
                "baseline": rate(
                    sum(
                        int(item.get("cache_hit_tokens") or 0)
                        for item in baseline["records"][2:]
                    ),
                    sum(
                        int(item.get("cache_miss_tokens") or 0)
                        for item in baseline["records"][2:]
                    ),
                ),
                "optimized": rate(
                    sum(
                        int(item.get("cache_hit_tokens") or 0)
                        for item in optimized["records"][2:]
                    ),
                    sum(
                        int(item.get("cache_miss_tokens") or 0)
                        for item in optimized["records"][2:]
                    ),
                ),
            },
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default="https://api.deepseek.com/v1")
    parser.add_argument("--model", default="deepseek-chat")
    parser.add_argument("--temperature", type=float, default=0.0)
    parser.add_argument("--max-output-tokens", type=int, default=256)
    parser.add_argument("--steady-turns", type=int, default=10)
    parser.add_argument("--context-window-tokens", type=int, default=128_000)
    parser.add_argument(
        "--pricing-file", help="JSON file containing the model pricing snapshot"
    )
    parser.add_argument("--output", default="artifacts/deepseek-cache-benchmark.json")
    args = parser.parse_args()
    if args.steady_turns < 1:
        parser.error("--steady-turns must be positive")
    report = asyncio.run(async_main(args))
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report["comparison"], ensure_ascii=False, indent=2))
    print(f"saved={output}")


if __name__ == "__main__":
    main()
