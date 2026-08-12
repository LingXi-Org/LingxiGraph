"""Minimal DeepSeek cache-first setup; requires DEEPSEEK_API_KEY to run."""

from lingxigraph import CacheFirstConfig, ImmutablePrefix, create_agent
from lingxigraph.integrations import OpenAICompatChatModel


def build_model(tools=()):
    prefix = ImmutablePrefix.create(
        system_prompt="You are a concise engineering assistant.",
        pinned_constraints=(
            "Use only supplied evidence.",
            "Put volatile context after this prefix.",
        ),
        tools=tools,
    )
    model = OpenAICompatChatModel(
        "deepseek-chat",
        base_url="https://api.deepseek.com/v1",
        immutable_prefix=prefix,
        cache_first=CacheFirstConfig(
            verify_mode="strict",
            context_window_tokens=128_000,
            max_output_tokens=1_024,
        ),
    )
    return model, prefix


if __name__ == "__main__":
    # Supply the same stable ``tools`` tuple to every turn.  The live benchmark
    # under scripts/ shows the full cold/warm/steady-state measurement loop.
    model, prefix = build_model()
    graph = create_agent(model, prefix=prefix)
    print(graph.invoke({"messages": [{"type": "human", "content": "Say hello."}]}))
