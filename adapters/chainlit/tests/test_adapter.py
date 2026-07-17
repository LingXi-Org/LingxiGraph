from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from lingxigraph import (
    END,
    START,
    AIMessage,
    AIMessageChunk,
    HumanMessage,
    Interrupt,
    MessagesState,
    Runtime,
    StateGraph,
    interrupt,
)

from lingxigraph_chainlit import ChainlitAdapter, ObservabilityOptions, SessionInfo
from lingxigraph_chainlit.models import Projection


class FakeRenderer:
    def __init__(self, answers: list[str] | None = None) -> None:
        self.projections: list[Projection] = []
        self.answers = list(answers or [])
        self.errors: list[str] = []

    async def render(self, projection: Projection) -> None:
        self.projections.append(projection)

    async def ask(self, projection: Projection) -> str | None:
        self.projections.append(projection)
        return self.answers.pop(0) if self.answers else None

    async def error(self, message: str) -> None:
        self.errors.append(message)


def streaming_graph():
    async def answer(state: MessagesState, runtime: Runtime[Any]):
        latest = state["messages"][-1].content
        runtime.emit("progress", {"input": latest})
        runtime.emit_message(AIMessageChunk("echo:", id=f"answer-{latest}"))
        runtime.emit_message(AIMessageChunk(str(latest), id=f"answer-{latest}"))
        return {"messages": [AIMessage(f"echo:{latest}", id=f"answer-{latest}")]}

    builder = StateGraph(MessagesState, name="chainlit-test")
    builder.add_node("agent", answer).add_edge(START, "agent").add_edge("agent", END)
    return builder.compile()


@pytest.mark.asyncio
async def test_embedded_streaming_multiturn_and_sqlite_continuity(tmp_path: Path) -> None:
    database = tmp_path / "checkpoints.db"
    renderer = FakeRenderer()
    adapter = ChainlitAdapter(streaming_graph(), sqlite_path=database)
    session = SessionInfo("thread-1", "session-1")

    await adapter._drive(
        {"messages": [HumanMessage("one", id="u1")]}, session, renderer, "one"
    )
    await adapter._drive(
        {"messages": [HumanMessage("two", id="u2")]}, session, renderer, "two"
    )
    snapshot = await adapter.graph.aget_state(adapter._config("thread-1"))
    assert [message.content for message in snapshot.values["messages"]] == [
        "one",
        "echo:one",
        "two",
        "echo:two",
    ]
    assert "".join(
        item.content for item in renderer.projections if item.kind == "assistant_token"
    ) == "echo:oneecho:two"
    custom = next(item for item in renderer.projections if item.kind == "custom")
    assert "one" not in custom.content

    adapter.checkpointer.close()
    reopened = ChainlitAdapter(streaming_graph(), sqlite_path=database)
    persisted = await reopened.graph.aget_state(reopened._config("thread-1"))
    assert persisted.values["messages"][-1].content == "echo:two"
    reopened.checkpointer.close()


@pytest.mark.asyncio
async def test_interrupt_is_asked_and_resumed(tmp_path: Path) -> None:
    def approval(state: MessagesState):
        answer = interrupt({"question": "Approve?"})
        return {"messages": [AIMessage(f"approved:{answer}")]}

    builder = StateGraph(MessagesState).add_node("approval", approval)
    builder.add_edge(START, "approval").add_edge("approval", END)
    renderer = FakeRenderer(["yes"])
    adapter = ChainlitAdapter(builder.compile(), sqlite_path=tmp_path / "interrupt.db")
    session = SessionInfo("approval-thread", "session")

    await adapter._drive(
        {"messages": [HumanMessage("start")]}, session, renderer, "start"
    )
    snapshot = await adapter.graph.aget_state(adapter._config(session.thread_id))
    assert not snapshot.interrupts
    assert snapshot.values["messages"][-1].content == "approved:yes"
    assert any(item.kind == "interrupt" and item.content == "Approve?" for item in renderer.projections)
    adapter.checkpointer.close()


@pytest.mark.asyncio
async def test_parallel_interrupt_answers_use_interrupt_ids(tmp_path: Path) -> None:
    def left(state: MessagesState):
        return {"messages": [AIMessage(f"left:{interrupt('Left?')}")]}

    def right(state: MessagesState):
        return {"messages": [AIMessage(f"right:{interrupt('Right?')}")]}

    builder = StateGraph(MessagesState)
    builder.add_node("left", left).add_node("right", right)
    builder.add_edge(START, "left").add_edge(START, "right")
    builder.add_edge("left", END).add_edge("right", END)
    renderer = FakeRenderer(["L", "R"])
    adapter = ChainlitAdapter(builder.compile(), sqlite_path=tmp_path / "parallel.db")

    await adapter._drive(
        {"messages": [HumanMessage("start")]},
        SessionInfo("parallel", "session"),
        renderer,
        "start",
    )
    snapshot = await adapter.graph.aget_state(adapter._config("parallel"))
    contents = [message.content for message in snapshot.values["messages"]]
    assert "left:L" in contents
    assert "right:R" in contents
    adapter.checkpointer.close()


@pytest.mark.asyncio
async def test_cancellation_token_stops_active_run(tmp_path: Path) -> None:
    started = asyncio.Event()

    async def slow(state: MessagesState, runtime: Runtime[Any]):
        started.set()
        while True:
            runtime.raise_if_cancelled()
            await asyncio.sleep(0.01)

    builder = StateGraph(MessagesState).add_node("slow", slow)
    builder.add_edge(START, "slow").add_edge("slow", END)
    adapter = ChainlitAdapter(builder.compile(), sqlite_path=tmp_path / "cancel.db")
    session = SessionInfo("cancel", "session")
    task = asyncio.create_task(
        adapter._drive(
            {"messages": [HumanMessage("start")]}, session, FakeRenderer(), "start"
        )
    )
    await asyncio.wait_for(started.wait(), timeout=1)
    cancellation = adapter._thread_state(session.thread_id).cancellation
    assert cancellation is not None
    cancellation.cancel()
    await asyncio.wait_for(task, timeout=1)
    adapter.checkpointer.close()


def test_checkpointer_precedence_and_options(tmp_path: Path) -> None:
    from lingxigraph import InMemorySaver

    saver = InMemorySaver()
    graph = streaming_graph().with_runtime(checkpointer=saver)
    adapter = ChainlitAdapter(
        graph,
        sqlite_path=tmp_path / "unused.db",
        observability=ObservabilityOptions(show_tool_io=True),
    )
    assert adapter.checkpointer is saver
    assert not (tmp_path / "unused.db").exists()


@pytest.mark.asyncio
async def test_callback_lifecycle_and_pending_message_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def approval(state: MessagesState):
        value = interrupt("Continue?")
        return {"messages": [AIMessage(f"continued:{value}")]}

    builder = StateGraph(MessagesState).add_node("approval", approval)
    builder.add_edge(START, "approval").add_edge("approval", END)
    renderers: list[FakeRenderer] = []

    def renderer_factory():
        renderer = FakeRenderer()
        renderers.append(renderer)
        return renderer

    adapter = ChainlitAdapter(
        builder.compile(),
        sqlite_path=tmp_path / "callbacks.db",
        renderer_factory=renderer_factory,
    )
    from lingxigraph_chainlit import adapter as adapter_module

    session = SimpleNamespace(
        thread_id="callback-thread",
        id="session",
        user=SimpleNamespace(identifier="alice"),
        chat_profile="support",
    )
    monkeypatch.setattr(adapter_module.cl, "context", SimpleNamespace(session=session))
    registered = []
    for name in (
        "on_chat_start",
        "on_message",
        "on_chat_resume",
        "on_chat_end",
        "on_stop",
        "on_app_shutdown",
    ):
        monkeypatch.setattr(
            adapter_module.cl,
            name,
            lambda callback, hook=name: registered.append((hook, callback)) or callback,
        )

    assert adapter.install() is adapter
    assert {name for name, _ in registered} == {
        "on_chat_start",
        "on_message",
        "on_chat_resume",
        "on_chat_end",
        "on_stop",
        "on_app_shutdown",
    }
    await adapter.on_chat_start()
    await adapter._drive(
        {"messages": [HumanMessage("start")]},
        SessionInfo("callback-thread", "session"),
        FakeRenderer(),
        "start",
    )
    assert await adapter._pending_interrupts("callback-thread")

    await adapter.on_message(SimpleNamespace(content="yes", id="reply"))
    snapshot = await adapter.graph.aget_state(adapter._config("callback-thread"))
    assert snapshot.values["messages"][-1].content == "continued:yes"
    assert adapter._session_info().user_identifier == "alice"

    adapter._thread_state("callback-thread").cancellation = SimpleNamespace(  # type: ignore[assignment]
        cancel=lambda: None
    )
    await adapter.on_stop()
    await adapter.on_chat_end()
    adapter._thread_state("callback-thread").cancellation = None
    await adapter.on_app_shutdown()


@pytest.mark.asyncio
async def test_context_factories_and_interrupt_edge_cases(tmp_path: Path) -> None:
    async def context_factory(session: SessionInfo, text: str | None):
        return {"user": session.user_identifier, "text": text}

    adapter = ChainlitAdapter(
        streaming_graph(),
        sqlite_path=tmp_path / "context.db",
        context_factory=context_factory,
    )
    session = SessionInfo("thread", "session", "alice")
    assert await adapter._resolve_context(session, "hello") == {
        "user": "alice",
        "text": "hello",
    }
    adapter._context_factory = lambda _session, _text: "invalid"  # type: ignore[assignment]
    with pytest.raises(TypeError):
        await adapter._resolve_context(session, None)

    renderer = FakeRenderer()
    marker = Interrupt("stop", resumable=False)
    assert await adapter._answers((marker,), renderer) is None
    assert renderer.errors
    anonymous = Interrupt("answer", id=None)
    assert await adapter._answers((anonymous,), FakeRenderer(["ok"])) == "ok"
    adapter.checkpointer.close()


@pytest.mark.asyncio
async def test_graph_failure_returns_one_safe_error(tmp_path: Path) -> None:
    def fail(_state: MessagesState):
        raise RuntimeError("internal secret")

    builder = StateGraph(MessagesState).add_node("fail", fail)
    builder.add_edge(START, "fail").add_edge("fail", END)
    renderer = FakeRenderer()
    adapter = ChainlitAdapter(builder.compile(), sqlite_path=tmp_path / "failure.db")
    await adapter._drive(
        {"messages": [HumanMessage("start")]},
        SessionInfo("failure", "session"),
        renderer,
        "start",
    )
    assert renderer.errors == ["The graph run failed. Please try again."]
    run_end = next(item for item in renderer.projections if item.kind == "run_end")
    assert run_end.content == "failed"
    adapter.checkpointer.close()
