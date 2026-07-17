from __future__ import annotations

from typing import Any

import pytest

from lingxigraph_chainlit.models import Projection
from lingxigraph_chainlit.ui import ChainlitRenderer


class FakeStep:
    instances: list[FakeStep] = []

    def __init__(self, **values: Any) -> None:
        self.__dict__.update(values)
        self.metadata = values.get("metadata", {})
        self.input = ""
        self.output = ""
        self.is_error = False
        self.sent = 0
        self.updated = 0
        self.instances.append(self)

    async def send(self):
        self.sent += 1
        return self

    async def update(self):
        self.updated += 1
        return True


class FakeMessage:
    instances: list[FakeMessage] = []

    def __init__(self, content: str, **values: Any) -> None:
        self.content = content
        self.values = values
        self.tokens: list[str] = []
        self.sent = 0
        self.is_error = False
        self.instances.append(self)

    async def stream_token(self, token: str) -> None:
        self.tokens.append(token)

    async def send(self):
        self.sent += 1
        return self


class FakeAskUserMessage:
    response: dict[str, Any] | None = {"output": "yes"}

    def __init__(self, **values: Any) -> None:
        self.values = values

    async def send(self):
        return self.response


@pytest.fixture
def fake_chainlit(monkeypatch: pytest.MonkeyPatch):
    from lingxigraph_chainlit import ui

    FakeStep.instances.clear()
    FakeMessage.instances.clear()
    monkeypatch.setattr(ui.cl, "Step", FakeStep)
    monkeypatch.setattr(ui.cl, "Message", FakeMessage)
    monkeypatch.setattr(ui.cl, "AskUserMessage", FakeAskUserMessage)


@pytest.mark.asyncio
async def test_renderer_materializes_all_projection_types(fake_chainlit) -> None:
    renderer = ChainlitRenderer(default_open=True)
    metadata = {"thread_id": "thread", "run_id": "run"}
    await renderer.render(Projection("run_start", "run", name="run", metadata=metadata))
    await renderer.render(
        Projection("node_start", "node", name="agent", metadata=metadata, status="running")
    )
    await renderer.render(
        Projection("node_update", "node", content="done", status="completed")
    )
    await renderer.render(
        Projection("node_update", "late-node", name="late", status="retrying")
    )
    await renderer.render(Projection("assistant_token", "message", content="hel"))
    await renderer.render(Projection("assistant_token", "message", content="lo"))
    await renderer.render(Projection("assistant_end", "message"))
    await renderer.render(Projection("assistant_message", "final", content="answer"))
    await renderer.render(
        Projection("tool_start", "tool", name="lookup", content="input", metadata=metadata)
    )
    await renderer.render(
        Projection("tool_start", "tool", name="lookup", content="input-2", metadata=metadata)
    )
    await renderer.render(
        Projection("tool_end", "tool", content="output", status="success")
    )
    await renderer.render(
        Projection("tool_end", "late-tool", name="late", status="error", is_error=True)
    )
    await renderer.render(Projection("custom", "custom", name="progress", content="hidden"))
    await renderer.render(
        Projection("run_end", "run", content="complete", status="completed")
    )

    assert FakeMessage.instances[0].tokens == ["hel", "lo"]
    assert FakeMessage.instances[0].sent == 1
    assert FakeMessage.instances[1].content == "answer"
    assert any(step.name == "custom:progress" for step in FakeStep.instances)
    assert any(step.is_error for step in FakeStep.instances)


@pytest.mark.asyncio
async def test_renderer_ask_timeout_and_error(fake_chainlit) -> None:
    renderer = ChainlitRenderer()
    prompt = Projection("interrupt", "i", content="Approve?")
    assert await renderer.ask(prompt) == "yes"
    FakeAskUserMessage.response = None
    assert await renderer.ask(prompt) is None
    FakeAskUserMessage.response = {"output": None}
    assert await renderer.ask(prompt) is None
    await renderer.error("failed")
    assert FakeMessage.instances[-1].is_error
    FakeAskUserMessage.response = {"output": "yes"}
