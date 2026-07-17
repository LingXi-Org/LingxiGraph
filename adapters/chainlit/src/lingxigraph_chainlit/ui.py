"""Thin Chainlit renderer kept separate from graph event projection."""

from __future__ import annotations

from typing import Protocol

import chainlit as cl

from .models import Projection


class ProjectionRenderer(Protocol):
    async def render(self, projection: Projection) -> None: ...

    async def ask(self, projection: Projection) -> str | None: ...

    async def error(self, message: str) -> None: ...


class ChainlitRenderer:
    """Materialize UI-neutral projections with Chainlit primitives."""

    def __init__(self, *, default_open: bool = False) -> None:
        self.default_open = default_open
        self._runs: dict[str, cl.Step] = {}
        self._nodes: dict[str, cl.Step] = {}
        self._tools: dict[str, cl.Step] = {}
        self._messages: dict[str, cl.Message] = {}

    async def render(self, projection: Projection) -> None:
        run_step: cl.Step | None
        node_step: cl.Step | None
        tool_step: cl.Step | None
        if projection.kind == "run_start":
            run_step = cl.Step(
                name=projection.name,
                type="run",
                metadata=projection.metadata,
                default_open=self.default_open,
                show_input=False,
            )
            run_step.input = {"thread_id": projection.metadata.get("thread_id")}
            await run_step.send()
            self._runs[projection.key] = run_step
            return
        if projection.kind == "run_end":
            run_step = self._runs.get(projection.key)
            if run_step is not None:
                run_step.output = projection.content or projection.status or "completed"
                run_step.is_error = projection.is_error
                run_step.metadata = {**run_step.metadata, "status": projection.status}
                await run_step.update()
            return
        if projection.kind == "node_start":
            node_step = cl.Step(
                name=projection.name,
                type="tool",
                metadata=projection.metadata,
                default_open=self.default_open,
                show_input=False,
            )
            node_step.input = "running"
            await node_step.send()
            self._nodes[projection.key] = node_step
            return
        if projection.kind == "node_update":
            node_step = self._nodes.get(projection.key)
            if node_step is None:
                node_step = cl.Step(
                    name=projection.name,
                    type="tool",
                    metadata=projection.metadata,
                    default_open=self.default_open,
                    show_input=False,
                )
                await node_step.send()
                self._nodes[projection.key] = node_step
            node_step.output = projection.content or projection.status or "updated"
            node_step.is_error = projection.is_error
            node_step.metadata = {**node_step.metadata, "status": projection.status}
            await node_step.update()
            return
        if projection.kind == "assistant_token":
            message = self._messages.get(projection.key)
            if message is None:
                message = cl.Message(content="", metadata=projection.metadata)
                self._messages[projection.key] = message
            await message.stream_token(str(projection.content or ""))
            return
        if projection.kind == "assistant_message":
            message = cl.Message(content=str(projection.content or ""), metadata=projection.metadata)
            message.is_error = projection.is_error
            await message.send()
            return
        if projection.kind == "assistant_end":
            message = self._messages.pop(projection.key, None)
            if message is not None:
                await message.send()
            return
        if projection.kind == "tool_start":
            tool_step = self._tools.get(projection.key)
            if tool_step is None:
                tool_step = cl.Step(
                    name=projection.name,
                    type="tool",
                    metadata=projection.metadata,
                    default_open=self.default_open,
                )
                tool_step.input = projection.content
                await tool_step.send()
                self._tools[projection.key] = tool_step
            else:
                tool_step.input = projection.content
                tool_step.metadata = {**tool_step.metadata, **projection.metadata}
                await tool_step.update()
            return
        if projection.kind == "tool_end":
            tool_step = self._tools.get(projection.key)
            if tool_step is None:
                tool_step = cl.Step(
                    name=projection.name,
                    type="tool",
                    metadata=projection.metadata,
                    default_open=self.default_open,
                )
                await tool_step.send()
                self._tools[projection.key] = tool_step
            tool_step.output = projection.content or projection.status or "completed"
            tool_step.is_error = projection.is_error
            tool_step.metadata = {**tool_step.metadata, "status": projection.status}
            await tool_step.update()
            return
        if projection.kind == "custom":
            custom_step = cl.Step(
                name=f"custom:{projection.name}",
                type="undefined",
                metadata=projection.metadata,
                default_open=self.default_open,
                show_input=False,
            )
            custom_step.output = projection.content
            await custom_step.send()

    async def ask(self, projection: Projection) -> str | None:
        response = await cl.AskUserMessage(
            content=str(projection.content),
            timeout=90,
            raise_on_timeout=False,
        ).send()
        if response is None:
            return None
        value = response.get("output")
        return None if value is None else str(value)

    async def error(self, message: str) -> None:
        value = cl.Message(content=message)
        value.is_error = True
        await value.send()


__all__ = ["ChainlitRenderer", "ProjectionRenderer"]
