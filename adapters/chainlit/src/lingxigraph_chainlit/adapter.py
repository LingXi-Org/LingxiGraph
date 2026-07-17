"""Embedded LingxiGraph lifecycle hosted by Chainlit callbacks."""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import uuid4

import chainlit as cl
from lingxigraph import (
    CancellationToken,
    Command,
    CompiledGraph,
    CompiledStateGraph,
    EmptyInputError,
    GraphCancelledError,
    HumanMessage,
    SqliteSaver,
)
from lingxigraph.checkpoint import Checkpointer
from lingxigraph.events import EventKind
from lingxigraph.types import Interrupt

from .models import ObservabilityOptions, Projection, SessionInfo
from .projector import EventProjector
from .ui import ChainlitRenderer, ProjectionRenderer

logger = logging.getLogger(__name__)

ContextFactory = Callable[
    [SessionInfo, str | None], Mapping[str, Any] | Awaitable[Mapping[str, Any]]
]
RendererFactory = Callable[[], ProjectionRenderer]


@dataclass(slots=True)
class _ThreadState:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    cancellation: CancellationToken | None = None


class ChainlitAdapter:
    """Bind one embedded MessagesState graph to Chainlit callbacks."""

    def __init__(
        self,
        graph: CompiledGraph | Callable[[], CompiledGraph],
        *,
        checkpointer: Checkpointer | None = None,
        sqlite_path: str | Path = ".chainlit/lingxigraph.db",
        context: Mapping[str, Any] | None = None,
        context_factory: ContextFactory | None = None,
        observability: ObservabilityOptions | None = None,
        renderer_factory: RendererFactory | None = None,
    ) -> None:
        value = graph() if callable(graph) and not isinstance(graph, CompiledStateGraph) else graph
        if not isinstance(value, CompiledStateGraph):
            raise TypeError("graph must be a CompiledGraph or a zero-argument factory")
        self.observability = observability or ObservabilityOptions()
        self._context = dict(context or {})
        self._context_factory = context_factory
        self._renderer_factory = renderer_factory or (
            lambda: ChainlitRenderer(default_open=self.observability.default_open)
        )
        self._states: dict[str, _ThreadState] = {}
        self._owns_checkpointer = False

        selected = checkpointer or value.checkpointer
        if selected is None:
            database = str(sqlite_path)
            if database != ":memory:":
                Path(database).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
            selected = SqliteSaver(database)
            self._owns_checkpointer = True
        self.checkpointer = selected
        self.graph = value.with_runtime(checkpointer=selected)

    def install(self) -> ChainlitAdapter:
        """Register this adapter as the callback owner for the current Chainlit app."""

        cl.on_chat_start(self.on_chat_start)
        cl.on_message(self.on_message)
        cl.on_chat_resume(self.on_chat_resume)
        cl.on_chat_end(self.on_chat_end)
        cl.on_stop(self.on_stop)
        cl.on_app_shutdown(self.on_app_shutdown)
        return self

    async def on_chat_start(self) -> None:
        self._thread_state(self._session_info().thread_id)

    async def on_chat_resume(self, _thread: Mapping[str, Any]) -> None:
        session = self._session_info()
        renderer = self._renderer_factory()
        state = self._thread_state(session.thread_id)
        async with state.lock:
            markers = await self._pending_interrupts(session.thread_id)
            if markers:
                value = await self._answers(markers, renderer)
                if value is not None:
                    await self._drive(Command(resume=value), session, renderer, None)

    async def on_message(self, message: cl.Message) -> None:
        session = self._session_info()
        renderer = self._renderer_factory()
        state = self._thread_state(session.thread_id)
        async with state.lock:
            markers = await self._pending_interrupts(session.thread_id)
            if markers:
                value = await self._answers(markers, renderer, first_answer=message.content)
                if value is not None:
                    await self._drive(Command(resume=value), session, renderer, message.content)
                return
            graph_input = {
                "messages": [HumanMessage(message.content, id=message.id)],
            }
            await self._drive(graph_input, session, renderer, message.content)

    async def on_stop(self) -> None:
        state = self._states.get(self._session_info().thread_id)
        if state is not None and state.cancellation is not None:
            state.cancellation.cancel()

    async def on_chat_end(self) -> None:
        await self.on_stop()

    async def on_app_shutdown(self) -> None:
        if self._owns_checkpointer:
            close = getattr(self.checkpointer, "close", None)
            if close is not None:
                close()

    async def _drive(
        self,
        graph_input: Mapping[str, Any] | Command[Any],
        session: SessionInfo,
        renderer: ProjectionRenderer,
        user_message: str | None,
    ) -> None:
        state = self._thread_state(session.thread_id)
        current: Mapping[str, Any] | Command[Any] = graph_input
        while True:
            cancellation = CancellationToken()
            state.cancellation = cancellation
            projector = EventProjector(self.observability)
            markers: tuple[Interrupt, ...] = ()
            try:
                context = await self._resolve_context(session, user_message)
                async for event in self.graph.astream(
                    current,
                    self._config(session.thread_id),
                    context=context,
                    stream_mode="events",
                    subgraphs=True,
                    run_id=str(uuid4()),
                    cancellation=cancellation,
                ):
                    if event.kind is EventKind.INTERRUPT_RAISED:
                        markers = tuple(event.data.get("interrupts", ()))
                    for projection in projector.project(event):
                        if projection.kind != "interrupt":
                            await renderer.render(projection)
                for projection in projector.finish():
                    await renderer.render(projection)
            except GraphCancelledError:
                logger.info("LingxiGraph run cancelled", extra={"thread_id": session.thread_id})
                return
            except Exception:
                logger.exception("LingxiGraph Chainlit run failed")
                await renderer.error("The graph run failed. Please try again.")
                return
            finally:
                if state.cancellation is cancellation:
                    state.cancellation = None

            if not markers:
                return
            answer = await self._answers(markers, renderer)
            if answer is None:
                return
            current = Command(resume=answer)
            user_message = None

    async def _answers(
        self,
        markers: tuple[Interrupt, ...],
        renderer: ProjectionRenderer,
        *,
        first_answer: str | None = None,
    ) -> Any | None:
        answers: dict[str, Any] = {}
        anonymous: list[Any] = []
        for index, marker in enumerate(markers):
            if not marker.resumable:
                await renderer.error("The graph paused at a non-resumable interrupt.")
                return None
            answer = first_answer if index == 0 and first_answer is not None else None
            if answer is None:
                projection = Projection(
                    "interrupt",
                    str(marker.id or f"interrupt:{index}"),
                    name="Input required",
                    content=EventProjector._interrupt_prompt(marker.value),
                    metadata={"interrupt_id": marker.id, "value": marker.value},
                )
                answer = await renderer.ask(projection)
            if answer is None:
                return None
            if marker.id is None:
                anonymous.append(answer)
            else:
                answers[str(marker.id)] = answer
        if answers and not anonymous:
            return answers
        if len(markers) == 1:
            return next(iter(answers.values()), anonymous[0] if anonymous else None)
        return answers or anonymous

    async def _pending_interrupts(self, thread_id: str) -> tuple[Interrupt, ...]:
        try:
            snapshot = await self.graph.aget_state(self._config(thread_id))
        except EmptyInputError:
            return ()
        return tuple(snapshot.interrupts)

    async def _resolve_context(
        self, session: SessionInfo, user_message: str | None
    ) -> Mapping[str, Any]:
        if self._context_factory is None:
            return dict(self._context)
        value = self._context_factory(session, user_message)
        if inspect.isawaitable(value):
            value = await value
        if not isinstance(value, Mapping):
            raise TypeError("context_factory must return a mapping")
        return dict(value)

    @staticmethod
    def _config(thread_id: str) -> dict[str, Any]:
        return {"configurable": {"thread_id": thread_id}}

    def _thread_state(self, thread_id: str) -> _ThreadState:
        return self._states.setdefault(thread_id, _ThreadState())

    @staticmethod
    def _session_info() -> SessionInfo:
        session = cl.context.session
        user = getattr(session, "user", None)
        return SessionInfo(
            thread_id=str(session.thread_id),
            session_id=str(session.id),
            user_identifier=getattr(user, "identifier", None),
            chat_profile=getattr(session, "chat_profile", None),
        )


def install_chainlit(
    graph: CompiledGraph | Callable[[], CompiledGraph], **kwargs: Any
) -> ChainlitAdapter:
    """Construct and register a :class:`ChainlitAdapter`."""

    return ChainlitAdapter(graph, **kwargs).install()


__all__ = ["ChainlitAdapter", "ContextFactory", "install_chainlit"]
