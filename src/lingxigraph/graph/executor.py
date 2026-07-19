"""Pregel-style superstep executor for compiled state graphs."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import inspect
import random
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import partial
from types import MappingProxyType
from typing import Any
from uuid import uuid4

from ..cache import BaseCache
from ..channels import Channel, ReplaceValue, merge_updates
from ..checkpoint import Checkpoint, Checkpointer, PendingWrite
from ..constants import END, START
from ..errors import (
    BudgetExceededError,
    EmptyInputError,
    GraphCancelledError,
    GraphInterrupt,
    GraphRecursionError,
    GraphTimeoutError,
    GraphValidationError,
    InvalidUpdateError,
    PersistenceError,
)
from ..events import Event, EventKind
from ..observability import start_span
from ..runtime import (
    CancellationToken,
    ExecutionBudget,
    Runtime,
    _reset_runtime_context,
    _RuntimeContext,
    _set_runtime_context,
    get_runtime,
)
from ..schema import SchemaAdapter
from ..serialization import JsonSerializer, Serializer
from ..types import (
    Command,
    CommandScope,
    Durability,
    Interrupt,
    Send,
    StateSnapshot,
    SubgraphPersistence,
    TaskSnapshot,
    _InterruptContext,
    _reset_interrupt_context,
    _set_interrupt_context,
    _utc_now,
)
from .builder import _ConditionalEdge, _Edge, _NodeSpec
from .structure import EdgeInfo, GraphInfo, NodeInfo

StreamMode = str | Sequence[str]


@dataclass(frozen=True, slots=True)
class _Task:
    """One unit of work in a superstep: a named node or a Send delivery."""

    id: str
    node: str
    send: Send | None = None
    path: tuple[str, ...] = ()


@dataclass(slots=True)
class _TaskResult:
    task: _Task
    update: Mapping[str, Any]
    goto: tuple[str | Send, ...]
    interrupt: Interrupt | None = None
    cached: bool = False
    parent_command: Command[Any] | None = None


class _ParentCommand(BaseException):
    """Control-flow signal that crosses exactly one subgraph boundary."""

    def __init__(self, command: Command[Any]) -> None:
        self.command = command
        super().__init__("command targets parent graph")


class _AsyncCheckpointWriter:
    """Per-run ordered checkpoint writer used by ``Durability.ASYNC``."""

    def __init__(self) -> None:
        self._tail: asyncio.Task[Any] | None = None

    def enqueue(self, operation: Callable[[], Awaitable[Any]]) -> None:
        previous = self._tail

        async def write() -> Any:
            if previous is not None:
                await previous
            return await operation()

        self._tail = asyncio.create_task(write())

    async def flush(self) -> None:
        if self._tail is not None:
            await asyncio.shield(self._tail)


@dataclass(frozen=True, slots=True)
class _CachedResult:
    value: Mapping[str, Any]


def _callable_arity(action: Callable[..., Any]) -> int:
    """Return 2 when the callable accepts a second positional config argument."""

    try:
        signature = inspect.signature(action)
    except (TypeError, ValueError):
        return 1
    positional = 0
    for parameter in signature.parameters.values():
        if parameter.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            positional += 1
        elif parameter.kind is inspect.Parameter.VAR_POSITIONAL:
            return 2
    return 2 if positional >= 2 else 1


def _describe_callable(action: Callable[..., Any] | None) -> str | None:
    """A stable human-readable label for a node's callable, for the debugger."""

    if action is None:
        return None
    for attribute in ("__qualname__", "__name__"):
        label = getattr(action, attribute, None)
        if label:
            return str(label)
    return type(action).__name__


def _callable_uses_runtime(action: Callable[..., Any]) -> bool:
    """Return whether the callable's second argument requests Runtime."""

    try:
        parameters = list(inspect.signature(action).parameters.values())
    except (TypeError, ValueError):
        return False
    positional = [
        parameter
        for parameter in parameters
        if parameter.kind
        in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    if len(positional) < 2:
        return False
    parameter = positional[1]
    annotation = parameter.annotation
    return parameter.name == "runtime" or annotation is Runtime or getattr(
        annotation, "__origin__", None
    ) is Runtime


class CompiledStateGraph:
    """An immutable graph definition that can be invoked repeatedly."""

    def __init__(
        self,
        *,
        state_schema: type,
        input_schema: type | None = None,
        output_schema: type | None = None,
        context_schema: type | None = None,
        graph_name: str | None = None,
        graph_version: str = "1",
        channels: dict[str, Channel],
        nodes: dict[str, _NodeSpec],
        edges: tuple[_Edge, ...],
        conditional_edges: tuple[_ConditionalEdge, ...],
        checkpointer: Checkpointer | None,
        store: Any | None = None,
        cache: BaseCache | None = None,
        serializer: Serializer | None = None,
        step_timeout: float | None = None,
        interrupt_before: tuple[str, ...],
        interrupt_after: tuple[str, ...],
    ) -> None:
        self.state_schema = state_schema
        self.input_schema = input_schema or state_schema
        self.output_schema = output_schema or state_schema
        self._state_adapter = SchemaAdapter(state_schema)
        self._output_adapter = SchemaAdapter(self.output_schema)
        state_keys = frozenset(self._state_adapter.fields)
        output_keys = frozenset(self._output_adapter.fields)
        self._output_keys = None if output_keys == state_keys else output_keys
        self.context_schema = context_schema
        self.graph_name = str(graph_name or getattr(state_schema, "__name__", "graph"))
        self.graph_version = graph_version
        self.schema_hash = SchemaAdapter(state_schema).fingerprint()
        self.channels = MappingProxyType(channels)
        self.nodes = MappingProxyType(nodes)
        self._node_order = {name: index for index, name in enumerate(nodes)}
        self._edges = edges
        self._conditional_edges = conditional_edges
        self.checkpointer = checkpointer
        self.store = store
        self.cache = cache
        self.serializer = serializer or JsonSerializer()
        self.step_timeout = step_timeout
        self.interrupt_before = frozenset(interrupt_before)
        self.interrupt_after = frozenset(interrupt_after)
        self._node_arity = {
            name: 1 if spec.subgraph is not None else _callable_arity(spec.action)
            for name, spec in nodes.items()
        }
        self._node_runtime = {
            name: False if spec.subgraph is not None else _callable_uses_runtime(spec.action)
            for name, spec in nodes.items()
        }
        self._path_arity = {
            index: _callable_arity(conditional.path)
            for index, conditional in enumerate(conditional_edges)
        }
        self._path_runtime = {
            index: _callable_uses_runtime(conditional.path)
            for index, conditional in enumerate(conditional_edges)
        }
        self._concurrency_limits = {
            name: spec.max_concurrency
            for name, spec in nodes.items()
            if spec.max_concurrency is not None
        }
        self._active_runs: dict[str, CancellationToken] = {}
        self._event_sequences: dict[str, int] = {}
        self._run_budgets: dict[str, ExecutionBudget] = {}
        self.response_format: Mapping[str, Any] | type | None = None

    def _child_runtime(
        self,
        checkpointer: Checkpointer | None,
        store: Any | None,
        cache: BaseCache | None = None,
    ) -> CompiledStateGraph:
        """Rebind this graph to the parent run's checkpointer and store."""

        return CompiledStateGraph(
            state_schema=self.state_schema,
            input_schema=self.input_schema,
            output_schema=self.output_schema,
            context_schema=self.context_schema,
            graph_name=self.graph_name,
            graph_version=self.graph_version,
            channels=dict(self.channels),
            nodes=dict(self.nodes),
            edges=self._edges,
            conditional_edges=self._conditional_edges,
            checkpointer=checkpointer,
            store=store,
            cache=self.cache if cache is None else cache,
            serializer=self.serializer,
            step_timeout=self.step_timeout,
            interrupt_before=tuple(self.interrupt_before),
            interrupt_after=tuple(self.interrupt_after),
        )

    def with_runtime(
        self,
        *,
        checkpointer: Checkpointer | None = None,
        store: Any | None = None,
        cache: BaseCache | None = None,
    ) -> CompiledStateGraph:
        """Return an immutable graph rebound to deployment-managed services."""

        return self._child_runtime(
            checkpointer if checkpointer is not None else self.checkpointer,
            store if store is not None else self.store,
            cache if cache is not None else self.cache,
        )

    async def ainvoke(
        self,
        input: Mapping[str, Any] | Command[Any] | None,
        config: Mapping[str, Any] | None = None,
        *,
        context: Any | None = None,
        durability: Durability | str = Durability.SYNC,
        run_id: str | None = None,
        cancellation: CancellationToken | None = None,
        _is_subgraph: bool = False,
        _project: bool = True,
    ) -> dict[str, Any]:
        last: dict[str, Any] | None = None
        async for value in self.astream(
            input,
            config,
            stream_mode="values",
            context=context,
            durability=durability,
            run_id=run_id,
            cancellation=cancellation,
            _is_subgraph=_is_subgraph,
            _project=_project,
        ):
            last = value
        if last is not None:
            if "__interrupt__" in last:
                return last
            return self._validate_output(last)
        if self.checkpointer is not None and config is not None:
            return dict((await self.aget_state(config)).values)
        return {}

    def invoke(
        self,
        input: Mapping[str, Any] | Command[Any] | None,
        config: Mapping[str, Any] | None = None,
        *,
        context: Any | None = None,
        durability: Durability | str = Durability.SYNC,
        run_id: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> dict[str, Any]:
        self._reject_running_loop("invoke", "ainvoke")
        return asyncio.run(
            self.ainvoke(
                input,
                config,
                context=context,
                durability=durability,
                run_id=run_id,
                cancellation=cancellation,
            )
        )

    async def astream(
        self,
        input: Mapping[str, Any] | Command[Any] | None,
        config: Mapping[str, Any] | None = None,
        *,
        stream_mode: StreamMode = "values",
        context: Any | None = None,
        durability: Durability | str = Durability.SYNC,
        subgraphs: bool = False,
        run_id: str | None = None,
        cancellation: CancellationToken | None = None,
        _project: bool = True,
        _is_subgraph: bool = False,
    ) -> AsyncIterator[Any]:
        valid_modes = {"values", "updates", "events", "custom", "messages"}
        if not isinstance(stream_mode, str):
            modes = tuple(dict.fromkeys(stream_mode))
            if not modes or any(mode not in valid_modes for mode in modes):
                raise ValueError("stream_mode sequence contains an unsupported mode")
            current_state: dict[str, Any] = {}
            pending_updates: dict[str, Any] = {}
            async for event in self.astream(
                input,
                config,
                stream_mode="events",
                context=context,
                durability=durability,
                subgraphs=subgraphs,
                run_id=run_id,
                cancellation=cancellation,
                _project=False,
                _is_subgraph=_is_subgraph,
            ):
                if "events" in modes:
                    yield ("events", event)
                if event.kind in {EventKind.RUN_STARTED, EventKind.STATE_UPDATED, EventKind.RUN_COMPLETED}:
                    if isinstance(event.data.get("state"), Mapping):
                        current_state = copy.deepcopy(dict(event.data["state"]))
                if event.kind in {EventKind.NODE_COMPLETED, EventKind.NODE_CACHED}:
                    pending_updates[str(event.data.get("task_id") or event.task_id)] = copy.deepcopy(
                        event.data.get("update", {})
                    )
                if event.kind is EventKind.STATE_UPDATED:
                    if "updates" in modes:
                        yield ("updates", pending_updates)
                    pending_updates = {}
                    if "values" in modes:
                        value = current_state
                        if _project and self._output_keys is not None:
                            value = {key: item for key, item in value.items() if key in self._output_keys}
                        yield ("values", copy.deepcopy(value))
                elif event.kind is EventKind.INTERRUPT_RAISED:
                    markers = event.data.get("interrupts", ())
                    if "updates" in modes:
                        yield ("updates", {"__interrupt__": markers})
                    if "values" in modes:
                        value = {**copy.deepcopy(current_state), "__interrupt__": markers}
                        if _project and self._output_keys is not None:
                            value = {
                                key: item
                                for key, item in value.items()
                                if key in self._output_keys or key == "__interrupt__"
                            }
                        yield ("values", value)
                elif event.kind is EventKind.CUSTOM and "custom" in modes:
                    channel = event.data.get("channel", "custom")
                    value = event.data.get("value")
                    yield ("custom", value if channel == "custom" else {channel: value})
                elif event.kind is EventKind.MESSAGE and "messages" in modes:
                    yield ("messages", event.data.get("value"))
            return
        if stream_mode not in valid_modes:
            raise ValueError(
                "stream_mode must be 'values', 'updates', 'events', 'custom', or 'messages'"
            )
        selected_durability = Durability(durability)
        if self.context_schema is not None and isinstance(context, Mapping):
            context = SchemaAdapter(self.context_schema).validate(context)
        async for item in self._run(
            input,
            dict(config or {}),
            stream_mode,
            context=context,
            durability=selected_durability,
            run_id=run_id,
            cancellation=cancellation,
            allow_parent_command=_is_subgraph,
            subgraphs=subgraphs,
        ):
            if (
                _project
                and stream_mode == "values"
                and self._output_keys is not None
                and isinstance(item, Mapping)
            ):
                yield {
                    key: copy.deepcopy(value)
                    for key, value in item.items()
                    if key in self._output_keys or key == "__interrupt__"
                }
            else:
                yield item

    def stream(
        self,
        input: Mapping[str, Any] | Command[Any] | None,
        config: Mapping[str, Any] | None = None,
        *,
        stream_mode: StreamMode = "values",
        context: Any | None = None,
        durability: Durability | str = Durability.SYNC,
        subgraphs: bool = False,
        run_id: str | None = None,
        cancellation: CancellationToken | None = None,
    ) -> Iterator[Any]:
        self._reject_running_loop("stream", "astream")

        def iterate() -> Iterator[Any]:
            loop = asyncio.new_event_loop()
            iterator = self.astream(
                input,
                config,
                stream_mode=stream_mode,
                context=context,
                durability=durability,
                subgraphs=subgraphs,
                run_id=run_id,
                cancellation=cancellation,
            )
            try:
                while True:
                    try:
                        yield loop.run_until_complete(anext(iterator))
                    except StopAsyncIteration:
                        break
            finally:
                close = getattr(iterator, "aclose", None)
                if close is not None:
                    loop.run_until_complete(close())
                loop.close()

        return iterate()

    def get_state(
        self, config: Mapping[str, Any], *, subgraphs: bool = False
    ) -> StateSnapshot:
        del subgraphs
        if self.checkpointer is None:
            raise ValueError("get_state() requires a graph compiled with a checkpointer")
        item = self.checkpointer.get_tuple(config)
        if item is None:
            raise EmptyInputError("no checkpoint exists for the requested thread")
        return self._snapshot(item)

    def get_state_history(self, config: Mapping[str, Any]) -> Iterator[StateSnapshot]:
        if self.checkpointer is None:
            raise ValueError("get_state_history() requires a checkpointer")
        for item in self.checkpointer.list(config):
            yield self._snapshot(item)

    async def aget_state(
        self, config: Mapping[str, Any], *, subgraphs: bool = False
    ) -> StateSnapshot:
        del subgraphs
        if self.checkpointer is None:
            raise ValueError("aget_state() requires a graph compiled with a checkpointer")
        item = await self._aget_tuple(config)
        if item is None:
            raise EmptyInputError("no checkpoint exists for the requested thread")
        return self._snapshot(item)

    async def aget_state_history(
        self, config: Mapping[str, Any]
    ) -> AsyncIterator[StateSnapshot]:
        if self.checkpointer is None:
            raise ValueError("aget_state_history() requires a checkpointer")
        method = getattr(self.checkpointer, "alist", None)
        if method is not None:
            async for item in method(config):
                yield self._snapshot(item)
            return
        checkpointer = self.checkpointer
        items = await asyncio.to_thread(lambda: tuple(checkpointer.list(config)))
        for item in items:
            yield self._snapshot(item)

    @staticmethod
    def _snapshot(item: Any) -> StateSnapshot:
        checkpoint = item.checkpoint
        return StateSnapshot(
            values=copy.deepcopy(dict(checkpoint.channel_values)),
            next=checkpoint.next + tuple(send.node for send in checkpoint.pending_sends),
            config=copy.deepcopy(dict(item.config)),
            metadata=copy.deepcopy(dict(item.metadata)),
            created_at=checkpoint.ts,
            interrupts=checkpoint.pending_interrupts,
            tasks=checkpoint.tasks,
            parent_config=(
                {
                    **copy.deepcopy(dict(item.config)),
                    "configurable": {
                        **copy.deepcopy(dict(item.config.get("configurable", {}))),
                        "checkpoint_id": checkpoint.parent_id,
                    },
                }
                if checkpoint.parent_id is not None
                else None
            ),
        )

    def update_state(
        self,
        config: Mapping[str, Any],
        values: Mapping[str, Any],
        *,
        as_node: str | None = None,
    ) -> Mapping[str, Any]:
        if self.checkpointer is None:
            raise ValueError("update_state() requires a checkpointer")
        item = self.checkpointer.get_tuple(config)
        if item is None:
            raise EmptyInputError("no checkpoint exists for the requested thread")
        state = merge_updates(
            item.checkpoint.channel_values,
            [("__update_state__", values)],
            self.channels,
        )
        checkpoint = Checkpoint(
            id=str(uuid4()),
            ts=_utc_now(),
            step=item.checkpoint.step,
            channel_values=state,
            next=item.checkpoint.next,
            pending_sends=item.checkpoint.pending_sends,
            pending_interrupts=item.checkpoint.pending_interrupts,
            parent_id=item.checkpoint.id,
            namespace=item.checkpoint.namespace,
            run_id=item.checkpoint.run_id,
            channel_versions={
                **dict(item.checkpoint.channel_versions),
                **{
                    key: item.checkpoint.channel_versions.get(key, 0) + 1
                    for key in values
                },
            },
            tasks=item.checkpoint.tasks,
        )
        metadata = {
            **dict(item.metadata),
            "source": "update_state",
            "as_node": as_node,
        }
        return self.checkpointer.put(config, checkpoint, metadata)

    async def aupdate_state(
        self,
        config: Mapping[str, Any],
        values: Mapping[str, Any],
        *,
        as_node: str | None = None,
    ) -> Mapping[str, Any]:
        if self.checkpointer is None:
            raise ValueError("aupdate_state() requires a checkpointer")
        item = await self._aget_tuple(config)
        if item is None:
            raise EmptyInputError("no checkpoint exists for the requested thread")
        state = merge_updates(
            item.checkpoint.channel_values,
            [("__update_state__", values)],
            self.channels,
        )
        checkpoint = Checkpoint(
            id=str(uuid4()),
            ts=_utc_now(),
            step=item.checkpoint.step,
            channel_values=state,
            next=item.checkpoint.next,
            pending_sends=item.checkpoint.pending_sends,
            pending_interrupts=item.checkpoint.pending_interrupts,
            parent_id=item.checkpoint.id,
            namespace=item.checkpoint.namespace,
            run_id=item.checkpoint.run_id,
            channel_versions={
                **dict(item.checkpoint.channel_versions),
                **{key: item.checkpoint.channel_versions.get(key, 0) + 1 for key in values},
            },
            tasks=item.checkpoint.tasks,
        )
        return await self._aput(
            config,
            checkpoint,
            {**dict(item.metadata), "source": "update_state", "as_node": as_node},
        )

    def replay(
        self,
        config: Mapping[str, Any],
        *,
        context: Any | None = None,
    ) -> dict[str, Any]:
        """Replay execution after the checkpoint selected by ``config``."""

        return self.invoke(None, config, context=context)

    async def areplay(
        self, config: Mapping[str, Any], *, context: Any | None = None
    ) -> dict[str, Any]:
        return await self.ainvoke(None, config, context=context)

    def fork(
        self,
        config: Mapping[str, Any],
        values: Mapping[str, Any],
        *,
        as_node: str | None = None,
    ) -> Mapping[str, Any]:
        """Create a new checkpoint branch without modifying history."""

        return self.update_state(config, values, as_node=as_node)

    async def afork(
        self,
        config: Mapping[str, Any],
        values: Mapping[str, Any],
        *,
        as_node: str | None = None,
    ) -> Mapping[str, Any]:
        return await self.aupdate_state(config, values, as_node=as_node)

    def cancel(self, run_id: str) -> bool:
        token = self._active_runs.get(run_id)
        if token is None:
            return False
        token.cancel()
        return True

    def get_graph(self, *, xray: bool = False) -> GraphInfo:
        """Return a stable structural view suitable for tooling and diagrams.

        With ``xray=True`` every subgraph node is recursively expanded and its
        topology is attached to :attr:`NodeInfo.subgraph`, so tooling (and the
        Studio graph explorer) can explain the compiled multi-agent graph down
        to its nested nodes.
        """

        node_infos = [NodeInfo(START, kind="start")]
        for name, spec in self.nodes.items():
            is_subgraph = spec.subgraph is not None
            expanded: GraphInfo | None = None
            if is_subgraph and xray:
                inner = getattr(spec.subgraph, "get_graph", None)
                if callable(inner):
                    try:
                        expanded = inner(xray=True)
                    except TypeError:
                        expanded = inner()
            node_infos.append(
                NodeInfo(
                    name,
                    dict(spec.metadata or {}),
                    is_subgraph=is_subgraph,
                    kind="subgraph" if is_subgraph else "node",
                    debug=self._node_debug(name, spec),
                    subgraph=expanded,
                )
            )
        node_infos.append(NodeInfo(END, kind="end"))
        edges = [
            EdgeInfo(source, edge.target, label=edge.trigger if len(edge.sources) > 1 else None)
            for edge in self._edges
            for source in edge.sources
        ]
        for conditional in self._conditional_edges:
            if conditional.path_map is None:
                targets = (*self.nodes.keys(), END)
                edges.extend(
                    EdgeInfo(conditional.source, target, conditional=True)
                    for target in targets
                    if target != conditional.source
                )
            else:
                edges.extend(
                    EdgeInfo(
                        conditional.source,
                        target,
                        conditional=True,
                        label=str(route),
                    )
                    for route, target in conditional.path_map.items()
                )
        return GraphInfo(tuple(node_infos), tuple(edges))

    def _node_debug(self, name: str, spec: Any) -> dict[str, Any]:
        """Execution semantics surfaced for the Studio debugger."""

        retry = getattr(spec, "retry", None)
        debug: dict[str, Any] = {
            "callable": _describe_callable(getattr(spec, "action", None)),
            "uses_runtime": bool(self._node_runtime.get(name, False)),
        }
        if getattr(spec, "timeout", None) is not None:
            debug["timeout"] = spec.timeout
        if getattr(spec, "max_concurrency", None) is not None:
            debug["max_concurrency"] = spec.max_concurrency
        if getattr(spec, "defer", False):
            debug["defer"] = True
        if getattr(spec, "cache", None) is not None:
            debug["cache"] = True
        if getattr(spec, "middleware", ()):
            debug["middleware"] = len(spec.middleware)
        if retry is not None:
            debug["retry"] = {
                "max_attempts": getattr(retry, "max_attempts", None),
                "backoff": getattr(retry, "backoff_factor", None),
            }
        if getattr(spec, "subgraph", None) is not None:
            debug["callable"] = getattr(spec.subgraph, "graph_name", debug.get("callable"))
            debug["subgraph_persistence"] = getattr(
                getattr(spec, "subgraph_persistence", None), "value", None
            )
        return debug

    def draw_mermaid(self, *, xray: bool = False) -> str:
        return self.get_graph(xray=xray).draw_mermaid(xray=xray)

    async def _run(
        self,
        input: Mapping[str, Any] | Command[Any] | None,
        config: dict[str, Any],
        stream_mode: str,
        *,
        context: Any | None,
        durability: Durability,
        run_id: str | None,
        cancellation: CancellationToken | None,
        allow_parent_command: bool,
        subgraphs: bool,
    ) -> AsyncIterator[Any]:
        run_id = run_id or str(uuid4())
        cancellation = cancellation or CancellationToken()
        self._active_runs[run_id] = cancellation
        self._event_sequences[run_id] = 0
        try:
            parent_budget = get_runtime().budget
        except RuntimeError:
            parent_budget = None
        limits = {**dict(config.get("configurable", {})), **config}
        self._run_budgets[run_id] = parent_budget or ExecutionBudget(
            max_tool_calls=(
                int(limits["max_tool_calls"]) if limits.get("max_tool_calls") is not None else None
            ),
            max_model_calls=(
                int(limits["max_model_calls"])
                if limits.get("max_model_calls") is not None
                else None
            ),
            max_tokens=(int(limits["max_tokens"]) if limits.get("max_tokens") is not None else None),
            max_cost=(float(limits["max_cost"]) if limits.get("max_cost") is not None else None),
        )
        node_semaphores = {
            name: asyncio.Semaphore(limit)
            for name, limit in self._concurrency_limits.items()
        }
        checkpoint_writer = (
            _AsyncCheckpointWriter() if durability is Durability.ASYNC else None
        )
        configurable = config.get("configurable", {})
        namespace_value = configurable.get("checkpoint_ns", "")
        namespace = tuple(part for part in str(namespace_value).split("|") if part)
        thread_id = self._thread_id(config)
        run_timeout = config.get("run_timeout")
        if run_timeout is not None and (not isinstance(run_timeout, (int, float)) or run_timeout <= 0):
            raise ValueError("run_timeout must be a positive number")
        deadline = (
            datetime.now(UTC) + timedelta(seconds=float(run_timeout))
            if run_timeout is not None
            else None
        )
        recursion_limit = config.get("recursion_limit", 25)
        if not isinstance(recursion_limit, int) or recursion_limit < 1:
            raise ValueError("recursion_limit must be a positive integer")

        latest = None
        if self.checkpointer is not None:
            self._require_thread_id(config)
            latest = await self._aget_tuple(config)

        state: dict[str, Any]
        active: tuple[str, ...]
        sends: tuple[Send, ...]
        step: int
        join_progress: dict[int, set[str]] = {}
        pending_deferred: set[str] = set()
        resume: dict[str, list[Any]] = {}
        skip_before: set[str] = set()
        pending_interrupts: tuple[Interrupt, ...] = ()
        needs_entry = False
        channel_versions: dict[str, int] = {}
        parent_checkpoint_id: str | None = None

        if latest is not None:
            checkpoint = latest.checkpoint
            state = copy.deepcopy(dict(checkpoint.channel_values))
            active = checkpoint.next
            sends = checkpoint.pending_sends
            step = checkpoint.step + 1
            pending_interrupts = checkpoint.pending_interrupts
            channel_versions = dict(checkpoint.channel_versions)
            parent_checkpoint_id = checkpoint.id
            resume = {
                str(task_id): list(values)
                for task_id, values in dict(latest.metadata.get("resume", {})).items()
            }
            join_progress = {
                int(index): set(nodes)
                for index, nodes in latest.metadata.get("join_progress", {}).items()
            }
            pending_deferred = set(latest.metadata.get("deferred", ()))
            marker = latest.metadata.get("static_interrupt")
            if isinstance(marker, Mapping) and marker.get("position") == "before":
                skip_before = set(marker.get("nodes", ()))
        else:
            state = {}
            active = ()
            sends = ()
            step = 0
            needs_entry = True

        is_resume_command = isinstance(input, Command) and pending_interrupts != ()
        if isinstance(input, Command):
            if latest is None:
                raise EmptyInputError("Command requires an existing checkpoint")
            if input.update:
                state = merge_updates(
                    state,
                    [("__input__", self._state_adapter.validate_partial(input.update))],
                    self.channels,
                )
            if input.goto is not None:
                names, new_sends = self._split_targets(self._as_targets(input.goto))
                active = self._normalize_targets(names)
                sends = new_sends
            if pending_interrupts:
                self._deliver_resume(input.resume, pending_interrupts, resume)
                pending_interrupts = ()
        elif input is None:
            if latest is None:
                raise EmptyInputError("input cannot be None without an existing checkpoint")
        elif isinstance(input, Mapping):
            validated_input = SchemaAdapter(self.input_schema).validate(input)
            state = merge_updates(state, [("__input__", validated_input)], self.channels)
            if latest is None or (not active and not sends):
                needs_entry = True
                step = 0
        else:
            raise InvalidUpdateError("graph input must be a mapping, Command, or None")

        if needs_entry:
            route_runtime = self._make_runtime(
                context=context,
                config=config,
                run_id=run_id,
                task_id=START,
                checkpoint_id=parent_checkpoint_id,
                namespace=namespace,
                cancellation=cancellation,
                deadline=deadline,
            )
            active, sends = await self._entry_tasks(state, config, route_runtime)

        if self.checkpointer is not None:
            self.serializer.dumps(state)

        try:
            state_digest = hashlib.sha256(self.serializer.dumps(state)).hexdigest()[:20]
        except Exception:
            state_digest = hashlib.sha256(repr(state).encode("utf-8")).hexdigest()[:20]
        base_checkpoint_id = parent_checkpoint_id or f"input:{self.schema_hash[:12]}:{state_digest}"

        if stream_mode == "events":
            yield self._event(
                EventKind.RUN_STARTED,
                run_id,
                step=step,
                namespace=namespace,
                thread_id=thread_id,
                data={"state": copy.deepcopy(state)},
            )

        if latest is not None and latest.checkpoint.pending_interrupts and not is_resume_command:
            markers = latest.checkpoint.pending_interrupts
            if stream_mode == "values":
                yield self._interrupt_output(state, markers)
            elif stream_mode == "updates":
                yield {"__interrupt__": markers}
            elif stream_mode == "events":
                yield self._event(
                    EventKind.INTERRUPT_RAISED,
                    run_id,
                    step=step,
                    namespace=namespace,
                    thread_id=thread_id,
                    data={"interrupts": markers},
                )
            self._active_runs.pop(run_id, None)
            self._event_sequences.pop(run_id, None)
            return

        if not active and not sends:
            if stream_mode == "values":
                yield copy.deepcopy(state)
            elif stream_mode == "events":
                yield self._event(
                    EventKind.RUN_COMPLETED,
                    run_id,
                    step=step,
                    namespace=namespace,
                    thread_id=thread_id,
                    data={
                        "state": self._validate_output(state),
                        "budget": self._run_budgets[run_id].snapshot(),
                    },
                )
            self._active_runs.pop(run_id, None)
            self._event_sequences.pop(run_id, None)
            return

        executed_steps = 0
        try:
            while active or sends:
                cancellation.raise_if_cancelled()
                if deadline is not None and datetime.now(UTC) >= deadline:
                    raise GraphTimeoutError("graph run deadline exceeded")
                if executed_steps >= recursion_limit:
                    raise GraphRecursionError(
                        f"graph exceeded recursion_limit={recursion_limit} after "
                        f"{executed_steps} supersteps"
                    )

                deferred_now = {
                    node for node in active if self.nodes[node].defer
                }
                non_deferred = tuple(node for node in active if node not in deferred_now)
                if deferred_now and (non_deferred or sends):
                    pending_deferred.update(deferred_now)
                    active = non_deferred
                tasks = self._plan_tasks(active, sends, namespace)
                task_nodes = {task.node for task in tasks}

                if stream_mode == "events":
                    yield self._event(
                        EventKind.STEP_STARTED,
                        run_id,
                        step=step,
                        namespace=namespace,
                        checkpoint_id=base_checkpoint_id,
                        thread_id=thread_id,
                        data={"tasks": tuple(task.id for task in tasks)},
                    )

                before_nodes = task_nodes & self.interrupt_before - skip_before
                skip_before.clear()
                if before_nodes:
                    self._require_interrupt_support(config)
                    ordered_before = self._normalize_targets(tuple(before_nodes))
                    marker = Interrupt(
                        value={"nodes": ordered_before},
                        id=f"static-before-{step}",
                        when="before",
                    )
                    saved_id = await self._save_checkpoint(
                        config,
                        state,
                        active,
                        sends,
                        step - 1,
                        (),
                        {
                            "source": "interrupt_before",
                            "static_interrupt": {"position": "before", "nodes": ordered_before},
                            "join_progress": self._serialize_join_progress(join_progress),
                            "resume": {key: list(value) for key, value in resume.items()},
                            "deferred": sorted(pending_deferred),
                        },
                        parent_id=parent_checkpoint_id,
                        namespace=namespace,
                        run_id=run_id,
                        channel_versions=channel_versions,
                        writer=checkpoint_writer,
                    )
                    parent_checkpoint_id = saved_id or parent_checkpoint_id
                    if stream_mode == "values":
                        yield copy.deepcopy(state)
                    elif stream_mode == "updates":
                        yield {}
                    elif stream_mode == "events":
                        yield self._event(
                            EventKind.CHECKPOINT_SAVED,
                            run_id,
                            step=step,
                            namespace=namespace,
                            checkpoint_id=saved_id,
                            thread_id=thread_id,
                        )
                        yield self._event(
                            EventKind.RUN_PAUSED,
                            run_id,
                            step=step,
                            namespace=namespace,
                            checkpoint_id=saved_id,
                            thread_id=thread_id,
                            data={"reason": "interrupt_before"},
                        )
                        yield self._event(
                            EventKind.INTERRUPT_RAISED,
                            run_id,
                            step=step,
                            namespace=namespace,
                            thread_id=thread_id,
                            data={"interrupts": (marker,)},
                        )
                    return

                if stream_mode == "events":
                    for task in tasks:
                        yield self._event(
                            EventKind.NODE_STARTED,
                            run_id,
                            step=step,
                            node=task.node,
                            namespace=namespace,
                            task_id=task.id,
                            checkpoint_id=base_checkpoint_id,
                            thread_id=thread_id,
                            data={"task_id": task.id},
                        )

                snapshot = copy.deepcopy(state)
                event_queue: asyncio.Queue[tuple[str, Any, str]] = asyncio.Queue()
                loop = asyncio.get_running_loop()

                def emit(
                    channel: str,
                    value: Any,
                    task_id: str = "",
                    _loop: asyncio.AbstractEventLoop = loop,
                    _queue: asyncio.Queue[tuple[str, Any, str]] = event_queue,
                ) -> None:
                    if _loop.is_closed():
                        return
                    try:
                        _loop.call_soon_threadsafe(
                            _queue.put_nowait,
                            (channel, copy.deepcopy(value), task_id),
                        )
                    except RuntimeError:
                        if not _loop.is_closed():
                            raise

                persisted = {
                    write.task_id: write
                    for write in await self._get_pending_writes(config, base_checkpoint_id)
                    if write.error is None
                }
                futures: list[asyncio.Task[Any]] = []
                pending_tasks: list[_Task] = []
                outcomes_by_id: dict[str, _TaskResult | BaseException] = {}
                for task in tasks:
                    write = persisted.get(task.id)
                    if write is not None:
                        outcomes_by_id[task.id] = _TaskResult(
                            task=task,
                            update=write.values,
                            goto=write.goto,
                            cached=True,
                        )
                        continue
                    pending_tasks.append(task)
                    futures.append(
                        asyncio.create_task(
                            self._run_task(
                                task,
                                snapshot,
                                resume,
                                config,
                                step,
                                context=context,
                                run_id=run_id,
                                checkpoint_id=base_checkpoint_id,
                                namespace=namespace,
                                cancellation=cancellation,
                                deadline=deadline,
                                emit=partial(emit, task_id=task.id),
                                node_semaphores=node_semaphores,
                                remaining_steps=recursion_limit - executed_steps,
                                stream_mode=stream_mode,
                                stream_subgraphs=subgraphs,
                            )
                        )
                    )
                gather = asyncio.gather(*futures, return_exceptions=True)
                next_event: asyncio.Task[tuple[str, Any, str]] | None = None
                try:
                    async with asyncio.timeout(self.step_timeout):
                        while not gather.done():
                            next_event = asyncio.create_task(event_queue.get())
                            waitables: set[asyncio.Future[Any]] = {gather, next_event}
                            done, _ = await asyncio.wait(
                                waitables,
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                            if next_event in done:
                                channel, value, task_id_value = next_event.result()
                                if channel == "__subgraph__" and subgraphs:
                                    yield value
                                elif stream_mode == "custom" and channel not in {
                                    "__retry__",
                                    "messages",
                                }:
                                    yield value if channel == "custom" else {channel: value}
                                elif stream_mode == "messages" and channel == "messages":
                                    yield value
                                elif stream_mode == "events":
                                    yield self._event(
                                        EventKind.NODE_RETRYING
                                        if channel == "__retry__"
                                        else EventKind.MESSAGE
                                        if channel == "messages"
                                        else EventKind.CUSTOM,
                                        run_id,
                                        step=step,
                                        namespace=namespace,
                                        task_id=task_id_value,
                                        checkpoint_id=base_checkpoint_id,
                                        thread_id=thread_id,
                                        data={"channel": channel, "value": value},
                                    )
                            else:
                                next_event.cancel()
                        completed_outcomes = await gather
                        while not event_queue.empty():
                            channel, value, task_id_value = event_queue.get_nowait()
                            if channel == "__subgraph__" and subgraphs:
                                yield value
                            elif stream_mode == "custom" and channel not in {
                                "__retry__",
                                "messages",
                            }:
                                yield value if channel == "custom" else {channel: value}
                            elif stream_mode == "messages" and channel == "messages":
                                yield value
                            elif stream_mode == "events":
                                yield self._event(
                                    EventKind.NODE_RETRYING
                                    if channel == "__retry__"
                                    else EventKind.MESSAGE
                                    if channel == "messages"
                                    else EventKind.CUSTOM,
                                    run_id,
                                    step=step,
                                    namespace=namespace,
                                    task_id=task_id_value,
                                    checkpoint_id=base_checkpoint_id,
                                    thread_id=thread_id,
                                    data={"channel": channel, "value": value},
                                )
                except TimeoutError as exc:
                    cancellation.cancel()
                    for future in futures:
                        future.cancel()
                    await asyncio.gather(*futures, return_exceptions=True)
                    raise GraphTimeoutError(
                        f"superstep {step} exceeded timeout={self.step_timeout}"
                    ) from exc
                except BaseException:
                    cancellation.cancel()
                    if next_event is not None and not next_event.done():
                        next_event.cancel()
                    for future in futures:
                        future.cancel()
                    await asyncio.gather(*futures, return_exceptions=True)
                    raise
                for task, outcome in zip(pending_tasks, completed_outcomes, strict=True):
                    outcomes_by_id[task.id] = outcome
                outcomes = [outcomes_by_id[task.id] for task in tasks]

                successful = [
                    item
                    for item in outcomes
                    if isinstance(item, _TaskResult) and item.interrupt is None
                ]
                await self._put_pending_results(
                    config, base_checkpoint_id, tasks, successful
                )
                failure = next(
                    (item for item in outcomes if isinstance(item, BaseException)), None
                )
                if failure is not None:
                    raise failure
                results: list[_TaskResult] = list(outcomes)  # type: ignore[arg-type]

                parent_commands = [
                    result.parent_command
                    for result in results
                    if result.parent_command is not None
                ]
                if parent_commands:
                    if len(parent_commands) != 1:
                        raise InvalidUpdateError(
                            "a superstep may return at most one Command(scope=PARENT)"
                        )
                    if not allow_parent_command:
                        raise InvalidUpdateError(
                            "Command(scope=PARENT) is only valid inside a subgraph node"
                        )
                    raise _ParentCommand(parent_commands[0])

                interrupts = tuple(
                    result.interrupt for result in results if result.interrupt is not None
                )
                if interrupts:
                    self._require_interrupt_support(config)
                    saved_id = await self._save_checkpoint(
                        config,
                        state,
                        active,
                        sends,
                        step - 1,
                        interrupts,
                        {
                            "source": "interrupt",
                            "resume": {key: list(value) for key, value in resume.items()},
                            "join_progress": self._serialize_join_progress(join_progress),
                            "deferred": sorted(pending_deferred),
                        },
                        parent_id=parent_checkpoint_id,
                        namespace=namespace,
                        run_id=run_id,
                        channel_versions=channel_versions,
                        tasks=tuple(self._task_snapshot(result) for result in results),
                        writer=checkpoint_writer,
                    )
                    if saved_id is not None:
                        if checkpoint_writer is not None:
                            await checkpoint_writer.flush()
                        await self._put_pending_results(config, saved_id, tasks, successful)
                        parent_checkpoint_id = saved_id
                    if stream_mode == "values":
                        yield self._interrupt_output(state, interrupts)
                    elif stream_mode == "updates":
                        yield {"__interrupt__": interrupts}
                    elif stream_mode == "events":
                        yield self._event(
                            EventKind.CHECKPOINT_SAVED,
                            run_id,
                            step=step,
                            namespace=namespace,
                            checkpoint_id=saved_id,
                            thread_id=thread_id,
                        )
                        yield self._event(
                            EventKind.RUN_PAUSED,
                            run_id,
                            step=step,
                            namespace=namespace,
                            checkpoint_id=saved_id,
                            thread_id=thread_id,
                            data={"reason": "interrupt"},
                        )
                        yield self._event(
                            EventKind.INTERRUPT_RAISED,
                            run_id,
                            step=step,
                            node=interrupts[0].task_id,
                            namespace=namespace,
                            checkpoint_id=saved_id,
                            thread_id=thread_id,
                            data={"interrupts": interrupts},
                        )
                    return

                updates = [(result.task.id, result.update) for result in results]
                state = merge_updates(state, updates, self.channels)
                for result in results:
                    for key in result.update:
                        channel_versions[key] = channel_versions.get(key, 0) + 1
                if stream_mode == "events":
                    for result in results:
                        yield self._event(
                            EventKind.NODE_CACHED if result.cached else EventKind.NODE_COMPLETED,
                            run_id,
                            step=step,
                            node=result.task.node,
                            namespace=namespace,
                            task_id=result.task.id,
                            checkpoint_id=base_checkpoint_id,
                            thread_id=thread_id,
                            data={
                                "task_id": result.task.id,
                                "update": self._display_update(result.update),
                            },
                        )
                    yield self._event(
                        EventKind.STATE_UPDATED,
                        run_id,
                        step=step,
                        namespace=namespace,
                        checkpoint_id=base_checkpoint_id,
                        thread_id=thread_id,
                        data={"state": copy.deepcopy(state)},
                    )

                completed = {task.node for task in tasks}
                next_names: list[str] = []
                next_sends: list[Send] = []

                for index, edge in enumerate(self._edges):
                    if edge.sources == (START,):
                        continue
                    if len(edge.sources) == 1:
                        if edge.sources[0] in completed:
                            next_names.append(edge.target)
                    else:
                        progress = join_progress.setdefault(index, set())
                        progress.update(completed.intersection(edge.sources))
                        ready = (
                            bool(progress)
                            if edge.trigger == "any"
                            else set(edge.sources).issubset(progress)
                        )
                        if ready:
                            next_names.append(edge.target)
                            progress.clear()

                for index, conditional in enumerate(self._conditional_edges):
                    if conditional.source == START or conditional.source not in completed:
                        continue
                    route_runtime = self._make_runtime(
                        context=context,
                        config=config,
                        run_id=run_id,
                        task_id=f"route:{conditional.source}",
                        checkpoint_id=base_checkpoint_id,
                        namespace=namespace,
                        cancellation=cancellation,
                        deadline=deadline,
                    )
                    route = await self._call(
                        conditional.path,
                        MappingProxyType(copy.deepcopy(state)),
                        config,
                        self._path_arity[index],
                        runtime=route_runtime,
                        uses_runtime=self._path_runtime[index],
                    )
                    route_names, route_sends = self._resolve_route(conditional, route)
                    next_names.extend(route_names)
                    next_sends.extend(route_sends)

                for result in results:
                    names, goto_sends = self._split_targets(result.goto)
                    next_names.extend(names)
                    next_sends.extend(goto_sends)

                active = self._normalize_targets(tuple(next_names))
                sends = tuple(next_sends)
                if pending_deferred and not active and not sends:
                    active = self._normalize_targets(tuple(pending_deferred))
                    pending_deferred.clear()
                resume = {}

                if durability is not Durability.EXIT:
                    saved_id = await self._save_checkpoint(
                        config,
                        state,
                        active,
                        sends,
                        step,
                        (),
                        {
                            "source": "loop",
                            "step": step,
                            "join_progress": self._serialize_join_progress(join_progress),
                            "resume": {},
                            "deferred": sorted(pending_deferred),
                            "durability": durability.value,
                        },
                        parent_id=parent_checkpoint_id,
                        namespace=namespace,
                        run_id=run_id,
                        channel_versions=channel_versions,
                        tasks=tuple(self._task_snapshot(result) for result in results),
                        writer=checkpoint_writer,
                    )
                    parent_checkpoint_id = saved_id or parent_checkpoint_id
                    base_checkpoint_id = saved_id or base_checkpoint_id
                    if stream_mode == "events" and saved_id is not None:
                        yield self._event(
                            EventKind.CHECKPOINT_SAVED,
                            run_id,
                            step=step,
                            namespace=namespace,
                            checkpoint_id=saved_id,
                            thread_id=thread_id,
                        )

                if stream_mode == "values":
                    yield copy.deepcopy(state)
                elif stream_mode == "updates":
                    yield {
                        result.task.id: self._display_update(result.update)
                        for result in results
                    }

                if stream_mode == "events":
                    yield self._event(
                        EventKind.STEP_COMPLETED,
                        run_id,
                        step=step,
                        namespace=namespace,
                        checkpoint_id=parent_checkpoint_id,
                        thread_id=thread_id,
                    )

                if completed & self.interrupt_after:
                    self._require_interrupt_support(config)
                    if stream_mode == "events":
                        marker = Interrupt(
                            value={
                                "nodes": self._normalize_targets(
                                    tuple(completed & self.interrupt_after)
                                )
                            },
                            id=f"static-after-{step}",
                            when="after",
                        )
                        yield self._event(
                            EventKind.RUN_PAUSED,
                            run_id,
                            step=step,
                            namespace=namespace,
                            checkpoint_id=parent_checkpoint_id,
                            thread_id=thread_id,
                            data={"reason": "interrupt_after"},
                        )
                        yield self._event(
                            EventKind.INTERRUPT_RAISED,
                            run_id,
                            step=step,
                            namespace=namespace,
                            checkpoint_id=parent_checkpoint_id,
                            thread_id=thread_id,
                            data={"interrupts": (marker,)},
                        )
                    return

                step += 1
                executed_steps += 1

            if durability is Durability.EXIT and self.checkpointer is not None:
                parent_checkpoint_id = await self._save_checkpoint(
                    config,
                    state,
                    (),
                    (),
                    max(step - 1, 0),
                    (),
                    {"source": "exit", "durability": durability.value},
                    parent_id=parent_checkpoint_id,
                    namespace=namespace,
                    run_id=run_id,
                    channel_versions=channel_versions,
                    writer=checkpoint_writer,
                )
                if stream_mode == "events":
                    yield self._event(
                        EventKind.CHECKPOINT_SAVED,
                        run_id,
                        step=step,
                        namespace=namespace,
                        checkpoint_id=parent_checkpoint_id,
                        thread_id=thread_id,
                    )
            if stream_mode == "events":
                yield self._event(
                    EventKind.RUN_COMPLETED,
                    run_id,
                    step=step,
                    namespace=namespace,
                    checkpoint_id=parent_checkpoint_id,
                    thread_id=thread_id,
                    data={
                        "state": self._validate_output(state),
                        "budget": self._run_budgets[run_id].snapshot(),
                    },
                )
        except GraphCancelledError:
            if stream_mode == "events":
                yield self._event(
                    EventKind.RUN_CANCELLED,
                    run_id,
                    step=step,
                    namespace=namespace,
                    thread_id=thread_id,
                )
            raise
        except GraphTimeoutError:
            if stream_mode == "events":
                yield self._event(
                    EventKind.RUN_TIMED_OUT,
                    run_id,
                    step=step,
                    namespace=namespace,
                    thread_id=thread_id,
                )
            raise
        except BudgetExceededError as exc:
            if stream_mode == "events":
                yield self._event(
                    EventKind.RUN_BUDGET_EXCEEDED,
                    run_id,
                    step=step,
                    namespace=namespace,
                    thread_id=thread_id,
                    data={"error": str(exc)},
                )
            raise
        except Exception as exc:
            if stream_mode == "events":
                yield self._event(
                    EventKind.RUN_FAILED,
                    run_id,
                    step=step,
                    namespace=namespace,
                    thread_id=thread_id,
                    data={"error": repr(exc)},
                )
            raise
        finally:
            if checkpoint_writer is not None:
                await checkpoint_writer.flush()
            self._active_runs.pop(run_id, None)
            self._event_sequences.pop(run_id, None)
            self._run_budgets.pop(run_id, None)

    def _plan_tasks(
        self,
        active: tuple[str, ...],
        sends: tuple[Send, ...],
        namespace: tuple[str, ...] = (),
    ) -> list[_Task]:
        tasks = [_Task(id=node, node=node, path=(*namespace, node)) for node in active]
        for index, send in enumerate(sends):
            if send.node not in self.nodes:
                raise GraphValidationError(f"Send targets unknown node {send.node!r}")
            task_id = f"{send.node}#{index}"
            tasks.append(
                _Task(id=task_id, node=send.node, send=send, path=(*namespace, task_id))
            )
        return tasks

    async def _entry_tasks(
        self,
        state: Mapping[str, Any],
        config: Mapping[str, Any],
        runtime: Runtime[Any],
    ) -> tuple[tuple[str, ...], tuple[Send, ...]]:
        names = [edge.target for edge in self._edges if edge.sources == (START,)]
        sends: list[Send] = []
        for index, conditional in enumerate(self._conditional_edges):
            if conditional.source != START:
                continue
            route = await self._call(
                conditional.path,
                MappingProxyType(copy.deepcopy(dict(state))),
                config,
                self._path_arity[index],
                runtime=runtime,
                uses_runtime=self._path_runtime[index],
            )
            route_names, route_sends = self._resolve_route(conditional, route)
            names.extend(route_names)
            sends.extend(route_sends)
        return self._normalize_targets(tuple(names)), tuple(sends)

    def _resolve_route(
        self, conditional: _ConditionalEdge, route: Any
    ) -> tuple[list[str], list[Send]]:
        names: list[str] = []
        sends: list[Send] = []
        for selected in self._as_targets(route):
            if isinstance(selected, Send):
                sends.append(selected)
                continue
            if conditional.path_map is not None:
                if selected not in conditional.path_map:
                    raise GraphValidationError(
                        f"conditional route from {conditional.source!r} returned "
                        f"unmapped path {selected!r}"
                    )
                names.append(conditional.path_map[selected])
            else:
                names.append(selected)
        return names, sends

    @staticmethod
    def _deliver_resume(
        value: Any,
        pending: tuple[Interrupt, ...],
        resume: dict[str, list[Any]],
    ) -> None:
        """Route a resume value to the task(s) whose interrupts it answers."""

        pending_ids = {marker.id: marker for marker in pending if marker.id is not None}
        if (
            isinstance(value, Mapping)
            and value
            and all(isinstance(key, str) and key in pending_ids for key in value)
        ):
            for interrupt_id, answer in value.items():
                marker = pending_ids[interrupt_id]
                if marker.task_id is not None:
                    resume.setdefault(marker.task_id, []).append(answer)
            return
        target = pending[0].task_id
        if target is not None:
            resume.setdefault(target, []).append(value)

    async def _run_task(
        self,
        task: _Task,
        state: Mapping[str, Any],
        resume: Mapping[str, list[Any]],
        config: Mapping[str, Any],
        step: int,
        *,
        context: Any | None,
        run_id: str,
        checkpoint_id: str,
        namespace: tuple[str, ...],
        cancellation: CancellationToken,
        deadline: datetime | None,
        emit: Callable[[str, Any], None],
        node_semaphores: Mapping[str, asyncio.Semaphore],
        remaining_steps: int,
        stream_mode: str,
        stream_subgraphs: bool,
    ) -> _TaskResult:
        spec = self.nodes[task.node]
        interrupt_context = _InterruptContext(
            resumable=self.checkpointer is not None and self._has_thread_id(config),
            resume_values=tuple(resume.get(task.id, ())),
            task_id=task.id,
            namespace=namespace,
            task_path=task.path,
        )
        node_runtime = self._make_runtime(
            context=context,
            config=config,
            run_id=run_id,
            task_id=task.id,
            checkpoint_id=checkpoint_id,
            namespace=namespace,
            cancellation=cancellation,
            deadline=deadline,
            emit=emit,
            metadata=spec.metadata,
            task_path=task.path,
            remaining_steps=remaining_steps,
            stream_mode=stream_mode,
            stream_subgraphs=stream_subgraphs,
        )
        runtime_context = _RuntimeContext(
            config=MappingProxyType(dict(config)),
            store=self.store,
            runtime=node_runtime,
        )
        token = _set_interrupt_context(interrupt_context)
        runtime_token = _set_runtime_context(runtime_context)
        try:
            try:
                semaphore = node_semaphores.get(task.node)
                if semaphore is not None:
                    async with semaphore:
                        result = await self._execute_task(
                            task,
                            spec,
                            state,
                            interrupt_context,
                            config,
                            step,
                            node_runtime,
                        )
                else:
                    result = await self._execute_task(
                        task,
                        spec,
                        state,
                        interrupt_context,
                        config,
                        step,
                        node_runtime,
                    )
            except GraphInterrupt as signal:
                return _TaskResult(task, {}, (), signal.interrupt)  # type: ignore[arg-type]
        finally:
            _reset_runtime_context(runtime_token)
            _reset_interrupt_context(token)

        if result is None:
            return _TaskResult(task, {}, ())
        if isinstance(result, _CachedResult):
            return _TaskResult(
                task, self._state_adapter.validate_partial(result.value), (), cached=True
            )
        if isinstance(result, Command):
            if result.scope is CommandScope.PARENT:
                return _TaskResult(task, {}, (), parent_command=result)
            update = self._state_adapter.validate_partial(result.update or {})
            goto = self._as_targets(result.goto) if result.goto is not None else ()
            return _TaskResult(task, update, goto)
        if isinstance(result, Mapping):
            return _TaskResult(task, self._state_adapter.validate_partial(result), ())
        raise InvalidUpdateError(
            f"node {task.node!r} returned {type(result).__name__}; expected dict or Command"
        )

    async def _execute_task(
        self,
        task: _Task,
        spec: _NodeSpec,
        state: Mapping[str, Any],
        interrupt_context: _InterruptContext,
        config: Mapping[str, Any],
        step: int,
        runtime: Runtime[Any],
    ) -> Any:
        with start_span(
            "lingxigraph.task",
            {
                "lingxigraph.run.id": runtime.run_id,
                "lingxigraph.task.id": task.id,
                "lingxigraph.node.name": task.node,
                "lingxigraph.step": step,
            },
        ):
            runtime.raise_if_cancelled()
            for middleware in spec.middleware:
                hook = getattr(middleware, "before_node", None)
                if hook is not None:
                    value = hook(task.node, state, runtime)
                    if inspect.isawaitable(value):
                        await value
            if spec.subgraph is not None:
                result = await self._run_subgraph(
                    task, spec, state, interrupt_context, config, step, runtime
                )
            else:
                result = await self._attempt(
                    task, spec, state, interrupt_context, config, runtime
                )
            for middleware in reversed(spec.middleware):
                hook = getattr(middleware, "after_node", None)
                if hook is not None:
                    value = hook(task.node, result, runtime)
                    if inspect.isawaitable(value):
                        await value
            return result

    def _validate_output(self, state: Mapping[str, Any]) -> dict[str, Any]:
        value = dict(state)
        if self._output_keys is not None:
            value = {key: item for key, item in value.items() if key in self._output_keys}
        return self._output_adapter.validate(value)

    async def _attempt(
        self,
        task: _Task,
        spec: _NodeSpec,
        state: Mapping[str, Any],
        context: _InterruptContext,
        config: Mapping[str, Any],
        runtime: Runtime[Any],
    ) -> Any:
        if task.send is not None:
            argument = task.send.arg
        else:
            argument = MappingProxyType(copy.deepcopy(dict(state)))
        cache_key = self._cache_key(task, spec, argument)
        if cache_key is not None:
            cached = await self._cache_get(cache_key)
            if cached is not None:
                return _CachedResult(cached)
        policy = spec.retry
        attempts = policy.max_attempts if policy is not None else 1
        interval = policy.initial_interval if policy is not None else 0.0
        for attempt in range(1, attempts + 1):
            context.call_index = 0
            try:
                runtime.raise_if_cancelled()
                call = self._call(
                    spec.action,
                    argument,
                    config,
                    self._node_arity[task.node],
                    runtime=runtime,
                    uses_runtime=self._node_runtime[task.node],
                )
                result = (
                    await asyncio.wait_for(call, timeout=spec.timeout)
                    if spec.timeout is not None
                    else await call
                )
                if cache_key is not None and isinstance(result, Mapping):
                    await self._cache_set(cache_key, result, spec.cache.ttl if spec.cache else None)
                return result
            except TimeoutError as exc:
                raise GraphTimeoutError(
                    f"node {task.node!r} exceeded timeout={spec.timeout}"
                ) from exc
            except Exception as exc:
                if (
                    policy is None
                    or attempt >= attempts
                    or not isinstance(exc, policy.retry_on)
                ):
                    raise
                delay = min(interval, policy.max_interval)
                if policy.jitter and delay > 0:
                    delay += random.uniform(0, delay / 2)
                runtime.emit(
                    "__retry__",
                    {
                        "node": task.node,
                        "attempt": attempt,
                        "next_attempt": attempt + 1,
                        "delay": delay,
                        "error": repr(exc),
                    },
                )
                await asyncio.sleep(delay)
                interval *= policy.backoff_factor
        raise AssertionError("unreachable retry state")

    async def _run_subgraph(
        self,
        task: _Task,
        spec: _NodeSpec,
        state: Mapping[str, Any],
        context: _InterruptContext,
        config: Mapping[str, Any],
        step: int,
        runtime: Runtime[Any],
    ) -> Any:
        child = spec.subgraph
        assert isinstance(child, CompiledStateGraph)
        shared = [key for key in child.channels if key in self.channels]
        subinput = {
            key: copy.deepcopy(state[key]) for key in shared if key in state
        }
        child_config: dict[str, Any] = {
            key: value for key, value in config.items() if key != "configurable"
        }
        configurable = {
            key: value
            for key, value in dict(config.get("configurable") or {}).items()
            if key not in ("thread_id", "checkpoint_id")
        }
        parent_thread = self._thread_id(config)
        durable = (
            spec.subgraph_persistence is not SubgraphPersistence.STATELESS
            and self.checkpointer is not None
            and parent_thread is not None
        )

        if not durable:
            runnable = child._child_runtime(None, child.store or self.store)
            configurable["checkpoint_ns"] = "|".join((*runtime.namespace, task.node))
            child_config["configurable"] = configurable
            try:
                result = await self._invoke_child(runnable, subinput, child_config, runtime)
            except _ParentCommand as signal:
                return Command(
                    update=signal.command.update,
                    goto=signal.command.goto,
                    scope=CommandScope.SELF,
                )
        else:
            runnable = child._child_runtime(self.checkpointer, child.store or self.store)
            # One child thread per activation: stable across interrupt replays
            # of the same superstep, fresh when a parent loop re-enters the node.
            activation = (
                task.node
                if spec.subgraph_persistence is SubgraphPersistence.THREAD
                else f"{task.id}@{step}"
            )
            configurable["thread_id"] = str(parent_thread)
            parent_namespace = tuple(runtime.namespace)
            configurable["checkpoint_ns"] = "|".join(
                (*parent_namespace, f"{task.node}:{activation}")
            )
            child_config["configurable"] = configurable
            assert self.checkpointer is not None
            existing = await self._aget_tuple(child_config)
            pending = existing.checkpoint.pending_interrupts if existing is not None else ()
            if pending:
                own = context.resume_values
                if not own:
                    # Replay without a new answer: surface the child's pause again.
                    raise GraphInterrupt(self._wrap_child_interrupt(task, pending[0], 0))
                context.call_index = len(own)
                try:
                    result = await self._invoke_child(
                        runnable, Command(resume=own[-1]), child_config, runtime
                    )
                except _ParentCommand as signal:
                    return Command(
                        update=signal.command.update,
                        goto=signal.command.goto,
                        scope=CommandScope.SELF,
                    )
            else:
                context.call_index = len(context.resume_values)
                try:
                    result = await self._invoke_child(
                        runnable, subinput, child_config, runtime
                    )
                except _ParentCommand as signal:
                    return Command(
                        update=signal.command.update,
                        goto=signal.command.goto,
                        scope=CommandScope.SELF,
                    )

        inner = result.get("__interrupt__")
        if inner:
            raise GraphInterrupt(
                self._wrap_child_interrupt(task, inner[0], len(context.resume_values))
            )
        # The child already merged the parent's seed values, so its final values
        # replace the parent channels instead of passing through the reducers.
        return {key: ReplaceValue(result[key]) for key in shared if key in result}

    async def _invoke_child(
        self,
        child: CompiledStateGraph,
        input: Mapping[str, Any] | Command[Any],
        config: Mapping[str, Any],
        runtime: Runtime[Any],
    ) -> dict[str, Any]:
        if not runtime.stream_subgraphs:
            return await child.ainvoke(
                input,
                config,
                context=runtime.context,
                cancellation=runtime.cancellation,
                _is_subgraph=True,
                _project=False,
            )
        requested = runtime.stream_mode or "values"
        result: dict[str, Any] = {}
        async for mode, chunk in child.astream(
            input,
            config,
            stream_mode=(requested, "values"),
            context=runtime.context,
            cancellation=runtime.cancellation,
            subgraphs=True,
            _is_subgraph=True,
            _project=False,
        ):
            if mode == "values" and isinstance(chunk, Mapping):
                result = copy.deepcopy(dict(chunk))
            if mode == requested:
                namespace_value = config.get("configurable", {}).get("checkpoint_ns", "")
                child_namespace = tuple(
                    part for part in str(namespace_value).split("|") if part
                )
                runtime.emit("__subgraph__", (child_namespace, chunk))
        return result

    @staticmethod
    def _wrap_child_interrupt(task: _Task, inner: Interrupt, consumed: int) -> Interrupt:
        return Interrupt(
            value=inner.value,
            id=f"{task.id}:{consumed}",
            when="during",
            task_id=task.id,
        )

    async def _call(
        self,
        action: Callable[..., Any],
        argument: Any,
        config: Mapping[str, Any],
        arity: int,
        *,
        runtime: Runtime[Any] | None = None,
        uses_runtime: bool = False,
    ) -> Any:
        second = runtime if uses_runtime else MappingProxyType(dict(config))
        args = (argument,) if arity == 1 else (argument, second)
        if inspect.iscoroutinefunction(action):
            return await action(*args)
        result = await asyncio.to_thread(action, *args)
        if inspect.isawaitable(result):
            return await result
        return result

    def _normalize_targets(self, targets: tuple[str, ...]) -> tuple[str, ...]:
        unique: set[str] = set()
        for target in targets:
            if target == END:
                continue
            if target == START or target not in self.nodes:
                raise GraphValidationError(f"runtime route selected unknown node {target!r}")
            unique.add(target)
        return tuple(sorted(unique, key=self._node_order.__getitem__))

    def _split_targets(
        self, targets: tuple[str | Send, ...]
    ) -> tuple[tuple[str, ...], tuple[Send, ...]]:
        names = tuple(target for target in targets if isinstance(target, str))
        sends = tuple(target for target in targets if isinstance(target, Send))
        for send in sends:
            if send.node not in self.nodes:
                raise GraphValidationError(f"Send targets unknown node {send.node!r}")
        return names, sends

    @staticmethod
    def _as_targets(value: Any) -> tuple[Any, ...]:
        if value is None:
            return ()
        if isinstance(value, (str, Send)):
            return (value,)
        if isinstance(value, (tuple, list, set, frozenset)):
            if not all(isinstance(item, (str, Send)) for item in value):
                raise GraphValidationError(
                    "route targets must be node-name strings or Send objects"
                )
            return tuple(value)
        raise GraphValidationError(
            "route must return a node name, a Send, or a sequence of them"
        )

    async def _save_checkpoint(
        self,
        config: Mapping[str, Any],
        state: Mapping[str, Any],
        next_nodes: tuple[str, ...],
        pending_sends: tuple[Send, ...],
        step: int,
        pending_interrupts: tuple[Interrupt, ...],
        metadata: Mapping[str, Any],
        *,
        parent_id: str | None = None,
        namespace: tuple[str, ...] = (),
        run_id: str | None = None,
        channel_versions: Mapping[str, int] | None = None,
        tasks: tuple[TaskSnapshot, ...] = (),
        writer: _AsyncCheckpointWriter | None = None,
    ) -> str | None:
        if self.checkpointer is None:
            return None
        checkpoint = Checkpoint(
            id=str(uuid4()),
            ts=_utc_now(),
            step=step,
            channel_values=copy.deepcopy(dict(state)),
            next=next_nodes,
            pending_sends=copy.deepcopy(pending_sends),
            pending_interrupts=pending_interrupts,
            parent_id=parent_id,
            namespace=namespace,
            run_id=run_id,
            channel_versions=dict(channel_versions or {}),
            tasks=tasks,
        )
        encoded = self.serializer.dumps(checkpoint)
        limits = {**dict(config.get("configurable", {})), **dict(config)}
        max_state_bytes = limits.get("max_state_bytes")
        if max_state_bytes is not None and len(encoded) > int(max_state_bytes):
            raise PersistenceError(
                f"checkpoint size {len(encoded)} exceeds max_state_bytes={max_state_bytes}"
            )
        with start_span(
            "lingxigraph.checkpoint.put",
            {
                "lingxigraph.checkpoint.id": checkpoint.id,
                "lingxigraph.checkpoint.step": step,
                "lingxigraph.run.id": run_id or "",
            },
        ):
            if writer is not None:
                writer.enqueue(lambda: self._aput(config, checkpoint, metadata))
            else:
                await self._aput(config, checkpoint, metadata)
        return checkpoint.id

    def _make_runtime(
        self,
        *,
        context: Any | None,
        config: Mapping[str, Any],
        run_id: str,
        task_id: str,
        checkpoint_id: str | None,
        namespace: tuple[str, ...],
        cancellation: CancellationToken,
        deadline: datetime | None,
        emit: Callable[[str, Any], None] | None = None,
        metadata: Mapping[str, Any] | None = None,
        task_path: tuple[str, ...] = (),
        remaining_steps: int | None = None,
        stream_mode: str | None = None,
        stream_subgraphs: bool = False,
    ) -> Runtime[Any]:
        # A retry or lease recovery receives the same key even when it happens
        # in another process/run attempt.  External side-effect services can
        # therefore provide exactly-once behavior on top of our at-least-once
        # task delivery semantics.
        raw_key = "|".join(
            (
                self.graph_name,
                self.graph_version,
                checkpoint_id or "input",
                *namespace,
                *task_path,
                task_id,
            )
        )
        idempotency_key = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
        return Runtime(
            context=context,
            config=MappingProxyType(dict(config)),
            store=self.store,
            cache=self.cache,
            cancellation=cancellation,
            deadline=deadline,
            run_id=run_id,
            task_id=task_id,
            checkpoint_id=checkpoint_id,
            namespace=namespace,
            idempotency_key=idempotency_key,
            metadata=MappingProxyType(dict(metadata or {})),
            remaining_steps=remaining_steps,
            stream_mode=stream_mode,
            stream_subgraphs=stream_subgraphs,
            budget=self._run_budgets.get(run_id),
            _emit=emit,
        )

    async def _aget_tuple(self, config: Mapping[str, Any]) -> Any:
        if self.checkpointer is None:
            return None
        method = getattr(self.checkpointer, "aget_tuple", None)
        if method is not None:
            return await method(config)
        return await asyncio.to_thread(self.checkpointer.get_tuple, config)

    async def _aput(
        self,
        config: Mapping[str, Any],
        checkpoint: Checkpoint,
        metadata: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        if self.checkpointer is None:
            return config
        method = getattr(self.checkpointer, "aput", None)
        if method is not None:
            return await method(config, checkpoint, metadata)
        return await asyncio.to_thread(self.checkpointer.put, config, checkpoint, metadata)

    def _cache_key(self, task: _Task, spec: _NodeSpec, argument: Any) -> str | None:
        if self.cache is None or spec.cache is None:
            return None
        selected = argument
        if spec.cache.key_fields and isinstance(argument, Mapping):
            selected = {
                key: argument.get(key)
                for key in spec.cache.key_fields
                if key in argument
            }
        elif isinstance(argument, Mapping):
            # Node state is deliberately exposed as an immutable mappingproxy,
            # while cache keys use the equivalent JSON-safe mapping value.
            selected = dict(argument)
        payload = self.serializer.dumps(
            {
                "graph": self.graph_name,
                "version": self.graph_version,
                "schema": self.schema_hash,
                "node": task.node,
                "input": selected,
            }
        )
        digest = hashlib.sha256(payload).hexdigest()
        return f"{spec.cache.namespace}:{digest}"

    async def _cache_get(self, key: str) -> Any | None:
        if self.cache is None:
            return None
        try:
            method = getattr(self.cache, "aget", None)
            if method is not None:
                return await method(key)
            return await asyncio.to_thread(self.cache.get, key)
        except Exception:
            # Cache is an optimization. Redis/cache outages must never stop a
            # durable run whose source of truth is the checkpointer.
            return None

    async def _cache_set(self, key: str, value: Any, ttl: float | None) -> None:
        if self.cache is None:
            return
        try:
            method = getattr(self.cache, "aset", None)
            if method is not None:
                await method(key, value, ttl=ttl)
                return
            await asyncio.to_thread(self.cache.set, key, value, ttl=ttl)
        except Exception:
            return

    async def _get_pending_writes(
        self, config: Mapping[str, Any], checkpoint_id: str
    ) -> tuple[PendingWrite, ...]:
        if self.checkpointer is None:
            return ()
        method = getattr(self.checkpointer, "aget_writes", None)
        if method is not None:
            return tuple(await method(config, checkpoint_id))
        method = getattr(self.checkpointer, "get_writes", None)
        if method is None:
            return ()
        return tuple(await asyncio.to_thread(method, config, checkpoint_id))

    async def _put_pending_results(
        self,
        config: Mapping[str, Any],
        checkpoint_id: str,
        tasks: list[_Task],
        results: list[_TaskResult],
    ) -> None:
        if self.checkpointer is None or not results:
            return
        order = {task.id: index for index, task in enumerate(tasks)}
        writes = tuple(
            PendingWrite(
                checkpoint_id=checkpoint_id,
                task_id=result.task.id,
                index=order[result.task.id],
                values=self._display_update(result.update),
                task_path=result.task.path,
                goto=result.goto,
            )
            for result in results
        )
        method = getattr(self.checkpointer, "aput_writes", None)
        if method is not None:
            await method(config, checkpoint_id, writes)
            return
        method = getattr(self.checkpointer, "put_writes", None)
        if method is not None:
            await asyncio.to_thread(method, config, checkpoint_id, writes)

    @staticmethod
    def _task_snapshot(result: _TaskResult) -> TaskSnapshot:
        return TaskSnapshot(
            id=result.task.id,
            name=result.task.node,
            path=result.task.path,
            interrupts=(result.interrupt,) if result.interrupt is not None else (),
            result=(
                CompiledStateGraph._display_update(result.update)
                if result.interrupt is None
                else None
            ),
        )

    def _event(
        self,
        kind: EventKind,
        run_id: str,
        *,
        step: int | None = None,
        node: str | None = None,
        data: Mapping[str, Any] | None = None,
        namespace: tuple[str, ...] = (),
        task_id: str | None = None,
        checkpoint_id: str | None = None,
        thread_id: str | None = None,
    ) -> Event:
        sequence = self._event_sequences.get(run_id, 0) + 1
        self._event_sequences[run_id] = sequence
        return Event(
            kind,
            run_id,
            step=step,
            node=node,
            data=data or {},
            namespace=namespace,
            task_id=task_id,
            checkpoint_id=checkpoint_id,
            graph_id=self.graph_name,
            thread_id=thread_id,
            sequence=sequence,
        )

    @staticmethod
    def _display_update(update: Mapping[str, Any]) -> dict[str, Any]:
        return {
            key: copy.deepcopy(value.value if isinstance(value, ReplaceValue) else value)
            for key, value in update.items()
        }

    @staticmethod
    def _serialize_join_progress(progress: Mapping[int, set[str]]) -> dict[str, list[str]]:
        return {str(index): sorted(nodes) for index, nodes in progress.items() if nodes}

    @staticmethod
    def _interrupt_output(
        state: Mapping[str, Any], markers: tuple[Interrupt, ...]
    ) -> dict[str, Any]:
        return {**copy.deepcopy(dict(state)), "__interrupt__": markers}

    def _require_interrupt_support(self, config: Mapping[str, Any]) -> None:
        if self.checkpointer is None or not self._has_thread_id(config):
            raise RuntimeError("interrupts require a checkpointer and a configurable thread_id")

    def _require_thread_id(self, config: Mapping[str, Any]) -> None:
        if not self._has_thread_id(config):
            raise ValueError("checkpointer requires config['configurable']['thread_id']")

    @staticmethod
    def _thread_id(config: Mapping[str, Any]) -> str | None:
        configurable = config.get("configurable")
        if isinstance(configurable, Mapping) and configurable.get("thread_id"):
            return str(configurable["thread_id"])
        return None

    @classmethod
    def _has_thread_id(cls, config: Mapping[str, Any]) -> bool:
        return cls._thread_id(config) is not None

    @staticmethod
    def _reject_running_loop(name: str, alternative: str) -> None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        raise RuntimeError(
            f"{name}() cannot be called from a running event loop; use {alternative}()"
        )


CompiledGraph = CompiledStateGraph

__all__ = ["CompiledGraph", "CompiledStateGraph"]
