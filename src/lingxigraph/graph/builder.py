"""Mutable graph builder and compile-time validation."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ..cache import BaseCache
from ..channels import extract_channels
from ..constants import END, START
from ..errors import GraphValidationError, InvalidUpdateError
from ..schema import SchemaAdapter
from ..serialization import JsonSerializer, Serializer
from ..types import CachePolicy, RetryPolicy, SubgraphPersistence

if TYPE_CHECKING:
    from .executor import CompiledStateGraph


@dataclass(frozen=True, slots=True)
class _Edge:
    sources: tuple[str, ...]
    target: str
    trigger: str = "all"


@dataclass(frozen=True, slots=True)
class _ConditionalEdge:
    source: str
    path: Callable[..., Any]
    path_map: Mapping[Any, str] | None


@dataclass(frozen=True, slots=True)
class _NodeSpec:
    action: Callable[..., Any]
    retry: RetryPolicy | None = None
    cache: CachePolicy | None = None
    timeout: float | None = None
    max_concurrency: int | None = None
    subgraph: Any | None = None
    subgraph_persistence: SubgraphPersistence = SubgraphPersistence.INVOCATION
    destinations: tuple[str, ...] = ()
    metadata: Mapping[str, Any] | None = None
    middleware: tuple[Any, ...] = ()


class StateGraph:
    """Build a stateful directed graph before compiling it for execution."""

    def __init__(
        self,
        state_schema: type,
        *,
        input_schema: type | None = None,
        output_schema: type | None = None,
        context_schema: type | None = None,
        name: str | None = None,
        version: str = "1",
    ) -> None:
        self.state_schema = state_schema
        self.input_schema = input_schema or state_schema
        self.output_schema = output_schema or state_schema
        self.context_schema = context_schema
        self.name = name
        self.version = version
        self.state_adapter = SchemaAdapter(state_schema)
        self.input_adapter = SchemaAdapter(self.input_schema)
        self.output_adapter = SchemaAdapter(self.output_schema)
        self.context_adapter = SchemaAdapter(context_schema) if context_schema is not None else None
        try:
            self.channels = extract_channels(state_schema)
        except InvalidUpdateError as exc:
            raise GraphValidationError(str(exc)) from exc
        self._nodes: dict[str, _NodeSpec] = {}
        self._edges: list[_Edge] = []
        self._conditional_edges: list[_ConditionalEdge] = []

    def add_node(
        self,
        name: str,
        action: "Callable[..., Any] | CompiledStateGraph",
        *,
        retry: RetryPolicy | None = None,
        retry_policy: RetryPolicy | None = None,
        cache_policy: CachePolicy | None = None,
        timeout: float | None = None,
        max_concurrency: int | None = None,
        destinations: Iterable[str] | None = None,
        metadata: Mapping[str, Any] | None = None,
        middleware: Iterable[Any] | None = None,
        subgraph_persistence: SubgraphPersistence | str = SubgraphPersistence.INVOCATION,
    ) -> StateGraph:
        """Register a node.

        ``destinations`` declares nodes this one may jump to at runtime via
        ``Command(goto=...)`` or ``Send``; they take part in reachability
        validation without adding execution edges.
        """

        if name in (START, END):
            raise GraphValidationError(f"{name!r} is a reserved node name")
        if not isinstance(name, str) or not name:
            raise GraphValidationError("node name must be a non-empty string")
        if name in self._nodes:
            raise GraphValidationError(f"node {name!r} is already registered")
        if retry is not None and retry_policy is not None:
            raise GraphValidationError("use either retry or retry_policy, not both")
        retry = retry_policy or retry
        if timeout is not None and timeout <= 0:
            raise GraphValidationError("node timeout must be positive")
        if max_concurrency is not None and max_concurrency < 1:
            raise GraphValidationError("max_concurrency must be at least 1")
        targets = tuple(destinations or ())
        persistence = SubgraphPersistence(subgraph_persistence)
        from .executor import CompiledStateGraph

        if isinstance(action, CompiledStateGraph):
            shared = set(action.channels) & set(self.channels)
            if not shared:
                raise GraphValidationError(
                    f"subgraph node {name!r} shares no state keys with the parent graph"
                )
            self._nodes[name] = _NodeSpec(
                action=action.ainvoke,
                retry=retry,
                cache=cache_policy,
                timeout=timeout,
                max_concurrency=max_concurrency,
                subgraph=action,
                subgraph_persistence=persistence,
                destinations=targets,
                metadata=dict(metadata or {}),
                middleware=tuple(middleware or ()),
            )
            return self
        if not callable(action):
            raise GraphValidationError(f"node {name!r} action must be callable")
        self._nodes[name] = _NodeSpec(
            action=action,
            retry=retry,
            cache=cache_policy,
            timeout=timeout,
            max_concurrency=max_concurrency,
            destinations=targets,
            metadata=dict(metadata or {}),
            middleware=tuple(middleware or ()),
        )
        return self

    def add_edge(
        self, start: str | Iterable[str], end: str, *, trigger: str = "all"
    ) -> StateGraph:
        sources = (start,) if isinstance(start, str) else tuple(start)
        if not sources:
            raise GraphValidationError("an edge must have at least one source")
        if len(set(sources)) != len(sources):
            raise GraphValidationError("fan-in edge sources must be unique")
        if trigger not in {"all", "any"}:
            raise GraphValidationError("edge trigger must be 'all' or 'any'")
        if len(sources) == 1 and trigger != "all":
            raise GraphValidationError("trigger is only meaningful for fan-in edges")
        self._edges.append(_Edge(sources, end, trigger))
        return self

    def add_conditional_edges(
        self,
        source: str,
        path: Callable[..., Any],
        path_map: Mapping[Any, str] | None = None,
    ) -> StateGraph:
        if not callable(path):
            raise GraphValidationError("conditional path must be callable")
        self._conditional_edges.append(
            _ConditionalEdge(source, path, dict(path_map) if path_map is not None else None)
        )
        return self

    def set_entry_point(self, node: str) -> StateGraph:
        return self.add_edge(START, node)

    def set_finish_point(self, node: str) -> StateGraph:
        return self.add_edge(node, END)

    def compile(
        self,
        *,
        checkpointer: Any | None = None,
        store: Any | None = None,
        cache: BaseCache | None = None,
        serializer: Serializer | None = None,
        step_timeout: float | None = None,
        interrupt_before: Iterable[str] | None = None,
        interrupt_after: Iterable[str] | None = None,
    ) -> "CompiledStateGraph":
        before = tuple(interrupt_before or ())
        after = tuple(interrupt_after or ())
        if step_timeout is not None and step_timeout <= 0:
            raise GraphValidationError("step_timeout must be positive")
        self._validate(before, after)
        from .executor import CompiledStateGraph

        return CompiledStateGraph(
            state_schema=self.state_schema,
            input_schema=self.input_schema,
            output_schema=self.output_schema,
            context_schema=self.context_schema,
            graph_name=self.name,
            graph_version=self.version,
            channels=dict(self.channels),
            nodes=dict(self._nodes),
            edges=tuple(self._edges),
            conditional_edges=tuple(self._conditional_edges),
            checkpointer=checkpointer,
            store=store,
            cache=cache,
            serializer=serializer or JsonSerializer(),
            step_timeout=step_timeout,
            interrupt_before=before,
            interrupt_after=after,
        )

    def _validate(self, interrupt_before: tuple[str, ...], interrupt_after: tuple[str, ...]) -> None:
        known = set(self._nodes)
        if not self._nodes:
            raise GraphValidationError("graph must contain at least one node")

        has_entry = False
        for edge in self._edges:
            if END in edge.sources:
                raise GraphValidationError("END cannot be an edge source")
            if START in edge.sources and len(edge.sources) != 1:
                raise GraphValidationError("START cannot participate in a fan-in edge")
            for source in edge.sources:
                if source != START and source not in known:
                    raise GraphValidationError(f"edge source {source!r} is not a registered node")
            if edge.target == START:
                raise GraphValidationError("START cannot be an edge target")
            if edge.target != END and edge.target not in known:
                raise GraphValidationError(f"edge target {edge.target!r} is not a registered node")
            has_entry |= edge.sources == (START,)

        for conditional in self._conditional_edges:
            if conditional.source != START and conditional.source not in known:
                raise GraphValidationError(
                    f"conditional source {conditional.source!r} is not a registered node"
                )
            if conditional.path_map is not None:
                for target in conditional.path_map.values():
                    if target == START or (target != END and target not in known):
                        raise GraphValidationError(
                            f"conditional target {target!r} is not a registered node"
                        )
            has_entry |= conditional.source == START

        if not has_entry:
            raise GraphValidationError("graph has no entry edge from START")

        for name, spec in self._nodes.items():
            for target in spec.destinations:
                if target == START or (target != END and target not in known):
                    raise GraphValidationError(
                        f"node {name!r} declares unknown destination {target!r}"
                    )

        for node in (*interrupt_before, *interrupt_after):
            if node not in known:
                raise GraphValidationError(f"interrupt node {node!r} is not registered")

        self._check_reachability(known)

    def _check_reachability(self, known: set[str]) -> None:
        # A conditional edge without a path_map can route anywhere at runtime,
        # so once its source is reachable every node must be considered live.
        reachable: set[str] = set()
        changed = True
        while changed:
            changed = False
            for edge in self._edges:
                sources_reachable = all(
                    source == START or source in reachable for source in edge.sources
                )
                if sources_reachable and edge.target not in (END, START, *reachable):
                    reachable.add(edge.target)
                    changed = True
            for conditional in self._conditional_edges:
                if conditional.source != START and conditional.source not in reachable:
                    continue
                if conditional.path_map is None:
                    if not known <= reachable:
                        reachable |= known
                        changed = True
                    continue
                for target in conditional.path_map.values():
                    if target not in (END, START) and target not in reachable:
                        reachable.add(target)
                        changed = True
            for name, spec in self._nodes.items():
                if name not in reachable:
                    continue
                for target in spec.destinations:
                    if target not in (END, START) and target not in reachable:
                        reachable.add(target)
                        changed = True
        unreachable = known - reachable
        if unreachable:
            raise GraphValidationError(
                "unreachable node(s): " + ", ".join(sorted(unreachable))
            )


__all__ = ["StateGraph"]
