"""Minimal functional API backed by the durable graph runtime."""

from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import Any

from .constants import END, START
from .graph import StateGraph


def entrypoint(
    state_schema: type,
    *,
    name: str | None = None,
    checkpointer: Any | None = None,
    store: Any | None = None,
):
    """Compile a one-node durable graph from a state-transform function."""

    def decorate(function: Callable[..., Any]):
        graph = StateGraph(state_schema, name=name or function.__name__, version="2")
        graph.add_node(function.__name__, function)
        graph.add_edge(START, function.__name__).add_edge(function.__name__, END)
        compiled = graph.compile(checkpointer=checkpointer, store=store)
        compiled.__wrapped__ = function  # type: ignore[attr-defined]
        return compiled

    return decorate


def task(function: Callable[..., Any]) -> Callable[..., Any]:
    """Mark a callable as a reusable functional task.

    The marker is intentionally lightweight in 2.0; durability is supplied by
    the enclosing ``@entrypoint`` graph node.
    """

    @wraps(function)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        return function(*args, **kwargs)

    wrapped.__lingxigraph_task__ = True  # type: ignore[attr-defined]
    return wrapped


__all__ = ["entrypoint", "task"]
