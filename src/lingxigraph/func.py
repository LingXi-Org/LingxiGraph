"""Minimal functional API backed by the durable graph runtime."""

from __future__ import annotations

import hashlib
import inspect
import json
from collections.abc import Callable
from functools import wraps
from typing import Any

from .constants import END, START
from .graph import StateGraph
from .runtime import Runtime, get_runtime


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
    """Create a reusable idempotent task with optional durable result caching.

    Inside graph execution the wrapper derives a stable key from the enclosing
    node idempotency key and call arguments.  When a Store is configured, a
    successful JSON-compatible result is persisted and reused after retries or
    lease recovery.  ``runtime`` and ``idempotency_key`` parameters are injected
    when declared by the wrapped callable.
    """

    def invocation(runtime: Runtime[Any] | None, args: tuple[Any, ...], kwargs: dict[str, Any]):
        values = dict(kwargs)
        parameters = inspect.signature(function).parameters
        if "runtime" in parameters and "runtime" not in values:
            values["runtime"] = runtime
        if "idempotency_key" in parameters and "idempotency_key" not in values:
            if runtime is None:
                raise RuntimeError("idempotency_key injection requires graph execution")
            values["idempotency_key"] = _task_key(function, runtime, args, kwargs)
        return values

    if inspect.iscoroutinefunction(function):
        @wraps(function)
        async def wrapped(*args: Any, **kwargs: Any) -> Any:
            runtime = _optional_runtime()
            key = _task_key(function, runtime, args, kwargs) if runtime is not None else None
            store = runtime.store if runtime is not None else None
            if key is not None and store is not None:
                getter = getattr(store, "aget", None)
                item = await getter(("__lingxigraph_tasks__",), key) if getter else store.get(
                    ("__lingxigraph_tasks__",), key
                )
                if item is not None:
                    return item.value["result"]
            result = await function(*args, **invocation(runtime, args, kwargs))
            if key is not None and store is not None:
                putter = getattr(store, "aput", None)
                if putter:
                    await putter(("__lingxigraph_tasks__",), key, {"result": result})
                else:
                    store.put(("__lingxigraph_tasks__",), key, {"result": result})
            return result
    else:
        @wraps(function)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            runtime = _optional_runtime()
            key = _task_key(function, runtime, args, kwargs) if runtime is not None else None
            store = runtime.store if runtime is not None else None
            if key is not None and store is not None and hasattr(store, "get"):
                item = store.get(("__lingxigraph_tasks__",), key)
                if item is not None:
                    return item.value["result"]
            result = function(*args, **invocation(runtime, args, kwargs))
            if key is not None and store is not None and hasattr(store, "put"):
                store.put(("__lingxigraph_tasks__",), key, {"result": result})
            return result

    wrapped.__lingxigraph_task__ = True  # type: ignore[attr-defined]
    return wrapped


def _optional_runtime() -> Runtime[Any] | None:
    try:
        return get_runtime()
    except RuntimeError:
        return None


def _task_key(
    function: Callable[..., Any],
    runtime: Runtime[Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> str:
    payload = json.dumps(
        {"args": args, "kwargs": kwargs},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=repr,
    )
    raw = f"{runtime.idempotency_key}|{function.__module__}.{function.__qualname__}|{payload}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


__all__ = ["entrypoint", "task"]
