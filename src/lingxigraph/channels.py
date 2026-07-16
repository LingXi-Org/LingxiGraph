"""State channels and reducer discovery for ``TypedDict`` schemas."""

from __future__ import annotations

import copy
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Annotated, Any, get_args, get_origin, get_type_hints

from .errors import InvalidUpdateError

Reducer = Callable[[Any, Any], Any]


@dataclass(frozen=True, slots=True)
class ReplaceValue:
    """Internal write wrapper that overwrites a channel instead of reducing.

    Subgraph nodes return their final channel values wrapped in this marker:
    the child already merged the parent's seed value, so folding the result
    through the reducer again would double-count it.
    """

    value: Any


@dataclass(frozen=True, slots=True)
class LastValue:
    """A channel that accepts at most one write per superstep."""

    value_type: Any = Any

    def merge(self, current: Any, writes: list[Any], *, key: str) -> Any:
        if len(writes) > 1:
            raise InvalidUpdateError(
                f"state key {key!r} received {len(writes)} writes in one superstep; "
                "declare an Annotated reducer to merge concurrent writes"
            )
        if not writes:
            return current
        write = writes[0]
        if isinstance(write, ReplaceValue):
            write = write.value
        return copy.deepcopy(write)


@dataclass(frozen=True, slots=True)
class BinaryOperatorAggregate:
    """A channel that folds writes through a binary reducer."""

    value_type: Any
    operator: Reducer

    def merge(self, current: Any, writes: list[Any], *, key: str) -> Any:
        del key
        if not writes:
            return current
        result = _MISSING if current is _MISSING else copy.deepcopy(current)
        for value in writes:
            if isinstance(value, ReplaceValue):
                result = copy.deepcopy(value.value)
            elif result is _MISSING:
                result = copy.deepcopy(value)
            else:
                result = self.operator(result, copy.deepcopy(value))
        return result


Channel = LastValue | BinaryOperatorAggregate
_MISSING = object()


def extract_channels(schema: type) -> dict[str, Channel]:
    """Build channels from a ``TypedDict``-style annotated schema."""

    model_fields = getattr(schema, "model_fields", None)
    if isinstance(model_fields, Mapping):
        hints = {
            name: getattr(field, "annotation", Any)
            for name, field in model_fields.items()
        }
    else:
        try:
            hints = get_type_hints(schema, include_extras=True)
        except (NameError, TypeError) as exc:
            raise InvalidUpdateError(f"cannot inspect state schema {schema!r}: {exc}") from exc
    if not hints:
        raise InvalidUpdateError("state schema must declare at least one annotated key")

    channels: dict[str, Channel] = {}
    for key, hint in hints.items():
        if get_origin(hint) is Annotated:
            args = get_args(hint)
            value_type, metadata = args[0], args[1:]
            reducer = next((item for item in metadata if callable(item)), None)
            channels[key] = (
                BinaryOperatorAggregate(value_type, reducer)
                if reducer is not None
                else LastValue(value_type)
            )
        else:
            channels[key] = LastValue(hint)
    return channels


def merge_updates(
    state: Mapping[str, Any],
    updates: list[tuple[str, Mapping[str, Any]]],
    channels: Mapping[str, Channel],
) -> dict[str, Any]:
    """Merge node updates deterministically in the supplied task order."""

    writes: dict[str, list[Any]] = {key: [] for key in channels}
    for node, update in updates:
        if not isinstance(update, Mapping):
            raise InvalidUpdateError(
                f"node {node!r} returned {type(update).__name__}; expected dict or Command"
            )
        unknown = set(update) - set(channels)
        if unknown:
            names = ", ".join(sorted(unknown))
            raise InvalidUpdateError(f"node {node!r} wrote unknown state key(s): {names}")
        for key, value in update.items():
            writes[key].append(value)

    merged = copy.deepcopy(dict(state))
    for key, channel in channels.items():
        current = merged.get(key, _MISSING)
        value = channel.merge(current, writes[key], key=key)
        if value is not _MISSING:
            merged[key] = value
    return merged


__all__ = [
    "BinaryOperatorAggregate",
    "Channel",
    "LastValue",
    "ReplaceValue",
    "extract_channels",
    "merge_updates",
]
