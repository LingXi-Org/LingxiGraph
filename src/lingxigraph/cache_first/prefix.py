"""Immutable, fingerprinted model prefix representation."""

from __future__ import annotations

import copy
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from ..messages import AIMessage, AnyMessage
from .canonical import canonicalize, canonicalize_schema, fingerprint
from .catalog import (
    ToolCatalogFingerprint,
    build_tool_catalog_fingerprint,
    canonicalize_tools,
    compare_tool_catalogs,
)


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze(item) for item in value)
    return value


def _message_shape(message: AnyMessage) -> Any:
    result: dict[str, Any] = {"type": message.type, "content": copy.deepcopy(message.content)}
    if getattr(message, "name", None):
        result["name"] = message.name
    if isinstance(message, AIMessage):
        result["tool_calls"] = [
            {
                "id": call.id,
                "name": call.name,
                "args": canonicalize(dict(call.args)),
                "type": call.type,
            }
            for call in message.tool_calls
        ]
    return result


class PrefixDriftError(ValueError):
    """Raised when an immutable prefix no longer matches its fingerprint."""

    def __init__(self, diagnostic: "PrefixDriftDiagnostic") -> None:
        self.diagnostic = diagnostic
        super().__init__(diagnostic.message)


@dataclass(frozen=True, slots=True)
class PrefixDriftDiagnostic:
    expected_fingerprint: str
    actual_fingerprint: str
    changed_sections: tuple[str, ...]
    previous_revision: int
    current_revision: int
    first_changed_component: str | None = None
    suggestion: str = (
        "Keep stable prefix data immutable and move volatile data to the request suffix."
    )
    tool_catalog_diff: Mapping[str, Any] | None = None

    @property
    def drift(self) -> bool:
        return self.expected_fingerprint != self.actual_fingerprint

    @property
    def message(self) -> str:
        sections = ", ".join(self.changed_sections) or "unknown"
        return (
            "immutable prefix fingerprint drift: "
            f"expected={self.expected_fingerprint}, actual={self.actual_fingerprint}, "
            f"sections={sections}, revision={self.previous_revision}->{self.current_revision}"
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "expected_fingerprint": self.expected_fingerprint,
            "actual_fingerprint": self.actual_fingerprint,
            "changed_sections": list(self.changed_sections),
            "previous_revision": self.previous_revision,
            "current_revision": self.current_revision,
            "first_changed_component": self.first_changed_component,
            "suggestion": self.suggestion,
            "tool_catalog_diff": dict(self.tool_catalog_diff or {}),
        }


@dataclass(frozen=True, slots=True)
class ImmutablePrefix:
    system_prompt: str = ""
    tools: tuple[Mapping[str, Any], ...] = ()
    few_shots: tuple[AnyMessage, ...] = ()
    pinned_constraints: tuple[str, ...] = ()
    revision: int = 1
    fingerprint: str = field(init=False)

    def __post_init__(self) -> None:
        normalized_tools = tuple(
            MappingProxyType(_freeze(dict(item))) for item in canonicalize_tools(self.tools)
        )
        normalized_shots = tuple(copy.deepcopy(self.few_shots))
        normalized_constraints = tuple(str(item) for item in self.pinned_constraints)
        if self.revision < 1:
            raise ValueError("prefix revision must be positive")
        object.__setattr__(self, "system_prompt", str(self.system_prompt))
        object.__setattr__(self, "tools", normalized_tools)
        object.__setattr__(self, "few_shots", normalized_shots)
        object.__setattr__(self, "pinned_constraints", normalized_constraints)
        object.__setattr__(self, "fingerprint", self._compute_fingerprint())

    @classmethod
    def create(
        cls,
        *,
        system_prompt: str = "",
        tools: Sequence[Any] = (),
        few_shots: Sequence[AnyMessage] = (),
        pinned_constraints: Sequence[str] = (),
    ) -> "ImmutablePrefix":
        return cls(system_prompt, tuple(tools), tuple(few_shots), tuple(pinned_constraints))

    @property
    def tool_catalog(self) -> ToolCatalogFingerprint:
        return build_tool_catalog_fingerprint(self.tools)

    def _shape(self) -> dict[str, Any]:
        return {
            "system_prompt": self.system_prompt,
            "tools": [canonicalize_schema(dict(item)) for item in self.tools],
            "few_shots": [_message_shape(item) for item in self.few_shots],
            "pinned_constraints": list(self.pinned_constraints),
        }

    def _compute_fingerprint(self) -> str:
        return fingerprint(self._shape())

    def verify(self) -> str:
        actual = self._compute_fingerprint()
        if actual != self.fingerprint:
            diagnostic = PrefixDriftDiagnostic(
                expected_fingerprint=self.fingerprint,
                actual_fingerprint=actual,
                changed_sections=("prefix",),
                previous_revision=self.revision,
                current_revision=self.revision,
            )
            raise PrefixDriftError(diagnostic)
        return actual

    def drift_against(self, other: "ImmutablePrefix") -> PrefixDriftDiagnostic:
        changed: list[str] = []
        first: str | None = None
        if self.system_prompt != other.system_prompt:
            changed.append("system_prompt")
            first = first or "system_prompt"
        if tuple(self.tools) != tuple(other.tools):
            changed.append("tools")
            first = first or "tools"
        if tuple(_message_shape(item) for item in self.few_shots) != tuple(
            _message_shape(item) for item in other.few_shots
        ):
            changed.append("few_shots")
            first = first or "few_shots"
        if self.pinned_constraints != other.pinned_constraints:
            changed.append("pinned_constraints")
            first = first or "pinned_constraints"
        if self.revision != other.revision:
            changed.append("revision")
            first = first or "revision"
        catalog_diff = compare_tool_catalogs(self.tool_catalog, other.tool_catalog)
        return PrefixDriftDiagnostic(
            expected_fingerprint=self.fingerprint,
            actual_fingerprint=other.fingerprint,
            changed_sections=tuple(changed),
            previous_revision=self.revision,
            current_revision=other.revision,
            first_changed_component=first,
            tool_catalog_diff={
                "kind": catalog_diff.kind,
                "added": list(catalog_diff.added),
                "removed": list(catalog_diff.removed),
                "changed": list(catalog_diff.changed),
            },
        )

    def evolve(
        self,
        *,
        system_prompt: str | None = None,
        tools: Sequence[Any] | None = None,
        few_shots: Sequence[AnyMessage] | None = None,
        pinned_constraints: Sequence[str] | None = None,
    ) -> "ImmutablePrefix":
        return type(self)(
            self.system_prompt if system_prompt is None else system_prompt,
            self.tools if tools is None else tuple(tools),
            self.few_shots if few_shots is None else tuple(few_shots),
            self.pinned_constraints
            if pinned_constraints is None
            else tuple(pinned_constraints),
            self.revision + 1,
        )

    def render_messages(self) -> tuple[AnyMessage, ...]:
        from ..messages import SystemMessage

        messages: list[AnyMessage] = []
        stable_prompt = self.system_prompt
        if self.pinned_constraints:
            stable_prompt = (
                f"{stable_prompt}\n\n<pinned_constraints>\n"
                + "\n".join(f"- {item}" for item in self.pinned_constraints)
                + "\n</pinned_constraints>"
            ).strip()
        if stable_prompt:
            messages.append(SystemMessage(stable_prompt))
        messages.extend(self.few_shots)
        return tuple(messages)


__all__ = ["ImmutablePrefix", "PrefixDriftDiagnostic", "PrefixDriftError"]
