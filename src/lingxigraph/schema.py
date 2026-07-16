"""Schema inspection and JSON-schema support for deployable graphs."""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, get_args, get_origin, get_type_hints

from .errors import GraphValidationError


def _json_type(annotation: Any) -> dict[str, Any]:
    origin = get_origin(annotation)
    args = get_args(annotation)
    if annotation in (Any, object):
        return {}
    if annotation is str:
        return {"type": "string"}
    if annotation is bool:
        return {"type": "boolean"}
    if annotation is int:
        return {"type": "integer"}
    if annotation is float:
        return {"type": "number"}
    if annotation is type(None):
        return {"type": "null"}
    if origin in (list, tuple, set, frozenset):
        item = _json_type(args[0]) if args else {}
        return {"type": "array", "items": item}
    if origin is dict:
        return {"type": "object", "additionalProperties": _json_type(args[1]) if args else {}}
    if origin is not None and str(origin).endswith("Union"):
        return {"anyOf": [_json_type(arg) for arg in args]}
    return {}


@dataclass(frozen=True, slots=True)
class SchemaAdapter:
    schema: type

    @property
    def fields(self) -> Mapping[str, Any]:
        model_fields = getattr(self.schema, "model_fields", None)
        if isinstance(model_fields, Mapping):
            return {
                name: getattr(field, "annotation", Any)
                for name, field in model_fields.items()
            }
        try:
            return get_type_hints(self.schema, include_extras=True)
        except (NameError, TypeError) as exc:
            raise GraphValidationError(f"cannot inspect schema {self.schema!r}: {exc}") from exc

    def json_schema(self) -> Mapping[str, Any]:
        method = getattr(self.schema, "model_json_schema", None)
        if callable(method):
            return method()
        fields = self.fields
        required = set(getattr(self.schema, "__required_keys__", ()))
        if dataclasses.is_dataclass(self.schema):
            required = {
                field.name
                for field in dataclasses.fields(self.schema)
                if field.default is dataclasses.MISSING
                and field.default_factory is dataclasses.MISSING
            }
        return {
            "type": "object",
            "title": getattr(self.schema, "__name__", "State"),
            "properties": {name: _json_type(annotation) for name, annotation in fields.items()},
            "required": sorted(required),
            "additionalProperties": False,
        }

    def validate(self, value: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            raise GraphValidationError("schema input must be a mapping")
        validate = getattr(self.schema, "model_validate", None)
        if callable(validate):
            model = validate(value)
            return dict(model.model_dump())
        unknown = set(value) - set(self.fields)
        if unknown:
            raise GraphValidationError(
                "input contains unknown field(s): " + ", ".join(sorted(unknown))
            )
        return dict(value)

    def fingerprint(self) -> str:
        import hashlib

        raw = json.dumps(self.json_schema(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


__all__ = ["SchemaAdapter"]
