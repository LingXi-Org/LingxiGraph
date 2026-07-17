"""Schema inspection and JSON-schema support for deployable graphs."""

from __future__ import annotations

import dataclasses
import json
import types
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Annotated, Any, Literal, Union, get_args, get_origin, get_type_hints

from .channels import ReplaceValue
from .errors import GraphValidationError


def _json_type(annotation: Any) -> dict[str, Any]:
    if get_origin(annotation) is Annotated:
        annotation = get_args(annotation)[0]
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
    if origin in (Union, types.UnionType):
        return {"anyOf": [_json_type(arg) for arg in args]}
    if origin is Literal:
        return {"enum": list(args)}
    if isinstance(annotation, type) and (
        dataclasses.is_dataclass(annotation)
        or callable(getattr(annotation, "model_json_schema", None))
    ):
        return dict(SchemaAdapter(annotation).json_schema())
    return {}


def _matches(value: Any, annotation: Any, path: str) -> None:
    """Validate a value against the useful runtime subset of Python typing."""

    if isinstance(value, ReplaceValue):
        value = value.value
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin is Annotated:
        _matches(value, args[0], path)
        return
    if annotation in (Any, object) or annotation is None:
        return
    if origin in (Union, types.UnionType):
        failures: list[str] = []
        for candidate in args:
            try:
                _matches(value, candidate, path)
                return
            except GraphValidationError as exc:
                failures.append(str(exc))
        raise GraphValidationError(
            f"{path} does not match any allowed type: " + "; ".join(failures)
        )
    if origin is Literal:
        if value not in args:
            raise GraphValidationError(f"{path} must be one of {args!r}")
        return
    if origin in (list, set, frozenset, Sequence):
        if not isinstance(value, (list, tuple, set, frozenset)) or isinstance(value, (str, bytes)):
            raise GraphValidationError(f"{path} must be an array")
        item_type = args[0] if args else Any
        for index, item in enumerate(value):
            _matches(item, item_type, f"{path}[{index}]")
        return
    if origin is tuple:
        if not isinstance(value, tuple):
            raise GraphValidationError(f"{path} must be a tuple")
        if len(args) == 2 and args[1] is Ellipsis:
            for index, item in enumerate(value):
                _matches(item, args[0], f"{path}[{index}]")
        elif args and len(value) != len(args):
            raise GraphValidationError(f"{path} must contain {len(args)} items")
        else:
            for index, (item, item_type) in enumerate(zip(value, args, strict=False)):
                _matches(item, item_type, f"{path}[{index}]")
        return
    if origin in (dict, Mapping):
        if not isinstance(value, Mapping):
            raise GraphValidationError(f"{path} must be an object")
        key_type, value_type = args if len(args) == 2 else (Any, Any)
        for key, item in value.items():
            _matches(key, key_type, f"{path}.<key>")
            _matches(item, value_type, f"{path}.{key}")
        return
    if isinstance(annotation, type) and dataclasses.is_dataclass(annotation):
        nested: Mapping[str, Any]
        if isinstance(value, annotation):
            nested = {field.name: getattr(value, field.name) for field in dataclasses.fields(value)}
        elif isinstance(value, Mapping):
            nested = value
        else:
            raise GraphValidationError(f"{path} must match {annotation.__name__}")
        try:
            SchemaAdapter(annotation).validate(nested)
        except GraphValidationError as exc:
            raise GraphValidationError(f"{path} must match {annotation.__name__}: {exc}") from exc
        return
    model_validate = getattr(annotation, "model_validate", None)
    if callable(model_validate):
        try:
            model_validate(value)
        except Exception as exc:
            raise GraphValidationError(f"{path} must match {annotation.__name__}: {exc}") from exc
        return
    if isinstance(annotation, type):
        # bool is a subclass of int, but accepting it for an integer schema is
        # surprising and differs from JSON Schema semantics.
        if annotation is int and isinstance(value, bool):
            raise GraphValidationError(f"{path} must be int, got bool")
        if not isinstance(value, annotation):
            raise GraphValidationError(
                f"{path} must be {annotation.__name__}, got {type(value).__name__}"
            )


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

    def validate(self, value: Mapping[str, Any], *, partial: bool = False) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            raise GraphValidationError("schema input must be a mapping")
        validate = getattr(self.schema, "model_validate", None)
        if callable(validate):
            if partial:
                fields = set(self.fields)
                unknown = set(value) - fields
                if unknown:
                    raise GraphValidationError(
                        "input contains unknown field(s): " + ", ".join(sorted(unknown))
                    )
                for name, item in value.items():
                    _matches(item, self.fields[name], name)
                return dict(value)
            try:
                model = validate(value)
            except Exception as exc:
                raise GraphValidationError(
                    f"input does not match {self.schema.__name__}: {exc}"
                ) from exc
            return dict(model.model_dump())
        unknown = set(value) - set(self.fields)
        if unknown:
            raise GraphValidationError(
                "input contains unknown field(s): " + ", ".join(sorted(unknown))
            )
        required = set(getattr(self.schema, "__required_keys__", ()))
        if dataclasses.is_dataclass(self.schema):
            required = {
                field.name
                for field in dataclasses.fields(self.schema)
                if field.default is dataclasses.MISSING
                and field.default_factory is dataclasses.MISSING
            }
        if not partial:
            missing = required - set(value)
            if missing:
                raise GraphValidationError(
                    "input is missing required field(s): " + ", ".join(sorted(missing))
                )
        for name, item in value.items():
            _matches(item, self.fields[name], name)
        return dict(value)

    def validate_partial(self, value: Mapping[str, Any]) -> dict[str, Any]:
        """Validate a state update without requiring unrelated state fields."""

        return self.validate(value, partial=True)

    def fingerprint(self) -> str:
        import hashlib

        raw = json.dumps(self.json_schema(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


__all__ = ["SchemaAdapter"]
