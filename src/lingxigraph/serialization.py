"""Safe, versioned JSON serialization used by production persistence."""

from __future__ import annotations

import base64
import json
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, time
from enum import Enum
from pathlib import PurePath
from typing import Any, Protocol, runtime_checkable
from uuid import UUID


class SerializationError(TypeError):
    """Raised when runtime state is not safe to persist."""


@runtime_checkable
class Serializer(Protocol):
    def dumps(self, value: Any) -> bytes: ...

    def loads(self, payload: bytes) -> Any: ...


class JsonSerializer:
    """Strict JSON serializer with a small allowlist of lossless extensions."""

    version = 1

    def dumps(self, value: Any) -> bytes:
        envelope = {"version": self.version, "value": self._encode(value)}
        try:
            return json.dumps(
                envelope,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise SerializationError(str(exc)) from exc

    def loads(self, payload: bytes) -> Any:
        try:
            envelope = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SerializationError("invalid JSON payload") from exc
        if envelope.get("version") != self.version:
            raise SerializationError(
                f"unsupported serializer version {envelope.get('version')!r}"
            )
        return self._decode(envelope["value"])

    def validate(self, value: Any) -> None:
        self.dumps(value)

    def _encode(self, value: Any) -> Any:
        if value is None or isinstance(value, (bool, int, float, str)):
            return value
        if isinstance(value, bytes):
            return {"__type__": "bytes", "value": base64.b64encode(value).decode("ascii")}
        if isinstance(value, tuple):
            return {"__type__": "tuple", "items": [self._encode(item) for item in value]}
        if isinstance(value, (set, frozenset)):
            encoded = [self._encode(item) for item in value]
            encoded.sort(key=lambda item: json.dumps(item, sort_keys=True))
            return {"__type__": "set", "items": encoded}
        if isinstance(value, list):
            return [self._encode(item) for item in value]
        if isinstance(value, dict):
            if not all(isinstance(key, str) for key in value):
                raise SerializationError("JSON state mappings must use string keys")
            return {key: self._encode(item) for key, item in value.items()}
        if isinstance(value, datetime):
            return {"__type__": "datetime", "value": value.isoformat()}
        if isinstance(value, date):
            return {"__type__": "date", "value": value.isoformat()}
        if isinstance(value, time):
            return {"__type__": "time", "value": value.isoformat()}
        if isinstance(value, UUID):
            return {"__type__": "uuid", "value": str(value)}
        if isinstance(value, PurePath):
            return {"__type__": "path", "value": str(value)}
        if isinstance(value, Enum):
            return self._encode(value.value)
        if hasattr(value, "model_dump") and callable(value.model_dump):
            return self._encode(value.model_dump(mode="json"))
        if is_dataclass(value) and not isinstance(value, type):
            return self._encode(asdict(value))
        raise SerializationError(
            f"unsafe or unsupported state value {type(value).__module__}.{type(value).__qualname__}"
        )

    def _decode(self, value: Any) -> Any:
        if isinstance(value, list):
            return [self._decode(item) for item in value]
        if not isinstance(value, dict):
            return value
        kind = value.get("__type__")
        if kind == "bytes":
            return base64.b64decode(value["value"], validate=True)
        if kind == "tuple":
            return tuple(self._decode(item) for item in value["items"])
        if kind == "set":
            return set(self._decode(item) for item in value["items"])
        if kind == "datetime":
            return datetime.fromisoformat(value["value"])
        if kind == "date":
            return date.fromisoformat(value["value"])
        if kind == "time":
            return time.fromisoformat(value["value"])
        if kind == "uuid":
            return UUID(value["value"])
        if kind == "path":
            return value["value"]
        return {key: self._decode(item) for key, item in value.items()}


__all__ = ["JsonSerializer", "SerializationError", "Serializer"]
