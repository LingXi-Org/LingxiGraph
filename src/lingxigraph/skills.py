"""Agent Skills discovery, validation, progressive loading, and safe resources."""

from __future__ import annotations

import base64
import html
import json
import mimetypes
import os
import re
import stat
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Protocol, runtime_checkable

from .errors import LingxiGraphError

MAX_SKILL_BYTES = 256 * 1024
MAX_RESOURCE_BYTES = 1024 * 1024
_ALLOWED_FIELDS = {
    "name",
    "description",
    "license",
    "compatibility",
    "metadata",
    "allowed-tools",
}
_RESOURCE_DIRECTORIES = {"assets", "references", "scripts"}
_FIELD_PATTERN = re.compile(r"^([A-Za-z][A-Za-z0-9-]*):(?:[ \t]*(.*))?$")
_DRIVE_PATTERN = re.compile(r"^[A-Za-z]:")


class SkillError(LingxiGraphError):
    """Base exception for Agent Skills operations."""


class SkillNotFoundError(SkillError):
    """Raised when a requested skill is not registered."""


class SkillResourceError(SkillError):
    """Raised when a skill resource cannot be read safely."""


@dataclass(frozen=True, slots=True)
class SkillValidationIssue:
    """One actionable validation diagnostic for a skill directory."""

    path: str
    code: str
    message: str


class SkillValidationError(SkillError):
    """Raised when discovery finds invalid or duplicate skills."""

    def __init__(self, issues: Sequence[SkillValidationIssue]) -> None:
        self.issues = tuple(issues)
        detail = "; ".join(f"{issue.path}: {issue.message}" for issue in self.issues)
        super().__init__(detail or "skill validation failed")


@dataclass(frozen=True, slots=True)
class SkillMetadata:
    """The discovery-time Agent Skills metadata exposed to a model."""

    name: str
    description: str


@dataclass(frozen=True, slots=True)
class SkillSpec:
    """A fully loaded SKILL.md document and its parsed frontmatter."""

    metadata: SkillMetadata
    body: str
    content: str
    license: str | None = None
    compatibility: str | None = None
    allowed_tools: str | None = None
    extra_metadata: Mapping[str, str] = field(default_factory=dict)

    @property
    def name(self) -> str:
        return self.metadata.name

    @property
    def description(self) -> str:
        return self.metadata.description


@dataclass(frozen=True, slots=True)
class SkillResource:
    """A safely loaded resource contained by one registered skill."""

    skill_name: str
    path: str
    media_type: str
    content: bytes

    @property
    def size(self) -> int:
        return len(self.content)


@runtime_checkable
class SkillSource(Protocol):
    """Provider-neutral source interface for Agent Skills."""

    def discover(self) -> Sequence[SkillMetadata]: ...

    def load(self, name: str) -> SkillSpec: ...

    def read_resource(self, name: str, path: str) -> SkillResource: ...


def _issue(path: Path | str, code: str, message: str) -> SkillValidationIssue:
    return SkillValidationIssue(str(path), code, message)


def _is_reparse_point(value: os.stat_result) -> bool:
    attributes = getattr(value, "st_file_attributes", 0)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return stat.S_ISLNK(value.st_mode) or bool(attributes & reparse_flag)


def _strip_inline_comment(value: str) -> str:
    quote: str | None = None
    escaped = False
    for index, character in enumerate(value):
        if escaped:
            escaped = False
            continue
        if character == "\\" and quote == '"':
            escaped = True
            continue
        if character in {"'", '"'}:
            if quote is None:
                quote = character
            elif quote == character:
                quote = None
            continue
        if character == "#" and quote is None and (index == 0 or value[index - 1].isspace()):
            return value[:index].rstrip()
    return value.rstrip()


def _parse_scalar(value: str, *, line_number: int) -> str:
    value = _strip_inline_comment(value).strip()
    if not value:
        return ""
    if value.startswith('"'):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"line {line_number}: invalid double-quoted string") from exc
        if not isinstance(parsed, str):
            raise ValueError(f"line {line_number}: frontmatter values must be strings")
        return parsed
    if value.startswith("'"):
        if len(value) < 2 or not value.endswith("'"):
            raise ValueError(f"line {line_number}: invalid single-quoted string")
        return value[1:-1].replace("''", "'")
    if value[0] in "[{&*!":
        raise ValueError(
            f"line {line_number}: collections, tags, anchors, and aliases are not supported"
        )
    if value in {"---", "..."}:
        raise ValueError(f"line {line_number}: invalid scalar value")
    return value


def _fold_block(lines: Sequence[str], style: str) -> str:
    if style == "|":
        return "\n".join(lines)
    paragraphs: list[str] = []
    current: list[str] = []
    for line in lines:
        if line:
            current.append(line)
        else:
            if current:
                paragraphs.append(" ".join(current))
                current = []
            paragraphs.append("")
    if current:
        paragraphs.append(" ".join(current))
    return "\n".join(paragraphs)


def _parse_frontmatter(content: str) -> tuple[dict[str, Any], str]:
    lines = content.splitlines()
    if not lines or lines[0] != "---":
        raise ValueError("SKILL.md must start with YAML frontmatter (---)")
    try:
        closing = lines.index("---", 1)
    except ValueError as exc:
        raise ValueError("SKILL.md frontmatter is not closed with ---") from exc

    frontmatter = lines[1:closing]
    result: dict[str, Any] = {}
    index = 0
    while index < len(frontmatter):
        raw = frontmatter[index]
        line_number = index + 2
        if "\t" in raw:
            raise ValueError(f"line {line_number}: tabs are not allowed in frontmatter")
        if not raw.strip() or raw.lstrip().startswith("#"):
            index += 1
            continue
        if raw != raw.lstrip():
            raise ValueError(f"line {line_number}: unexpected indentation")
        match = _FIELD_PATTERN.fullmatch(raw)
        if match is None:
            raise ValueError(f"line {line_number}: expected a top-level key and scalar value")
        key, raw_value = match.group(1), match.group(2) or ""
        if key in result:
            raise ValueError(f"line {line_number}: duplicate field {key!r}")

        marker = _strip_inline_comment(raw_value).strip()
        if marker in {"|", "|-", "|+", ">", ">-", ">+"}:
            style = marker[0]
            block: list[str] = []
            index += 1
            while index < len(frontmatter):
                nested = frontmatter[index]
                if nested and nested == nested.lstrip():
                    break
                if "\t" in nested:
                    raise ValueError(f"line {index + 2}: tabs are not allowed in frontmatter")
                block.append(nested[2:] if nested.startswith("  ") else "")
                index += 1
            result[key] = _fold_block(block, style).rstrip("\n")
            continue

        if key == "metadata" and not marker:
            metadata: dict[str, str] = {}
            index += 1
            while index < len(frontmatter):
                nested = frontmatter[index]
                nested_number = index + 2
                if not nested.strip() or nested.lstrip().startswith("#"):
                    index += 1
                    continue
                if nested == nested.lstrip():
                    break
                if "\t" in nested or not nested.startswith("  ") or nested.startswith("   "):
                    raise ValueError(f"line {nested_number}: metadata keys require two spaces")
                nested_match = _FIELD_PATTERN.fullmatch(nested[2:])
                if nested_match is None:
                    raise ValueError(f"line {nested_number}: invalid metadata entry")
                nested_key, nested_value = nested_match.group(1), nested_match.group(2) or ""
                if nested_key in metadata:
                    raise ValueError(
                        f"line {nested_number}: duplicate metadata field {nested_key!r}"
                    )
                metadata[nested_key] = _parse_scalar(nested_value, line_number=nested_number)
                index += 1
            result[key] = metadata
            continue

        result[key] = _parse_scalar(raw_value, line_number=line_number)
        index += 1

    body = "\n".join(lines[closing + 1 :]).strip()
    return result, body


def _read_text_file(path: Path, maximum: int) -> str:
    try:
        details = path.stat()
    except OSError as exc:
        raise ValueError(f"cannot stat {path.name}: {exc}") from exc
    if not stat.S_ISREG(details.st_mode):
        raise ValueError(f"{path.name} must be a regular file")
    if details.st_size > maximum:
        raise ValueError(f"{path.name} exceeds the {maximum}-byte size limit")
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"{path.name} must be valid UTF-8") from exc
    except OSError as exc:
        raise ValueError(f"cannot read {path.name}: {exc}") from exc


def _find_skill_md(skill_dir: Path) -> Path | None:
    for filename in ("SKILL.md", "skill.md"):
        candidate = skill_dir / filename
        if candidate.exists():
            return candidate
    return None


def _validate_metadata(
    metadata: Mapping[str, Any], skill_dir: Path
) -> tuple[list[SkillValidationIssue], dict[str, Any]]:
    issues: list[SkillValidationIssue] = []
    normalized = dict(metadata)
    extras = set(metadata) - _ALLOWED_FIELDS
    if extras:
        issues.append(
            _issue(
                skill_dir,
                "unexpected-fields",
                "unexpected frontmatter fields: " + ", ".join(sorted(extras)),
            )
        )

    name = metadata.get("name")
    if not isinstance(name, str) or not name.strip():
        issues.append(_issue(skill_dir, "invalid-name", "name must be a non-empty string"))
    else:
        name = unicodedata.normalize("NFKC", name.strip())
        normalized["name"] = name
        if len(name) > 64:
            issues.append(_issue(skill_dir, "invalid-name", "name exceeds 64 characters"))
        if name != name.lower():
            issues.append(_issue(skill_dir, "invalid-name", "name must be lowercase"))
        if name.startswith("-") or name.endswith("-"):
            issues.append(
                _issue(skill_dir, "invalid-name", "name cannot start or end with '-'")
            )
        if "--" in name:
            issues.append(_issue(skill_dir, "invalid-name", "name cannot contain '--'"))
        if not all(character.isalnum() or character == "-" for character in name):
            issues.append(
                _issue(
                    skill_dir, "invalid-name", "name may contain only letters, digits, and '-'"
                )
            )
        directory_name = unicodedata.normalize("NFKC", skill_dir.name)
        if name != directory_name:
            issues.append(
                _issue(skill_dir, "name-mismatch", "name must match the parent directory name")
            )

    description = metadata.get("description")
    if not isinstance(description, str) or not description.strip():
        issues.append(
            _issue(skill_dir, "invalid-description", "description must be a non-empty string")
        )
    else:
        description = description.strip()
        normalized["description"] = description
        if len(description) > 1024:
            issues.append(
                _issue(skill_dir, "invalid-description", "description exceeds 1024 characters")
            )

    for key in ("license", "allowed-tools"):
        if key in metadata and not isinstance(metadata[key], str):
            issues.append(_issue(skill_dir, f"invalid-{key}", f"{key} must be a string"))
    compatibility = metadata.get("compatibility")
    if compatibility is not None:
        if not isinstance(compatibility, str) or not compatibility.strip():
            issues.append(
                _issue(
                    skill_dir,
                    "invalid-compatibility",
                    "compatibility must be a non-empty string",
                )
            )
        elif len(compatibility) > 500:
            issues.append(
                _issue(
                    skill_dir, "invalid-compatibility", "compatibility exceeds 500 characters"
                )
            )
    extra_metadata = metadata.get("metadata", {})
    if not isinstance(extra_metadata, Mapping) or any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in getattr(extra_metadata, "items", lambda: ())()
    ):
        issues.append(
            _issue(skill_dir, "invalid-metadata", "metadata must map strings to strings")
        )
    return issues, normalized


def _inspect_skill(
    skill_dir: Path, *, max_skill_bytes: int
) -> tuple[SkillSpec | None, tuple[SkillValidationIssue, ...]]:
    issues: list[SkillValidationIssue] = []
    try:
        directory_details = skill_dir.lstat()
    except OSError as exc:
        return None, (_issue(skill_dir, "unreadable", str(exc)),)
    if _is_reparse_point(directory_details):
        return None, (_issue(skill_dir, "unsafe-link", "skill directories cannot be links"),)
    if not stat.S_ISDIR(directory_details.st_mode):
        return None, (_issue(skill_dir, "not-directory", "skill path must be a directory"),)
    skill_md = _find_skill_md(skill_dir)
    if skill_md is None:
        return None, (_issue(skill_dir, "missing-skill-md", "missing SKILL.md"),)
    try:
        file_details = skill_md.lstat()
        if _is_reparse_point(file_details):
            raise ValueError("SKILL.md cannot be a link or reparse point")
        content = _read_text_file(skill_md, max_skill_bytes)
        metadata, body = _parse_frontmatter(content)
    except (OSError, ValueError) as exc:
        return None, (_issue(skill_md, "invalid-skill-md", str(exc)),)
    metadata_issues, normalized = _validate_metadata(metadata, skill_dir)
    issues.extend(metadata_issues)
    if issues:
        return None, tuple(issues)
    spec = SkillSpec(
        metadata=SkillMetadata(normalized["name"], normalized["description"]),
        body=body,
        content=content,
        license=normalized.get("license"),
        compatibility=normalized.get("compatibility"),
        allowed_tools=normalized.get("allowed-tools"),
        extra_metadata=dict(normalized.get("metadata", {})),
    )
    return spec, ()


def validate_skill(
    skill_dir: str | os.PathLike[str], *, max_skill_bytes: int = MAX_SKILL_BYTES
) -> tuple[SkillValidationIssue, ...]:
    """Validate one Agent Skill directory without loading external dependencies."""

    if max_skill_bytes <= 0:
        raise ValueError("max_skill_bytes must be positive")
    _, issues = _inspect_skill(Path(skill_dir), max_skill_bytes=max_skill_bytes)
    return issues


class FilesystemSkillSource:
    """Discover Agent Skills from an explicitly authorized filesystem root."""

    def __init__(
        self,
        root: str | os.PathLike[str],
        *,
        max_skill_bytes: int = MAX_SKILL_BYTES,
        max_resource_bytes: int = MAX_RESOURCE_BYTES,
    ) -> None:
        if max_skill_bytes <= 0 or max_resource_bytes <= 0:
            raise ValueError("skill and resource size limits must be positive")
        self.root = Path(root)
        self.max_skill_bytes = max_skill_bytes
        self.max_resource_bytes = max_resource_bytes
        self._skills: dict[str, tuple[Path, SkillMetadata]] = {}

    def _candidate_directories(self) -> tuple[Path, ...]:
        if not self.root.exists():
            raise SkillValidationError(
                (_issue(self.root, "missing-root", "skill source root does not exist"),)
            )
        try:
            root_details = self.root.lstat()
        except OSError as exc:
            raise SkillValidationError((_issue(self.root, "unreadable", str(exc)),)) from exc
        if _is_reparse_point(root_details):
            raise SkillValidationError(
                (_issue(self.root, "unsafe-link", "skill source root cannot be a link"),)
            )
        if not stat.S_ISDIR(root_details.st_mode):
            raise SkillValidationError(
                (_issue(self.root, "not-directory", "skill source root must be a directory"),)
            )
        direct = _find_skill_md(self.root)
        if direct is not None:
            return (self.root,)
        try:
            entries = sorted(self.root.iterdir(), key=lambda item: item.name)
        except OSError as exc:
            raise SkillValidationError((_issue(self.root, "unreadable", str(exc)),)) from exc
        candidates: list[Path] = []
        issues: list[SkillValidationIssue] = []
        for entry in entries:
            try:
                details = entry.lstat()
            except OSError as exc:
                issues.append(_issue(entry, "unreadable", str(exc)))
                continue
            if _is_reparse_point(details):
                issues.append(_issue(entry, "unsafe-link", "skill roots cannot contain links"))
            elif stat.S_ISDIR(details.st_mode) and _find_skill_md(entry) is not None:
                candidates.append(entry)
        if issues:
            raise SkillValidationError(issues)
        return tuple(candidates)

    def discover(self) -> tuple[SkillMetadata, ...]:
        found: dict[str, tuple[Path, SkillMetadata]] = {}
        issues: list[SkillValidationIssue] = []
        for candidate in self._candidate_directories():
            spec, candidate_issues = _inspect_skill(
                candidate, max_skill_bytes=self.max_skill_bytes
            )
            issues.extend(candidate_issues)
            if spec is None:
                continue
            if spec.name in found:
                issues.append(
                    _issue(candidate, "duplicate-skill", f"duplicate skill name {spec.name!r}")
                )
                continue
            found[spec.name] = (candidate, spec.metadata)
        if issues:
            raise SkillValidationError(issues)
        self._skills = found
        return tuple(found[name][1] for name in sorted(found))

    def _skill_directory(self, name: str) -> Path:
        if not self._skills:
            self.discover()
        entry = self._skills.get(name)
        if entry is None:
            raise SkillNotFoundError(f"unknown skill {name!r}")
        return entry[0]

    def load(self, name: str) -> SkillSpec:
        skill_dir = self._skill_directory(name)
        spec, issues = _inspect_skill(skill_dir, max_skill_bytes=self.max_skill_bytes)
        if issues or spec is None:
            raise SkillValidationError(issues)
        if spec.name != name:
            raise SkillValidationError(
                (_issue(skill_dir, "skill-changed", "skill name changed after discovery"),)
            )
        return spec

    @staticmethod
    def _safe_relative_path(value: str) -> PurePosixPath:
        if not isinstance(value, str) or not value or "\x00" in value:
            raise SkillResourceError("resource path must be a non-empty string without NUL")
        normalized = value.replace("\\", "/")
        if normalized.startswith(("/", "//")) or _DRIVE_PATTERN.match(normalized):
            raise SkillResourceError("resource path must be relative to the skill root")
        segments = normalized.split("/")
        path = PurePosixPath(*segments)
        if (
            not segments
            or segments[0] not in _RESOURCE_DIRECTORIES
            or any(part in {"", ".", ".."} or ":" in part for part in segments)
        ):
            raise SkillResourceError(
                "resources must be safe relative paths under assets/, references/, or scripts/"
            )
        return path

    @staticmethod
    def _assert_safe_chain(skill_dir: Path, candidate: Path) -> None:
        current = skill_dir
        try:
            if _is_reparse_point(current.lstat()):
                raise SkillResourceError("skill directory cannot be a link or reparse point")
            for part in candidate.relative_to(skill_dir).parts:
                current = current / part
                details = current.lstat()
                if _is_reparse_point(details):
                    raise SkillResourceError("resource path contains a link or reparse point")
        except ValueError as exc:
            raise SkillResourceError("resource path escapes the skill root") from exc
        except OSError as exc:
            raise SkillResourceError(f"resource is not readable: {exc}") from exc

    def read_resource(self, name: str, path: str) -> SkillResource:
        skill_dir = self._skill_directory(name)
        relative = self._safe_relative_path(path)
        candidate = skill_dir.joinpath(*relative.parts)
        self._assert_safe_chain(skill_dir, candidate)
        try:
            root_resolved = skill_dir.resolve(strict=True)
            resolved = candidate.resolve(strict=True)
        except OSError as exc:
            raise SkillResourceError(f"resource is not readable: {exc}") from exc
        if not resolved.is_relative_to(root_resolved):
            raise SkillResourceError("resource path escapes the skill root")
        try:
            details = resolved.stat()
        except OSError as exc:
            raise SkillResourceError(f"resource is not readable: {exc}") from exc
        if not stat.S_ISREG(details.st_mode):
            raise SkillResourceError("resource must be a regular file")
        if details.st_size > self.max_resource_bytes:
            raise SkillResourceError(
                f"resource exceeds the {self.max_resource_bytes}-byte size limit"
            )
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(resolved, flags)
            try:
                opened = os.fstat(descriptor)
                if not stat.S_ISREG(opened.st_mode):
                    raise SkillResourceError("resource must remain a regular file")
                if opened.st_size > self.max_resource_bytes:
                    raise SkillResourceError(
                        f"resource exceeds the {self.max_resource_bytes}-byte size limit"
                    )
                chunks: list[bytes] = []
                remaining = self.max_resource_bytes + 1
                while remaining > 0:
                    chunk = os.read(descriptor, min(remaining, 64 * 1024))
                    if not chunk:
                        break
                    chunks.append(chunk)
                    remaining -= len(chunk)
                content = b"".join(chunks)
            finally:
                os.close(descriptor)
        except SkillResourceError:
            raise
        except OSError as exc:
            raise SkillResourceError(f"resource is not readable: {exc}") from exc
        if len(content) > self.max_resource_bytes:
            raise SkillResourceError(
                f"resource exceeds the {self.max_resource_bytes}-byte size limit"
            )
        self._assert_safe_chain(skill_dir, candidate)
        if resolved != candidate.resolve(strict=True):
            raise SkillResourceError("resource target changed while it was being read")
        media_type = mimetypes.guess_type(relative.name)[0] or "application/octet-stream"
        return SkillResource(name, relative.as_posix(), media_type, content)


class SkillRegistry:
    """Strict registry that combines one or more provider-neutral skill sources."""

    def __init__(self, sources: Sequence[SkillSource]) -> None:
        self.sources = tuple(sources)
        self._source_by_name: dict[str, SkillSource] = {}
        self._metadata: tuple[SkillMetadata, ...] = ()
        self._discover()

    def _discover(self) -> None:
        metadata: dict[str, SkillMetadata] = {}
        source_by_name: dict[str, SkillSource] = {}
        issues: list[SkillValidationIssue] = []
        for source in self.sources:
            for skill in source.discover():
                if skill.name in metadata:
                    issues.append(
                        _issue(
                            skill.name,
                            "duplicate-skill",
                            f"duplicate skill name {skill.name!r}",
                        )
                    )
                    continue
                metadata[skill.name] = skill
                source_by_name[skill.name] = source
        if issues:
            raise SkillValidationError(issues)
        self._metadata = tuple(metadata[name] for name in sorted(metadata))
        self._source_by_name = source_by_name

    def discover(self) -> tuple[SkillMetadata, ...]:
        return self._metadata

    def load(self, name: str) -> SkillSpec:
        source = self._source_by_name.get(name)
        if source is None:
            raise SkillNotFoundError(f"unknown skill {name!r}")
        return source.load(name)

    def read_resource(self, name: str, path: str) -> SkillResource:
        source = self._source_by_name.get(name)
        if source is None:
            raise SkillNotFoundError(f"unknown skill {name!r}")
        return source.read_resource(name, path)

    def catalog_prompt(self, *, max_skills: int | None = None) -> str:
        metadata = self._metadata
        if max_skills is not None:
            if max_skills <= 0:
                raise ValueError("max_skills must be positive when configured")
            metadata = metadata[:max_skills]
        lines = [
            "The following Agent Skills are available. Use read_skill only when a skill is relevant, "
            "then use read_skill_resource only for a referenced resource you actually need.",
            "<available_skills>",
        ]
        for skill in metadata:
            lines.extend(
                [
                    "<skill>",
                    f"<name>{html.escape(skill.name)}</name>",
                    f"<description>{html.escape(skill.description)}</description>",
                    "</skill>",
                ]
            )
        if max_skills is not None and len(self._metadata) > max_skills:
            lines.extend(
                [
                    f"<additional_skill_count>{len(self._metadata) - max_skills}</additional_skill_count>",
                    "<additional_skill_names>"
                    + ", ".join(skill.name for skill in self._metadata[max_skills:])
                    + "</additional_skill_names>",
                    "Use read_skill with an exact additional skill name when it is relevant.",
                ]
            )
        lines.append("</available_skills>")
        return "\n".join(lines)

    def tool_specs(self) -> tuple[Any, Any]:
        from .tools import ToolSpec

        def read_skill(skill_name: str) -> str:
            """Load the complete SKILL.md for a relevant available Agent Skill."""

            return self.load(skill_name).content

        def read_skill_resource(skill_name: str, path: str) -> str:
            """Read one referenced file under a Skill's references, scripts, or assets directory."""

            resource = self.read_resource(skill_name, path)
            try:
                decoded = resource.content.decode("utf-8")
                encoding = "utf-8"
                content = decoded
            except UnicodeDecodeError:
                encoding = "base64"
                content = base64.b64encode(resource.content).decode("ascii")
            return json.dumps(
                {
                    "skill_name": resource.skill_name,
                    "path": resource.path,
                    "media_type": resource.media_type,
                    "encoding": encoding,
                    "size": resource.size,
                    "content": content,
                },
                ensure_ascii=False,
            )

        parameters_name = {
            "type": "object",
            "properties": {"skill_name": {"type": "string"}},
            "required": ["skill_name"],
            "additionalProperties": False,
        }
        parameters_resource = {
            "type": "object",
            "properties": {
                "skill_name": {"type": "string"},
                "path": {"type": "string"},
            },
            "required": ["skill_name", "path"],
            "additionalProperties": False,
        }
        return (
            ToolSpec(
                name="read_skill",
                description=read_skill.__doc__ or "",
                parameters=parameters_name,
                func=read_skill,
                timeout=30.0,
            ),
            ToolSpec(
                name="read_skill_resource",
                description=read_skill_resource.__doc__ or "",
                parameters=parameters_resource,
                func=read_skill_resource,
                timeout=30.0,
            ),
        )


SkillInput = (
    SkillRegistry
    | SkillSource
    | str
    | os.PathLike[str]
    | Sequence[SkillSource | str | os.PathLike[str]]
    | None
)


def as_skill_registry(value: SkillInput) -> SkillRegistry | None:
    """Normalize the public ``skills=`` forms accepted by prebuilt agents."""

    if value is None:
        return None
    if isinstance(value, SkillRegistry):
        return value
    if isinstance(value, (str, os.PathLike)):
        return SkillRegistry((FilesystemSkillSource(value),))
    if isinstance(value, SkillSource):
        return SkillRegistry((value,))
    if not isinstance(value, Sequence):
        raise TypeError("skills must be a registry, source, path, or sequence of sources/paths")
    sources: list[SkillSource] = []
    for item in value:
        if isinstance(item, (str, os.PathLike)):
            sources.append(FilesystemSkillSource(item))
        elif isinstance(item, SkillSource):
            sources.append(item)
        else:
            raise TypeError("skills sequences may contain only SkillSource objects or paths")
    return SkillRegistry(sources) if sources else None


__all__ = [
    "FilesystemSkillSource",
    "MAX_RESOURCE_BYTES",
    "MAX_SKILL_BYTES",
    "SkillError",
    "SkillInput",
    "SkillMetadata",
    "SkillNotFoundError",
    "SkillRegistry",
    "SkillResource",
    "SkillResourceError",
    "SkillSource",
    "SkillSpec",
    "SkillValidationError",
    "SkillValidationIssue",
    "as_skill_registry",
    "validate_skill",
]
