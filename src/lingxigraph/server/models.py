"""Versioned API and persistence models for Agent Server."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

from ..types import Durability, MultitaskStrategy, RunStatus


def utcnow() -> datetime:
    return datetime.now(UTC)


def enum_value(value: Any) -> Any:
    """Return the stable wire value for an enum or normalized scalar."""

    return getattr(value, "value", value)


class ApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid", use_enum_values=True)


class GraphInfo(ApiModel):
    id: str
    version: str
    schema_hash: str
    input_schema: Mapping[str, Any]
    output_schema: Mapping[str, Any]
    context_schema: Mapping[str, Any] | None = None


class AssistantCreate(ApiModel):
    graph_id: str
    graph_version: str | None = None
    name: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    context: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class AssistantPatch(ApiModel):
    name: str | None = None
    config: dict[str, Any] | None = None
    context: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None


class Assistant(ApiModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    tenant_id: str
    graph_id: str
    graph_version: str
    name: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    context: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class ThreadCreate(ApiModel):
    metadata: dict[str, Any] = Field(default_factory=dict)


class ThreadPatch(ApiModel):
    metadata: dict[str, Any]


class Thread(ApiModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    tenant_id: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class RunCreate(ApiModel):
    assistant_id: str
    input: dict[str, Any] | None = None
    context: dict[str, Any] = Field(default_factory=dict)
    config: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    resume: Any | None = None
    update: dict[str, Any] | None = None
    goto: str | None = None
    durability: Durability = Durability.SYNC
    multitask_strategy: MultitaskStrategy = MultitaskStrategy.ENQUEUE
    run_timeout: float | None = None
    max_model_calls: int | None = Field(default=None, gt=0)
    max_tool_calls: int | None = Field(default=None, gt=0)
    max_tokens: int | None = Field(default=None, gt=0)
    max_cost: float | None = Field(default=None, gt=0)


class Run(ApiModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    tenant_id: str
    thread_id: str | None = None
    assistant_id: str
    graph_id: str
    graph_version: str
    idempotency_key: str | None = None
    request_digest: str | None = Field(default=None, exclude=True)
    status: RunStatus = RunStatus.PENDING
    input: dict[str, Any] | None = None
    context: dict[str, Any] = Field(default_factory=dict)
    config: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    resume: Any | None = None
    update: dict[str, Any] | None = None
    goto: str | None = None
    durability: Durability = Durability.SYNC
    error: dict[str, Any] | None = None
    output: dict[str, Any] | None = None
    attempt: int = 0
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    created_at: datetime = Field(default_factory=utcnow)
    started_at: datetime | None = None
    finished_at: datetime | None = None


class RunEvent(ApiModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    tenant_id: str
    run_id: str
    sequence: int
    kind: str
    data: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=utcnow)


class ScheduleCreate(ApiModel):
    assistant_id: str
    cron: str
    timezone: str = "UTC"
    input: dict[str, Any] = Field(default_factory=dict)
    enabled: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)


class SchedulePatch(ApiModel):
    cron: str | None = None
    timezone: str | None = None
    input: dict[str, Any] | None = None
    enabled: bool | None = None
    metadata: dict[str, Any] | None = None


class Schedule(ScheduleCreate):
    id: str = Field(default_factory=lambda: str(uuid4()))
    tenant_id: str
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class AuditRecord(ApiModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    tenant_id: str
    actor: str
    action: str
    resource_type: str
    resource_id: str | None = None
    result: Literal["allowed", "denied", "success", "failure"] = "success"
    trace_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=utcnow)


class Problem(ApiModel):
    type: str = "about:blank"
    title: str
    status: int
    detail: str
    code: str
    request_id: str
    retryable: bool = False


class StoreBatchRequest(ApiModel):
    operations: list[dict[str, Any]]


__all__ = [
    "Assistant",
    "AssistantCreate",
    "AssistantPatch",
    "AuditRecord",
    "GraphInfo",
    "Problem",
    "Run",
    "RunCreate",
    "RunEvent",
    "Schedule",
    "ScheduleCreate",
    "SchedulePatch",
    "StoreBatchRequest",
    "Thread",
    "ThreadCreate",
    "ThreadPatch",
    "utcnow",
    "enum_value",
]
