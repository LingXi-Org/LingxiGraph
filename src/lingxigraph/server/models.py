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
    #: Set atomically by the owning worker once graph execution has ended
    #: and before it starts the final steering flush + finalization commit
    #: (issue #16 PR #17 review round 6, point 3). Once set, ``/steer``
    #: must refuse new admission -- there is no safe point left for the
    #: graph to ever consume a newly-accepted event.
    steering_closed: bool = False
    #: Set atomically by the resume transaction that creates a descendant
    #: run from this (paused) run (issue #16 PR #17 review round 14,
    #: BLOCKER). This is authoritative control-plane lineage state -- it
    #: MUST NOT live in user-writable ``metadata``, since a caller could
    #: otherwise forge it at run-creation time to falsely trigger
    #: ``409 run_superseded`` / ``409 run_resume_conflict`` on a run that
    #: was never actually resumed. ``/steer``, ``/cancel``, and the
    #: concurrent-resume conflict gate all read this field, never
    #: ``metadata.get("superseded_by_run_id")``.
    superseded_by_run_id: str | None = None
    created_at: datetime = Field(default_factory=utcnow)
    started_at: datetime | None = None
    finished_at: datetime | None = None


class SteerCreate(ApiModel):
    kind: str = "user_input"
    payload: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = None


class SteerAccepted(ApiModel):
    id: str
    run_id: str
    sequence: int
    status: Literal["pending", "delivered", "consumed", "superseded"]
    kind: str
    created_at: datetime = Field(default_factory=utcnow)


class RunSteeringEvent(ApiModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    tenant_id: str
    run_id: str
    sequence: int
    kind: str
    payload: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = None
    status: Literal["pending", "delivered", "consumed", "superseded"] = "pending"
    created_at: datetime = Field(default_factory=utcnow)
    consumed_at: datetime | None = None
    #: Stable logical identity across a paused-run -> resumed-run transfer
    #: (see ``Repository.resume_run_with_pending_steering`` / issue #16 PR
    #: #17 review point 3). ``None`` for an event that was never
    #: transferred. When set, this is the ``id`` of the original
    #: ``superseded`` row the client's ``/steer`` call actually received --
    #: external callers correlate "the id I got back" to "the id that was
    #: eventually consumed" via this field rather than the (new,
    #: post-transfer) ``id``. ``created_at`` on the transferred row is also
    #: preserved from the original so ``queue_latency_seconds`` includes
    #: the time an event spent waiting while the run was paused.
    source_event_id: str | None = None


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
    "RunSteeringEvent",
    "SteerAccepted",
    "SteerCreate",
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
