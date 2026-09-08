from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class DesiredState(str, Enum):
    IDLE = "IDLE"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"


class RuntimeState(str, Enum):
    STARTING = "STARTING"
    IDLE = "IDLE"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    ERROR = "ERROR"


class CollectorType(str, Enum):
    IMAGE_ROLLOUT = "image_rollout"
    HEALTH_ERRORS = "health_errors"
    CRASH_TRIAGE = "crash_triage"


class CollectorConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=2, max_length=80, pattern=r"^[a-z][a-z0-9_-]+$")
    type: CollectorType
    enabled: bool = True
    interval_seconds: int = Field(default=30, ge=3, le=3600)
    initial_delay_seconds: int = Field(default=0, ge=0, le=3600)
    emit_only_changes: bool = True
    options: dict[str, Any] = Field(default_factory=dict)


class FlowControlConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    control_poll_seconds: int = Field(default=10, ge=3, le=300)
    heartbeat_seconds: int = Field(default=30, ge=10, le=600)
    default_lease_seconds: int = Field(default=120, ge=30, le=3600)
    flow_reload_seconds: int = Field(default=30, ge=10, le=600)
    shared_cache_seconds: int = Field(default=3, ge=0, le=60)


class FlowSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    control: FlowControlConfig = Field(default_factory=FlowControlConfig)
    collectors: list[CollectorConfig] = Field(min_length=1, max_length=20)

    @field_validator("collectors")
    @classmethod
    def collector_ids_unique(cls, values: list[CollectorConfig]) -> list[CollectorConfig]:
        ids = [item.id for item in values]
        if len(ids) != len(set(ids)):
            raise ValueError("collector ids must be unique")
        return values


class FlowMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=2, max_length=100)


class FlowDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    apiVersion: str = "assistant.example.io/v1alpha1"
    kind: str = "PatchAgentFlow"
    metadata: FlowMetadata
    spec: FlowSpec

    @model_validator(mode="after")
    def fixed_document_kind(self) -> "FlowDocument":
        if self.apiVersion != "assistant.example.io/v1alpha1":
            raise ValueError("unsupported flow apiVersion")
        if self.kind != "PatchAgentFlow":
            raise ValueError("kind must be PatchAgentFlow")
        return self


class ProcessInstruction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    command_id: str
    desired_state: DesiredState
    process_id: str | None = None
    flow_name: str | None = None
    target_tag: str | None = Field(
        default=None,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$",
    )
    lease_seconds: int = Field(default=120, ge=30, le=3600)
    issued_at: datetime
    parameters: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def running_requires_process(self) -> "ProcessInstruction":
        if self.desired_state == DesiredState.RUNNING:
            if not self.process_id or not self.flow_name or not self.target_tag:
                raise ValueError(
                    "RUNNING instruction requires process_id, flow_name and target_tag"
                )
        return self


class AgentRegistration(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cluster_id: str
    environment: str = Field(pattern="^(test|beta|prod)$")
    namespace_pattern: str
    agent_version: str
    flow_names: list[str]
    capabilities: list[str]


class CollectorEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_type: str
    cluster_id: str
    environment: str
    process_id: str
    flow_name: str
    collector_id: str
    collector_type: CollectorType
    sequence: int = Field(ge=1)
    collected_at: datetime = Field(default_factory=utc_now)
    duration_ms: int = 0
    fingerprint: str
    changed: bool
    target_tag: str
    data: dict[str, Any] = Field(default_factory=dict)
    errors: list[str] = Field(default_factory=list)


class AckEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cluster_id: str
    command_id: str
    process_id: str | None = None
    accepted_state: DesiredState
    at: datetime = Field(default_factory=utc_now)
    message: str


class AgentPublicState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cluster_id: str
    environment: str
    state: RuntimeState
    process_id: str | None = None
    flow_name: str | None = None
    target_tag: str | None = None
    lease_expires_at: datetime | None = None
    last_master_contact_at: datetime | None = None
    last_error: str | None = None
    collector_status: dict[str, dict[str, Any]] = Field(default_factory=dict)
