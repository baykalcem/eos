from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, field_serializer


class ExperimentStatus(Enum):
    CREATED = "CREATED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    SUSPENDED = "SUSPENDED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class ExperimentExecutionParameters(BaseModel):
    resume: bool = False


class Experiment(BaseModel):
    id: str
    type: str

    execution_parameters: ExperimentExecutionParameters

    status: ExperimentStatus = ExperimentStatus.CREATED

    labs: list[str] = []

    running_tasks: list[str] = []
    completed_tasks: list[str] = []

    dynamic_parameters: dict[str, dict[str, Any]] = {}

    metadata: dict[str, Any] = {}

    start_time: datetime | None = None
    end_time: datetime | None = None

    created_at: datetime = datetime.now(tz=timezone.utc)

    class Config:
        arbitrary_types_allowed = True

    @field_serializer("status")
    def status_enum_to_string(self, v: ExperimentStatus) -> str:
        return v.value
