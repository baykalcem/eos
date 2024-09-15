from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, field_serializer, Field, model_validator


class CampaignExecutionParameters(BaseModel):
    max_experiments: int = Field(0, ge=0)
    max_concurrent_experiments: int = Field(1, ge=1)

    do_optimization: bool
    optimizer_computer_ip: str = "127.0.0.1"
    dynamic_parameters: list[dict[str, dict[str, Any]]] | None = None

    resume: bool = False

    @model_validator(mode="after")
    def validate_dynamic_parameters(self) -> None:
        if not self.do_optimization:
            if not self.dynamic_parameters:
                raise ValueError("Campaign dynamic parameters must be provided if optimization is not enabled.")
            if len(self.dynamic_parameters) != self.max_experiments:
                raise ValueError(
                    "Dynamic parameters must be provided for all experiments up to the max experiments if "
                    "optimization is not enabled."
                )
        return self


class CampaignStatus(Enum):
    CREATED = "CREATED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    SUSPENDED = "SUSPENDED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class Campaign(BaseModel):
    id: str
    experiment_type: str

    execution_parameters: CampaignExecutionParameters

    status: CampaignStatus = CampaignStatus.CREATED
    experiments_completed: int = Field(0, ge=0)
    current_experiment_ids: list[str] = []

    pareto_solutions: list[dict[str, Any]] | None = None

    metadata: dict[str, Any] = {}

    start_time: datetime | None = None
    end_time: datetime | None = None

    created_at: datetime = datetime.now(tz=timezone.utc)

    @field_serializer("status")
    def status_enum_to_string(self, v: CampaignStatus) -> str:
        return v.value


class CampaignSample(BaseModel):
    campaign_id: str
    experiment_id: str

    inputs: dict[str, Any]
    outputs: dict[str, Any]

    created_at: datetime = datetime.now(tz=timezone.utc)
