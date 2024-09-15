from pydantic import BaseModel, Field

from eos.configuration.entities.task import TaskConfig


class TaskExecutionParameters(BaseModel):
    experiment_id: str
    task_config: TaskConfig
    resource_allocation_priority: int = Field(120, ge=0)
    resource_allocation_timeout: int = Field(30, ge=0)
