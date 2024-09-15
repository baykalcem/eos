from datetime import datetime, timezone
from enum import Enum
from typing import Any, ClassVar

from omegaconf import ListConfig, DictConfig, OmegaConf
from pydantic import BaseModel, field_serializer

from eos.configuration.entities.task import TaskDeviceConfig
from eos.containers.entities.container import Container


class TaskStatus(Enum):
  CREATED = "CREATED"
  RUNNING = "RUNNING"
  COMPLETED = "COMPLETED"
  FAILED = "FAILED"
  CANCELLED = "CANCELLED"


class TaskContainer(BaseModel):
  id: str


class TaskInput(BaseModel):
  parameters: dict[str, Any] | None = None
  containers: dict[str, Container] | None = None

  class Config:
    arbitrary_types_allowed = True

  @field_serializer("parameters")
  def serialize_parameters(self, parameters: dict[str, Any] | None, _info) -> Any:
    if parameters is None:
      return None
    return omegaconf_serializer(parameters)


class TaskOutput(BaseModel):
  parameters: dict[str, Any] | None = None
  containers: dict[str, Container] | None = None
  file_names: list[str] | None = None

  @field_serializer("parameters")
  def serialize_parameters(self, parameters: dict[str, Any] | None, _info) -> Any:
    if parameters is None:
      return None
    return omegaconf_serializer(parameters)


def omegaconf_serializer(obj: Any) -> Any:
  if isinstance(obj, ListConfig | DictConfig):
    return OmegaConf.to_object(obj)
  if isinstance(obj, dict):
    return {k: omegaconf_serializer(v) for k, v in obj.items()}
  if isinstance(obj, list):
    return [omegaconf_serializer(v) for v in obj]
  return obj


class Task(BaseModel):
  id: str
  type: str
  experiment_id: str

  devices: list[TaskDeviceConfig] = []
  input: TaskInput = TaskInput()
  output: TaskOutput = TaskInput()

  status: TaskStatus = TaskStatus.CREATED

  metadata: dict[str, Any] = {}
  start_time: datetime | None = None
  end_time: datetime | None = None

  created_at: datetime = datetime.now(tz=timezone.utc)

  class Config:
    arbitrary_types_allowed = True
    json_encoders: ClassVar = {
      ListConfig: lambda v: omegaconf_serializer(v),
      DictConfig: lambda v: omegaconf_serializer(v),
    }

  @field_serializer("status")
  def status_enum_to_string(self, v: TaskStatus) -> str:
    return v.value
