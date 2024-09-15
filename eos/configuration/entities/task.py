from dataclasses import dataclass, field
from typing import Any


@dataclass
class TaskDeviceConfig:
    lab_id: str
    id: str


@dataclass
class TaskConfig:
    id: str
    type: str
    devices: list[TaskDeviceConfig] = field(default_factory=list)
    containers: dict[str, str] = field(default_factory=dict)
    parameters: dict[str, Any] = field(default_factory=dict)
    dependencies: list[str] = field(default_factory=list)

    max_duration_seconds: int | None = None
    description: str | None = None
