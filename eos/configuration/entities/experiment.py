from dataclasses import dataclass
from typing import Any

from eos.configuration.entities.task import TaskConfig


@dataclass
class ExperimentContainerConfig:
    id: str
    description: str | None = None
    metadata: dict[str, Any] | None = None
    tags: list[str] | None = None


@dataclass
class ExperimentConfig:
    type: str
    description: str
    labs: list[str]
    tasks: list[TaskConfig]
    containers: list[ExperimentContainerConfig] | None = None
