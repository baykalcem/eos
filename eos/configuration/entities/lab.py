from dataclasses import dataclass, field
from typing import Any


@dataclass
class Location:
    description: str
    metadata: dict[str, Any] | None = None


@dataclass
class LabComputerConfig:
    ip: str
    description: str | None = None


@dataclass
class LabDeviceConfig:
    type: str
    computer: str
    location: str | None = None
    description: str | None = None
    initialization_parameters: dict[str, Any] | None = None


@dataclass
class LabContainerConfig:
    type: str
    location: str
    ids: list[str]
    description: str | None = None
    metadata: dict[str, Any] | None = None


@dataclass
class LabConfig:
    type: str
    description: str
    devices: dict[str, LabDeviceConfig]
    locations: dict[str, Location] = field(default_factory=dict)
    computers: dict[str, LabComputerConfig] = field(default_factory=dict)
    containers: list[LabContainerConfig] = field(default_factory=list)
