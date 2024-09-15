from dataclasses import dataclass
from typing import Any


@dataclass
class DeviceSpecification:
    type: str
    description: str | None = None
    initialization_parameters: dict[str, Any] | None = None
