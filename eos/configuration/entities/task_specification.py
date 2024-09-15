import re
from dataclasses import dataclass, field
from typing import Any

from eos.configuration.entities.parameters import (
    ParameterFactory,
    ParameterType,
)
from eos.configuration.exceptions import EosConfigurationError


@dataclass
class TaskSpecificationContainer:
    type: str

    def __post_init__(self):
        self._validate_type()

    def _validate_type(self) -> None:
        if not self.type.strip():
            raise EosConfigurationError("Container 'type' field must be specified.")


@dataclass
class TaskSpecificationOutputParameter:
    type: ParameterType
    description: str
    unit: str | None = None

    def __post_init__(self):
        self._validate_type()
        self._validate_unit_specified_if_type_numeric()
        self._validate_unit_not_specified_if_type_not_numeric()

    def _validate_type(self) -> None:
        try:
            self.type = ParameterType(self.type)
        except ValueError as e:
            raise EosConfigurationError(f"Invalid task output parameter type '{self.type}'") from e

    def _validate_unit_specified_if_type_numeric(self) -> None:
        if self.type not in [ParameterType.integer, ParameterType.decimal]:
            return
        if self.unit is None or self.unit.strip() == "":
            raise EosConfigurationError("Task output parameter type is numeric but no unit is specified.")

    def _validate_unit_not_specified_if_type_not_numeric(self) -> None:
        if self.type in [ParameterType.integer, ParameterType.decimal]:
            return
        if self.unit is not None:
            raise EosConfigurationError("Task output parameter type is not numeric but a unit is specified.")


@dataclass
class TaskSpecification:
    type: str
    description: str
    device_types: list[str] | None = None

    input_containers: dict[str, TaskSpecificationContainer] = field(default_factory=dict)
    input_parameters: dict[str, Any] = field(default_factory=dict)

    output_parameters: dict[str, TaskSpecificationOutputParameter] = field(default_factory=dict)
    output_containers: dict[str, TaskSpecificationContainer] = field(default_factory=dict)

    def __post_init__(self):
        if not self.output_containers:
            self.output_containers = self.input_containers.copy()

        self._validate_parameters()
        self._validate_parameter_names()
        self._validate_container_names()

    def _validate_parameters(self) -> None:
        for parameter in self.input_parameters.values():
            _ = ParameterFactory.create_parameter(ParameterType(parameter["type"]), **parameter)

    def _validate_parameter_names(self) -> None:
        valid_name_pattern = re.compile(r"^[a-zA-Z0-9_.]*$")

        for name in self.input_parameters:
            if not valid_name_pattern.match(name):
                raise EosConfigurationError(
                    f"Invalid task parameter name '{name}'. "
                    f"Only characters, numbers, dots, and underscores are allowed."
                )

        for name in self.output_parameters:
            if not valid_name_pattern.match(name):
                raise EosConfigurationError(
                    f"Invalid task parameter name '{name}'. "
                    f"Only characters, numbers, dots, and underscores are allowed."
                )

    def _validate_container_names(self) -> None:
        valid_name_pattern = re.compile(r"^[a-zA-Z0-9_.]*$")

        for name in self.input_containers:
            if not valid_name_pattern.match(name):
                raise EosConfigurationError(
                    f"Invalid task input container name '{name}'. "
                    f"Only characters, numbers, dots, and underscores are allowed."
                )

        for name in self.output_containers:
            if not valid_name_pattern.match(name):
                raise EosConfigurationError(
                    f"Invalid task output container name '{name}'. "
                    f"Only characters, numbers, dots, and underscores are allowed."
                )
