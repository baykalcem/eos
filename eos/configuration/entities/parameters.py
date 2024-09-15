from dataclasses import dataclass
from enum import Enum
from typing import Any, ClassVar

from omegaconf import ListConfig

from eos.configuration.exceptions import EosConfigurationError

AllowedParameterTypes = int | float | bool | str | list | dict


def is_dynamic_parameter(parameter: AllowedParameterTypes) -> bool:
  return isinstance(parameter, str) and parameter.lower() == "eos_dynamic"


class ParameterType(Enum):
  integer = "integer"
  decimal = "decimal"
  string = "string"
  boolean = "boolean"
  choice = "choice"
  list = "list"
  dictionary = "dictionary"

  def python_type(self) -> type:
    mapping = {
      "integer": int,
      "decimal": float,
      "string": str,
      "boolean": bool,
      "choice": str,
      "list": list,
      "dictionary": dict,
    }
    return mapping[self.value]


@dataclass(kw_only=True)
class Parameter:
  type: ParameterType
  description: str
  value: Any | None = None

  def __post_init__(self):
    self._validate_type()

  def _validate_type(self) -> None:
    try:
      self.type = ParameterType(self.type)
    except ValueError as e:
      raise EosConfigurationError(f"Invalid task parameter type '{self.type}'") from e


@dataclass(kw_only=True)
class NumericParameter(Parameter):
  unit: str
  min: int | float | None = None
  max: int | float | None = None

  def __post_init__(self):
    super().__post_init__()
    self._validate_unit()
    self._validate_min_max()
    self._validate_value_range()

  def _validate_unit(self) -> None:
    if not self.unit:
      raise EosConfigurationError("Task parameter type is numeric but no unit is specified.")

  def _validate_min_max(self) -> None:
    if self.min is not None and self.max is not None and self.min >= self.max:
      raise EosConfigurationError("Task parameter 'min' is greater than or equal to 'max'.")

  def _validate_value_range(self) -> None:
    if self.value is None or is_dynamic_parameter(self.value):
      return

    if not isinstance(self.value, int | float):
      raise EosConfigurationError("Task parameter value is not numerical.")
    if self.min is not None and self.value < self.min:
      raise EosConfigurationError("Task parameter value is less than 'min'.")
    if self.max is not None and self.value > self.max:
      raise EosConfigurationError("Task parameter value is greater than 'max'.")


@dataclass(kw_only=True)
class BooleanParameter(Parameter):
  def __post_init__(self):
    super().__post_init__()
    self._validate_value()

  def _validate_value(self) -> None:
    if not isinstance(self.value, bool) and not is_dynamic_parameter(self.value):
      raise EosConfigurationError(
        f"Task parameter value '{self.value}' is not true/false but the declared type is 'boolean'."
      )


@dataclass(kw_only=True)
class ChoiceParameter(Parameter):
  choices: list[str]

  def __post_init__(self):
    super().__post_init__()
    self._validate_choices()

  def _validate_choices(self) -> None:
    if not self.choices:
      raise EosConfigurationError("Task parameter choices are not specified when the type is 'choice'.")

    if (
      not self.value
      or len(self.value) == 0
      or self.value not in self.choices
      and not is_dynamic_parameter(self.value)
    ):
      raise EosConfigurationError(
        f"Task parameter value '{self.value}' is not one of the choices {self.choices}."
      )


@dataclass(kw_only=True)
class ListParameter(Parameter):
  element_type: ParameterType
  length: int | None = None
  min: list[int | float] | None = None
  max: list[int | float] | None = None

  def __post_init__(self):
    super().__post_init__()
    self._validate_element_type()
    self._validate_list_attributes()
    self._validate_elements_within_bounds()

  def _validate_element_type(self) -> None:
    if isinstance(self.element_type, str):
      try:
        self.element_type = ParameterType[self.element_type]
      except KeyError as e:
        raise EosConfigurationError(f"Invalid list parameter element type '{self.element_type}'") from e
    if self.element_type == ParameterType.list:
      raise EosConfigurationError("List parameter element type cannot be 'list'. Nested lists are not supported.")

  def _validate_list_attributes(self) -> None:
    for attr_name in ["value", "min", "max"]:
      attr_value = getattr(self, attr_name)
      if attr_value is None:
        continue

      if not isinstance(attr_value, list) and not isinstance(attr_value, ListConfig):
        raise EosConfigurationError(
          f"List parameter '{attr_name}' must be a list for 'list' type parameters.",
          EosConfigurationError,
        )
      if not all(isinstance(item, self.element_type.python_type()) for item in attr_value):
        raise EosConfigurationError(
          f"All elements of list parameter '{attr_name}' must be of the same type as specified "
          f"by 'element_type'."
        )
      if self.length is not None and len(attr_value) != self.length:
        raise EosConfigurationError(f"List parameter '{attr_name}' length must be {self.length}.")

  def _validate_elements_within_bounds(self) -> None:
    if self.value is None or is_dynamic_parameter(self.value) or self.min is None and self.max is None:
      return

    if self.length is None and (self.min is not None or self.max is not None):
      raise EosConfigurationError(
        "List parameter 'min' and 'max' can only be specified when 'length' is specified."
      )

    _min = self.min or [float("-inf")] * self.length
    _max = self.max or [float("inf")] * self.length
    for i, val in enumerate(self.value):
      if not _min[i] <= val <= _max[i]:
        raise EosConfigurationError(
          f"Element {i} of the list with value {val} is not within the the bounds [{_min[i]}, {_max[i]}]."
        )


@dataclass(kw_only=True)
class DictionaryParameter(Parameter):
  pass


class ParameterFactory:
  _TYPE_MAPPING: ClassVar = {
    ParameterType.integer: NumericParameter,
    ParameterType.decimal: NumericParameter,
    ParameterType.string: Parameter,
    ParameterType.boolean: BooleanParameter,
    ParameterType.choice: ChoiceParameter,
    ParameterType.list: ListParameter,
    ParameterType.dictionary: DictionaryParameter,
  }

  @staticmethod
  def create_parameter(parameter_type: ParameterType | str, **kwargs) -> Parameter:
    if isinstance(parameter_type, str):
      parameter_type = ParameterType(parameter_type)

    parameter_class = ParameterFactory._TYPE_MAPPING.get(parameter_type)
    if not parameter_class:
      raise EosConfigurationError(f"Unsupported parameter type: {parameter_type}")

    if "type" not in kwargs:
      kwargs["type"] = parameter_type

    return parameter_class(**kwargs)
