import copy
from typing import Any

from omegaconf import ListConfig, OmegaConf, DictConfig

from eos.configuration.entities.parameters import ParameterType, ParameterFactory
from eos.configuration.entities.task import TaskConfig
from eos.configuration.entities.task_specification import TaskSpecification
from eos.configuration.exceptions import EosConfigurationError
from eos.configuration.validation import validation_utils
from eos.logging.batch_error_logger import batch_error, raise_batched_errors
from eos.tasks.exceptions import EosTaskValidationError


class TaskInputParameterValidator:
    """
    Validates that the input parameters of a task conform to the task's specification.
    """

    def __init__(self, task: TaskConfig, task_spec: TaskSpecification):
        self._task_id = task.id
        self._input_parameters = task.parameters
        self._task_spec = task_spec

    def validate_input_parameters(self) -> None:
        """
        Validate the input parameters of a task.
        Ensure that all required parameters are provided and that the provided parameters conform to the task's
        specification.
        """
        for parameter_name in self._input_parameters:
            self._validate_parameter_in_task_spec(parameter_name)
        raise_batched_errors(root_exception_type=EosTaskValidationError)

        self._validate_all_required_parameters_provided()

        for parameter_name, parameter in self._input_parameters.items():
            self._validate_parameter(parameter_name, parameter)
        raise_batched_errors(root_exception_type=EosTaskValidationError)

    def _validate_parameter_in_task_spec(self, parameter_name: str) -> None:
        """
        Check that the parameter exists in the task specification.
        """
        if parameter_name not in self._task_spec.input_parameters:
            batch_error(
                f"Parameter '{parameter_name}' in task '{self._task_id}' is invalid. "
                f"Expected a parameter found in the task specification.",
                EosTaskValidationError,
            )

    def _validate_parameter(self, parameter_name: str, parameter: Any) -> None:
        """
        Validate a parameter according to the task specification. Expect that the parameter is concrete.
        """
        if validation_utils.is_dynamic_parameter(parameter):
            batch_error(
                f"Input parameter '{parameter_name}' in task '{self._task_id}' is 'eos_dynamic', which is not "
                f"allowed.",
                EosTaskValidationError,
            )
        else:
            self._validate_parameter_spec(parameter_name, parameter)

    def _validate_parameter_spec(self, parameter_name: str, parameter: Any) -> None:
        """
        Validate a parameter to make sure it conforms to its task specification.
        """
        parameter_spec = copy.deepcopy(self._task_spec.input_parameters[parameter_name])

        try:
            parameter = self._convert_value_type(parameter, ParameterType(parameter_spec.type))
        except Exception:
            batch_error(
                f"Parameter '{parameter_name}' in task '{self._task_id}' has incorrect type {type(parameter)}. "
                f"Expected type: '{parameter_spec.type}'.",
                EosTaskValidationError,
            )
            return

        parameter_spec["value"] = parameter

        try:
            parameter_type = ParameterType(parameter_spec.type)
            ParameterFactory.create_parameter(parameter_type, **parameter_spec)
        except EosConfigurationError as e:
            batch_error(
                f"Parameter '{parameter_name}' in task '{self._task_id}' validation error: {e}",
                EosTaskValidationError,
            )

    @staticmethod
    def _convert_value_type(value: Any, expected_type: ParameterType) -> Any:
        result = None

        if isinstance(value, expected_type.python_type()):
            result = value
        elif isinstance(value, ListConfig | DictConfig):
            value = OmegaConf.to_object(value)

        if result is None:
            conversion_map = {
                ParameterType.integer: int,
                ParameterType.decimal: float,
                ParameterType.string: str,
                ParameterType.choice: str,
            }

            if expected_type in conversion_map:
                result = conversion_map[expected_type](value)
            elif expected_type == ParameterType.boolean:
                if isinstance(value, bool):
                    result = value
                elif isinstance(value, str):
                    v = value.strip().lower()
                    if v == "true":
                        result = True
                    elif v == "false":
                        result = False
            elif expected_type == ParameterType.list and isinstance(value, list):
                result = list(value)
            elif expected_type == ParameterType.dictionary and isinstance(value, dict):
                result = value

        if result is None:
            raise ValueError(f"Cannot convert {value} to {expected_type}")

        return result

    def _validate_all_required_parameters_provided(self) -> None:
        """
        Validate that all required parameters are provided in the parameter dictionary.
        """
        missing_parameters = self._get_missing_required_task_parameters()

        if missing_parameters:
            raise EosTaskValidationError(
                f"Task '{self._task_id}' is missing required input parameters: {missing_parameters}"
            )

    def _get_missing_required_task_parameters(self) -> list[str]:
        """
        Get all the missing required parameters in the parameter dictionary.
        """
        required_parameters = self._get_required_input_parameters()
        return [
            parameter_name for parameter_name in required_parameters if parameter_name not in self._input_parameters
        ]

    def _get_required_input_parameters(self) -> list[str]:
        """
        Get all the required input parameters for the task.
        """
        return [param for param, spec in self._task_spec.input_parameters.items() if "value" not in spec]
