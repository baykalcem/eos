from typing import Any

from eos.configuration.entities.parameters import ParameterType
from eos.configuration.exceptions import EosTaskValidationError
from eos.configuration.spec_registries.task_specification_registry import TaskSpecificationRegistry


class TaskInputParameterCaster:
    def __init__(self):
        self.task_spec_registry = TaskSpecificationRegistry()

    def cast_input_parameters(self, task_id: str, task_type: str, input_parameters: dict[str, Any]) -> dict[str, Any]:
        """
        Cast input parameters of a task to the expected Python types.

        :param task_id: The ID of the task.
        :param task_type: The type of the task.
        :param input_parameters: The input parameters of the task.
        :return: The input parameters cast to the expected Python types.
        """
        task_spec = self.task_spec_registry.get_spec_by_type(task_type)

        for parameter_name, parameter in input_parameters.items():
            try:
                parameter_type = ParameterType(task_spec.input_parameters[parameter_name].type)
                input_parameters[parameter_name] = parameter_type.python_type()(parameter)
            except TypeError as e:
                raise EosTaskValidationError(
                    f"Failed to cast input parameter '{parameter_name}' of task '{task_id}' of type \
                    f'{type(parameter)}' to the expected type '{task_spec.input_parameters[parameter_name].type}'."
                ) from e

        return input_parameters
