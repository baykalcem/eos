import copy
import functools
from typing import Protocol

from eos.configuration.entities.task import TaskConfig
from eos.configuration.validation import validation_utils
from eos.experiments.experiment_manager import ExperimentManager
from eos.tasks.exceptions import EosTaskInputResolutionError
from eos.tasks.task_manager import TaskManager


class Resolver(Protocol):
    def __call__(self, experiment_id: str, task_config: TaskConfig) -> TaskConfig: ...


class TaskInputResolver:
    """
    Resolves dynamic parameters, input parameter references, and input container references for a task that is
    part of an experiment.
    """

    def __init__(self, task_manager: TaskManager, experiment_manager: ExperimentManager):
        self._task_manager = task_manager
        self._experiment_manager = experiment_manager

    def resolve_task_inputs(self, experiment_id: str, task_config: TaskConfig) -> TaskConfig:
        """
        Resolve all input references for a task.
        """
        return self._apply_resolvers(
            experiment_id,
            task_config,
            [
                self._resolve_dynamic_parameters,
                self._resolve_input_parameter_references,
                self._resolve_input_container_references,
            ],
        )

    def resolve_dynamic_parameters(self, experiment_id: str, task_config: TaskConfig) -> TaskConfig:
        """
        Resolve dynamic parameters for a task.
        """
        return self._apply_resolvers(experiment_id, task_config, [self._resolve_dynamic_parameters])

    def resolve_input_parameter_references(self, experiment_id: str, task_config: TaskConfig) -> TaskConfig:
        """
        Resolve input parameter references for a task.
        """
        return self._apply_resolvers(experiment_id, task_config, [self._resolve_input_parameter_references])

    def resolve_input_container_references(self, experiment_id: str, task_config: TaskConfig) -> TaskConfig:
        """
        Resolve input container references for a task.
        """
        return self._apply_resolvers(experiment_id, task_config, [self._resolve_input_container_references])

    def _apply_resolvers(self, experiment_id: str, task_config: TaskConfig, resolvers: list[Resolver]) -> TaskConfig:
        """
        Apply a list of resolver functions to the task config.
        """
        return functools.reduce(
            lambda config, resolver: resolver(experiment_id, config), resolvers, copy.deepcopy(task_config)
        )

    def _resolve_dynamic_parameters(self, experiment_id: str, task_config: TaskConfig) -> TaskConfig:
        experiment = self._experiment_manager.get_experiment(experiment_id)
        task_dynamic_parameters = experiment.dynamic_parameters.get(task_config.id, {})

        task_config.parameters.update(task_dynamic_parameters)

        unresolved_parameters = [
            param for param, value in task_config.parameters.items() if validation_utils.is_dynamic_parameter(value)
        ]

        if unresolved_parameters:
            raise EosTaskInputResolutionError(
                f"Unresolved input dynamic parameters in task '{task_config.id}': {unresolved_parameters}"
            )

        return task_config

    def _resolve_input_parameter_references(self, experiment_id: str, task_config: TaskConfig) -> TaskConfig:
        for param_name, param_value in task_config.parameters.items():
            if not validation_utils.is_parameter_reference(param_value):
                continue

            ref_task_id, ref_param_name = param_value.split(".")
            resolved_value = self._resolve_reference(experiment_id, ref_task_id, ref_param_name, "parameter")

            if resolved_value is not None:
                task_config.parameters[param_name] = resolved_value
            else:
                raise EosTaskInputResolutionError(
                    f"Unresolved input parameter reference '{param_value}' in task '{task_config.id}'"
                )

        return task_config

    def _resolve_input_container_references(self, experiment_id: str, task_config: TaskConfig) -> TaskConfig:
        for container_name, container_id in task_config.containers.items():
            if not validation_utils.is_container_reference(container_id):
                continue

            ref_task_id, ref_container_name = container_id.split(".")
            resolved_value = self._resolve_reference(experiment_id, ref_task_id, ref_container_name, "container")

            if resolved_value is not None:
                task_config.containers[container_name] = resolved_value
            else:
                raise EosTaskInputResolutionError(
                    f"Unresolved input container reference '{container_id}' in task '{task_config.id}'"
                )

        return task_config

    def _resolve_reference(self, experiment_id: str, ref_task_id: str, ref_name: str, ref_type: str) -> str | None:
        ref_task_output = self._task_manager.get_task_output(experiment_id, ref_task_id)

        if ref_type == "parameter":
            if ref_name in (ref_task_output.parameters or {}):
                return ref_task_output.parameters[ref_name]
            ref_task = self._task_manager.get_task(experiment_id, ref_task_id)
            if ref_name in (ref_task.input.parameters or {}):
                return ref_task.input.parameters[ref_name]
        elif ref_type == "container":
            if ref_name in (ref_task_output.containers or {}):
                return ref_task_output.containers[ref_name].id

        return None
