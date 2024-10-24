from abc import ABC, abstractmethod
from typing import Any

from eos.containers.entities.container import Container
from eos.devices.device_actor_wrapper_registry import DeviceActorWrapperRegistry
from eos.tasks.exceptions import EosTaskExecutionError


class BaseTask(ABC):
    """Base class for all tasks in EOS."""

    DevicesType = dict[str, DeviceActorWrapperRegistry]
    ParametersType = dict[str, Any]
    ContainersType = dict[str, Container]
    FilesType = dict[str, bytes]
    OutputType = tuple[ParametersType, ContainersType, FilesType]
    MAX_OUTPUT_LENGTH = 3

    def __init__(self, experiment_id: str, task_id: str) -> None:
        self._experiment_id = experiment_id
        self._task_id = task_id

    async def execute(
        self, devices: DevicesType, parameters: ParametersType, containers: ContainersType
    ) -> OutputType | None:
        """Execute a task with the given input and return the output."""
        try:
            output = await self._execute(devices, parameters, containers)

            output_parameters, output_containers, output_files = ({}, {}, {})

            if output:
                output_parameters = output[0] if len(output) > 0 and output[0] is not None else {}
                output_containers = output[1] if len(output) > 1 and output[1] is not None else {}
                output_files = output[2] if len(output) == BaseTask.MAX_OUTPUT_LENGTH and output[2] is not None else {}

            if containers:
                output_containers = {**containers, **output_containers}

            return output_parameters, output_containers, output_files
        except Exception as e:
            raise EosTaskExecutionError(f"Error executing task {self._task_id}") from e

    @abstractmethod
    async def _execute(
        self, devices: DevicesType, parameters: ParametersType, containers: ContainersType
    ) -> OutputType | None:
        """Implementation for the execution of a task."""
