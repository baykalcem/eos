import asyncio
from dataclasses import dataclass
from typing import Any

import ray
from omegaconf import OmegaConf
from ray import ObjectRef

from eos.configuration.configuration_manager import ConfigurationManager
from eos.configuration.plugin_registries.task_plugin_registry import TaskPluginRegistry
from eos.containers.container_manager import ContainerManager
from eos.containers.entities.container import Container
from eos.devices.device_actor_references import DeviceRayActorReference, DeviceRayActorWrapperReferences
from eos.devices.device_manager import DeviceManager
from eos.logging.logger import log
from eos.resource_allocation.entities.resource_request import (
    ActiveResourceAllocationRequest,
    ResourceAllocationRequest,
    ResourceType,
    ResourceRequestAllocationStatus,
)
from eos.resource_allocation.exceptions import EosResourceRequestError
from eos.resource_allocation.resource_allocation_manager import ResourceAllocationManager
from eos.scheduling.entities.scheduled_task import ScheduledTask
from eos.tasks.base_task import BaseTask
from eos.tasks.entities.task import TaskStatus
from eos.tasks.entities.task_execution_parameters import TaskExecutionParameters
from eos.tasks.exceptions import (
    EosTaskResourceAllocationError,
    EosTaskExecutionError,
    EosTaskValidationError,
    EosTaskExistsError,
)
from eos.tasks.task_input_parameter_caster import TaskInputParameterCaster
from eos.tasks.task_manager import TaskManager
from eos.tasks.task_validator import TaskValidator


@dataclass
class TaskExecutionContext:
    experiment_id: str
    task_id: str
    task_ref: ObjectRef | None = None
    active_resource_request: ActiveResourceAllocationRequest = None


class TaskExecutor:
    def __init__(
        self,
        task_manager: TaskManager,
        device_manager: DeviceManager,
        container_manager: ContainerManager,
        resource_allocation_manager: ResourceAllocationManager,
        configuration_manager: ConfigurationManager,
    ):
        self._task_manager = task_manager
        self._device_manager = device_manager
        self._container_manager = container_manager
        self._resource_allocation_manager = resource_allocation_manager
        self._configuration_manager = configuration_manager
        self._task_plugin_registry = TaskPluginRegistry()
        self._task_validator = TaskValidator()
        self._task_input_parameter_caster = TaskInputParameterCaster()

        self._active_tasks: dict[str, TaskExecutionContext] = {}

        log.debug("Task executor initialized.")

    async def request_task_execution(
        self, task_parameters: TaskExecutionParameters, scheduled_task: ScheduledTask | None = None
    ) -> BaseTask.OutputType | None:
        context = TaskExecutionContext(task_parameters.experiment_id, task_parameters.task_config.id)
        self._active_tasks[context.task_id] = context

        try:
            containers = self._prepare_containers(task_parameters)
            await self._initialize_task(task_parameters, containers)

            self._task_validator.validate(task_parameters.task_config)

            context.active_resource_request = (
                scheduled_task.allocated_resources
                if scheduled_task
                else await self._allocate_resources(task_parameters)
            )

            context.task_ref = self._execute_task(task_parameters, containers)
            return await context.task_ref
        except EosTaskExistsError as e:
            raise EosTaskExecutionError(
                f"Error executing task '{context.task_id}' in experiment '{context.experiment_id}'"
            ) from e
        except EosTaskValidationError as e:
            self._task_manager.fail_task(context.experiment_id, context.task_id)
            log.warning(f"EXP '{context.experiment_id}' - Failed task '{context.task_id}'.")
            raise EosTaskValidationError(
                f"Validation error for task '{context.task_id}' in experiment '{context.experiment_id}'"
            ) from e
        except EosTaskResourceAllocationError as e:
            self._task_manager.fail_task(context.experiment_id, context.task_id)
            log.warning(f"EXP '{context.experiment_id}' - Failed task '{context.task_id}'.")
            raise EosTaskResourceAllocationError(
                f"Failed to allocate resources for task '{context.task_id}' in experiment '{context.experiment_id}'"
            ) from e
        except Exception as e:
            self._task_manager.fail_task(context.experiment_id, context.task_id)
            log.warning(f"EXP '{context.experiment_id}' - Failed task '{context.task_id}'.")
            raise EosTaskExecutionError(
                f"Error executing task '{context.task_id}' in experiment '{context.experiment_id}'"
            ) from e
        finally:
            if context.active_resource_request and not scheduled_task:
                self._release_resources(context.active_resource_request)

            if context.task_id in self._active_tasks:
                del self._active_tasks[context.task_id]

    async def request_task_cancellation(self, experiment_id: str, task_id: str) -> None:
        context = self._active_tasks.get(task_id)
        if not context:
            return

        if context.task_ref:
            ray.cancel(context.task_ref, recursive=True)

        if context.active_resource_request:
            self._resource_allocation_manager.abort_active_request(context.active_resource_request.id)
            self._resource_allocation_manager.process_active_requests()

        self._task_manager.cancel_task(experiment_id, task_id)
        log.warning(f"EXP '{experiment_id}' - Cancelled task '{task_id}'.")
        del self._active_tasks[task_id]

    def _prepare_containers(self, execution_parameters: TaskExecutionParameters) -> dict[str, Container]:
        return {
            container_name: self._container_manager.get_container(container_id)
            for container_name, container_id in execution_parameters.task_config.containers.items()
        }

    async def _initialize_task(
        self, execution_parameters: TaskExecutionParameters, containers: dict[str, Container]
    ) -> None:
        experiment_id, task_id = execution_parameters.experiment_id, execution_parameters.task_config.id
        log.debug(f"Execution of task '{task_id}' for experiment '{experiment_id}' has been requested")

        task = self._task_manager.get_task(experiment_id, task_id)
        if task and task.status == TaskStatus.RUNNING:
            log.warning(f"Found running task '{task_id}' for experiment '{experiment_id}'. Restarting it.")
            await self.request_task_cancellation(experiment_id, task_id)
            self._task_manager.delete_task(experiment_id, task_id)

        self._task_manager.create_task(
            experiment_id=experiment_id,
            task_id=task_id,
            task_type=execution_parameters.task_config.type,
            devices=execution_parameters.task_config.devices,
            parameters=execution_parameters.task_config.parameters,
            containers=containers,
        )

    async def _allocate_resources(
        self, execution_parameters: TaskExecutionParameters
    ) -> ActiveResourceAllocationRequest:
        resource_request = self._create_resource_request(execution_parameters)
        return await self._request_resources(resource_request, execution_parameters.resource_allocation_timeout)

    def _get_device_actor_references(self, task_parameters: TaskExecutionParameters) -> list[DeviceRayActorReference]:
        return [
            DeviceRayActorReference(
                id=device.id,
                lab_id=device.lab_id,
                type=self._configuration_manager.labs[device.lab_id].devices[device.id].type,
                actor_handle=self._device_manager.get_device_actor(device.lab_id, device.id),
            )
            for device in task_parameters.task_config.devices
        ]

    def _execute_task(
        self,
        task_execution_parameters: TaskExecutionParameters,
        containers: dict[str, Container],
    ) -> ObjectRef:
        experiment_id, task_id = task_execution_parameters.experiment_id, task_execution_parameters.task_config.id
        device_actor_references = self._get_device_actor_references(task_execution_parameters)
        task_class_type = self._task_plugin_registry.get_task_class_type(task_execution_parameters.task_config.type)
        parameters = task_execution_parameters.task_config.parameters
        if not isinstance(parameters, dict):
            parameters = OmegaConf.to_object(parameters)

        parameters = self._task_input_parameter_caster.cast_input_parameters(
            task_id, task_execution_parameters.task_config.type, parameters
        )

        @ray.remote(num_cpus=0)
        def _ray_execute_task(
            _experiment_id: str,
            _task_id: str,
            _devices_actor_references: list[DeviceRayActorReference],
            _parameters: dict[str, Any],
            _containers: dict[str, Container],
        ) -> tuple:
            task = task_class_type(_experiment_id, _task_id)
            devices = DeviceRayActorWrapperReferences(_devices_actor_references)
            return task.execute(devices, _parameters, _containers)

        self._task_manager.start_task(experiment_id, task_id)
        log.info(f"EXP '{experiment_id}' - Started task '{task_id}'.")

        return _ray_execute_task.options(name=f"{experiment_id}.{task_id}").remote(
            experiment_id,
            task_id,
            device_actor_references,
            parameters,
            containers,
        )

    @staticmethod
    def _create_resource_request(
        task_parameters: TaskExecutionParameters,
    ) -> ResourceAllocationRequest:
        task_id, experiment_id = task_parameters.task_config.id, task_parameters.experiment_id
        resource_allocation_priority = task_parameters.resource_allocation_priority

        request = ResourceAllocationRequest(
            requester=task_id,
            experiment_id=experiment_id,
            reason=f"Resources required for task '{task_id}'",
            priority=resource_allocation_priority,
        )

        for device in task_parameters.task_config.devices:
            request.add_resource(device.id, device.lab_id, ResourceType.DEVICE)

        for container_id in task_parameters.task_config.containers.values():
            request.add_resource(container_id, "", ResourceType.CONTAINER)

        return request

    async def _request_resources(
        self, resource_request: ResourceAllocationRequest, timeout: int = 30
    ) -> ActiveResourceAllocationRequest:
        allocation_event = asyncio.Event()
        active_request = None

        def resource_request_callback(request: ActiveResourceAllocationRequest) -> None:
            nonlocal active_request
            active_request = request
            allocation_event.set()

        active_resource_request = self._resource_allocation_manager.request_resources(
            resource_request, resource_request_callback
        )

        if active_resource_request.status == ResourceRequestAllocationStatus.ALLOCATED:
            return active_resource_request

        self._resource_allocation_manager.process_active_requests()

        try:
            await asyncio.wait_for(allocation_event.wait(), timeout)
        except asyncio.TimeoutError as e:
            self._resource_allocation_manager.abort_active_request(active_resource_request.id)
            raise EosTaskResourceAllocationError(
                f"Resource allocation timed out after {timeout} seconds for task '{resource_request.requester}'. "
                f"Aborting all resource allocations for this task."
            ) from e

        if not active_request:
            raise EosTaskResourceAllocationError(f"Error allocating resources for task '{resource_request.requester}'")

        return active_request

    def _release_resources(self, active_request: ActiveResourceAllocationRequest) -> None:
        try:
            self._resource_allocation_manager.release_resources(active_request)
            self._resource_allocation_manager.process_active_requests()
        except EosResourceRequestError as e:
            raise EosTaskExecutionError(f"Error releasing task '{active_request.request.requester}' resources") from e
