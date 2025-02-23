from ortools.sat.python import cp_model
import asyncio

from eos.configuration.configuration_manager import ConfigurationManager
from eos.configuration.entities.task import TaskDeviceConfig, TaskConfig
from eos.configuration.experiment_graph.experiment_graph import ExperimentGraph
from eos.devices.device_manager import DeviceManager
from eos.devices.entities.device import DeviceStatus
from eos.experiments.entities.experiment import Experiment
from eos.experiments.experiment_manager import ExperimentManager
from eos.logging.logger import log
from eos.database.abstract_sql_db_interface import AsyncDbSession
from eos.resource_allocation.entities.resource_request import (
    ActiveResourceAllocationRequest,
    ResourceAllocationRequest,
    ResourceType,
    ResourceRequestAllocationStatus,
)
from eos.resource_allocation.exceptions import EosResourceRequestError
from eos.resource_allocation.resource_allocation_manager import ResourceAllocationManager
from eos.scheduling.abstract_scheduler import AbstractScheduler
from eos.scheduling.entities.scheduled_task import ScheduledTask
from eos.scheduling.exceptions import EosSchedulerRegistrationError
from eos.tasks.task_input_resolver import TaskInputResolver
from eos.tasks.task_manager import TaskManager
from eos.utils.di.di_container import inject_all

class CPSATScheduler(AbstractScheduler):
    @inject_all
    def __init__(
        self,
        configuration_manager: ConfigurationManager,
        experiment_manager: ExperimentManager,
        task_manager: TaskManager,
        device_manager: DeviceManager,
        resource_allocation_manager: ResourceAllocationManager,
    ):
        self._configuration_manager = configuration_manager
        self._experiment_manager = experiment_manager
        self._task_manager = task_manager
        self._device_manager = device_manager
        self._resource_allocation_manager = resource_allocation_manager

        self._registered_experiments = {}
        self._lock = asyncio.Lock()

        log.debug("CP-SAT scheduler initialized.")

    async def register_experiment(
        self, experiment_id: str, experiment_type: str, experiment_graph: ExperimentGraph
    ) -> None:
        async with self._lock:
            if experiment_type not in self._configuration_manager.experiments:
                raise EosSchedulerRegistrationError(
                    f"Cannot register experiment '{experiment_type}', it does not exist."
                )
            self._registered_experiments[experiment_id] = (experiment_type, experiment_graph)
            log.debug("Experiment '%s' registered for scheduling.", experiment_id)

    async def unregister_experiment(self, db: AsyncDbSession, experiment_id: str) -> None:
        """
        Unregister an experiment from the scheduler. The scheduler will no longer consider this experiment when tasks
        are requested.
        """
        async with self._lock:
            if experiment_id in self._registered_experiments:
                del self._registered_experiments[experiment_id]
                await self._release_experiment_resources(db, experiment_id)
            else:
                raise EosSchedulerRegistrationError(
                    f"Cannot unregister experiment {experiment_id} from the scheduler as it is not registered."
                )


    async def request_tasks(self, db: AsyncDbSession, experiment_id: str) -> list[ScheduledTask]:
        async with self._lock:
            if experiment_id not in self._registered_experiments:
                raise EosSchedulerRegistrationError(
                    f"Cannot request tasks for unregistered experiment {experiment_id}."
                )
            experiment_type, experiment_graph = self._registered_experiments[experiment_id]

            model = cp_model.CpModel()
            task_vars = {}
            start_times = {}
            end_times = {}
            all_tasks = experiment_graph.get_topologically_sorted_tasks()
            completed_tasks = await self._experiment_manager.get_completed_tasks(db, experiment_id)
            pending_tasks = [task_id for task_id in all_tasks if task_id not in completed_tasks]

            horizon = len(pending_tasks) * 10  # Upper bound on makespan

            for task_id in pending_tasks:
                start_times[task_id] = model.NewIntVar(0, horizon, f"start_{task_id}")
                duration = 1  # Assume each task takes 1 unit time (adjust if needed)
                end_times[task_id] = model.NewIntVar(0, horizon, f"end_{task_id}")
                model.Add(end_times[task_id] == start_times[task_id] + duration)
                task_vars[task_id] = model.NewBoolVar(f"task_{task_id}")

                for dep in experiment_graph.get_task_dependencies(task_id):
                    if dep in end_times:
                        model.Add(start_times[task_id] >= end_times[dep])

            makespan = model.NewIntVar(0, horizon, "makespan")
            model.AddMaxEquality(makespan, [end_times[task] for task in pending_tasks])
            model.Minimize(makespan)

            solver = cp_model.CpSolver()
            status = solver.Solve(model)

            scheduled_tasks = []
            if status == cp_model.FEASIBLE or status == cp_model.OPTIMAL:
                for task_id in pending_tasks:
                    if solver.Value(task_vars[task_id]) == 1:
                        scheduled_tasks.append(ScheduledTask(id=task_id, experiment_id=experiment_id))

            return scheduled_tasks

    @staticmethod
    def _create_resource_request(
        task_id: str,
        task_config: TaskConfig,
        experiment: Experiment,
    ) -> ResourceAllocationRequest | None:
        """
        Create a single resource allocation request for all devices and containers required by a task.
        Returns None if no resources are required.
        """
        # Skip resource allocation if no resources are needed
        if not task_config.devices and not task_config.containers:
            return None

        request = ResourceAllocationRequest(
            requester=task_id,
            experiment_id=experiment.id,
            priority=experiment.priority,
            reason=f"Resources required for task '{task_id}'",
        )

        for device in task_config.devices:
            request.add_resource(device.id, device.lab_id, ResourceType.DEVICE)

        for container_id in task_config.containers.values():
            request.add_resource(container_id, "", ResourceType.CONTAINER)

        return request

    async def _release_task_resources(self, db: AsyncDbSession, experiment_id: str, task_id: str) -> None:
        active_request = self._allocated_resources[experiment_id].pop(task_id, None)
        if active_request:
            try:
                await self._resource_allocation_manager.release_resources(db, active_request)
            except EosResourceRequestError as e:
                log.error(f"Error releasing resources for task '{task_id}' in experiment '{experiment_id}': {e}")

    async def _release_experiment_resources(self, db: AsyncDbSession, experiment_id: str) -> None:
        task_ids = list(self._allocated_resources.get(experiment_id, {}).keys())
        for task_id in task_ids:
            await self._release_task_resources(db, experiment_id, task_id)

        if experiment_id in self._allocated_resources:
            del self._allocated_resources[experiment_id]

    @staticmethod
    def _check_task_dependencies_met(
        task_id: str, completed_tasks: set[str], experiment_graph: ExperimentGraph
    ) -> bool:
        """
        Return True if all dependencies of a task have been completed, False otherwise.
        """
        dependencies = experiment_graph.get_task_dependencies(task_id)
        return all(dep in completed_tasks for dep in dependencies)

    async def _check_device_available(
        self, db: AsyncDbSession, task_config: TaskConfig, experiment_id: str, task_device: TaskDeviceConfig
    ) -> bool:
        """
        Check if a device is available for a task. A device is available if:
        1. It is active
        2. It is either not allocated, or is allocated to the requesting task
        """
        device = await self._device_manager.get_device(db, task_device.lab_id, task_device.id)
        if device.status == DeviceStatus.INACTIVE:
            log.warning(
                f"Device {task_device.id} in lab {task_device.lab_id} is inactive but is requested by task "
                f"{task_device.id}."
            )
            return False

        allocation = await self._device_allocation_manager.get_allocation(db, task_device.lab_id, task_device.id)
        if not allocation:
            return True

        # Check if device is allocated to this task
        return allocation.owner == task_config.id and allocation.experiment_id == experiment_id

    async def _check_container_available(
        self, db: AsyncDbSession, task_config: TaskConfig, experiment_id: str, container_id: str
    ) -> bool:
        """
        Check if a container is available for a task. A container is available if:
        1. It is not allocated, or
        2. It is allocated to the requesting task
        """
        allocation = await self._container_allocator.get_allocation(db, container_id)
        if not allocation:
            return True

        # Check if container is allocated to this task
        return allocation.owner == task_config.id and allocation.experiment_id == experiment_id

    async def is_experiment_completed(self, db: AsyncDbSession, experiment_id: str) -> bool:
        """
        Check if an experiment has been completed. The scheduler should consider the completed tasks from the task
        manager to determine if the experiment has been completed.
        """
        if experiment_id not in self._registered_experiments:
            raise EosSchedulerRegistrationError(
                f"Cannot check if experiment {experiment_id} is completed as it is not registered."
            )

        experiment_type, experiment_graph = self._registered_experiments[experiment_id]
        all_tasks = experiment_graph.get_task_graph().nodes
        completed_tasks = await self._experiment_manager.get_completed_tasks(db, experiment_id)

        return all(task in completed_tasks for task in all_tasks)

#TODO: Test and tune current implementation