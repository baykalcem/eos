import asyncio
import traceback
from typing import Any

from eos.configuration.entities.task import TaskConfig
from eos.containers.container_manager import ContainerManager
from eos.containers.entities.container import Container
from eos.logging.logger import log
from eos.tasks.entities.task import TaskOutput
from eos.tasks.entities.task_execution_parameters import TaskExecutionParameters
from eos.tasks.exceptions import EosTaskExecutionError, EosTaskValidationError, EosTaskStateError
from eos.tasks.task_executor import TaskExecutor
from eos.tasks.task_manager import TaskManager


class OnDemandTaskExecutor:
    """
    Executor for on-demand tasks (not part of an experiment or campaign).
    """

    EXPERIMENT_ID = "on_demand"

    def __init__(self, task_executor: TaskExecutor, task_manager: TaskManager, container_manager: ContainerManager):
        self._task_executor = task_executor
        self._task_manager = task_manager
        self._container_manager = container_manager

        self._task_futures: dict[str, asyncio.Task] = {}

        log.debug("On-demand task executor initialized.")

    async def submit_task(
        self,
        task_config: TaskConfig,
        resource_allocation_priority: int = 90,
        resource_allocation_timeout: int = 3600,
    ) -> None:
        task_id = task_config.id
        task_execution_parameters = TaskExecutionParameters(
            experiment_id=self.EXPERIMENT_ID,
            task_config=task_config,
            resource_allocation_priority=resource_allocation_priority,
            resource_allocation_timeout=resource_allocation_timeout,
        )

        self._task_futures[task_id] = asyncio.create_task(
            self._task_executor.request_task_execution(task_execution_parameters)
        )
        log.info(f"Submitted on-demand task '{task_id}'.")

    async def cancel_task(self, task_id: str) -> None:
        if task_id not in self._task_futures:
            raise EosTaskExecutionError(f"Cannot cancel non-existent on-demand task '{task_id}'.")

        future = self._task_futures[task_id]
        future.cancel()
        await self._task_executor.request_task_cancellation(self.EXPERIMENT_ID, task_id)
        del self._task_futures[task_id]
        log.info(f"Cancelled on-demand task '{task_id}'.")

    async def process_tasks(self) -> None:
        completed_tasks = []

        for task_id, future in self._task_futures.items():
            if future.done():
                try:
                    output = await future
                    self._process_task_output(task_id, *output)
                except asyncio.CancelledError:
                    log.info(f"On-demand task '{task_id}' was cancelled.")
                except (EosTaskExecutionError, EosTaskValidationError, EosTaskStateError):
                    log.error(f"Failed on-demand task '{task_id}': {traceback.format_exc()}")
                finally:
                    completed_tasks.append(task_id)

        for task_id in completed_tasks:
            del self._task_futures[task_id]

    def _process_task_output(
        self,
        task_id: str,
        output_parameters: dict[str, Any],
        output_containers: dict[str, Container],
        output_files: dict[str, bytes],
    ) -> None:
        for container in output_containers.values():
            self._container_manager.update_container(container)

        task_output = TaskOutput(
            experiment_id=self.EXPERIMENT_ID,
            task_id=task_id,
            parameters=output_parameters,
            containers=output_containers,
            file_names=list(output_files.keys()),
        )

        for file_name, file_data in output_files.items():
            self._task_manager.add_task_output_file(self.EXPERIMENT_ID, task_id, file_name, file_data)

        self._task_manager.add_task_output(self.EXPERIMENT_ID, task_id, task_output)
        self._task_manager.complete_task(self.EXPERIMENT_ID, task_id)
        log.info(f"EXP '{self.EXPERIMENT_ID}' - Completed task '{task_id}'.")
