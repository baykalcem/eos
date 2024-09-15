from collections.abc import AsyncIterable
from datetime import datetime, timezone
from typing import Any

from eos.configuration.configuration_manager import ConfigurationManager
from eos.configuration.entities.task import TaskDeviceConfig
from eos.containers.entities.container import Container
from eos.experiments.repositories.experiment_repository import ExperimentRepository
from eos.logging.logger import log
from eos.persistence.db_manager import DbManager
from eos.persistence.file_db_manager import FileDbManager
from eos.tasks.entities.task import Task, TaskStatus, TaskInput, TaskOutput
from eos.tasks.exceptions import EosTaskStateError, EosTaskExistsError
from eos.tasks.repositories.task_repository import TaskRepository


class TaskManager:
    """
    Manages the state of all tasks in EOS.
    """

    def __init__(
        self,
        configuration_manager: ConfigurationManager,
        db_manager: DbManager,
        file_db_manager: FileDbManager,
    ):
        self._configuration_manager = configuration_manager
        self._db_manager = db_manager
        self._file_db_manager = file_db_manager
        self._tasks = TaskRepository("tasks", db_manager)
        self._tasks.create_indices([("experiment_id", 1), ("id", 1)], unique=True)
        self._experiments = ExperimentRepository("experiments", db_manager)

        log.debug("Task manager initialized.")

    def create_task(
        self,
        experiment_id: str,
        task_id: str,
        task_type: str,
        devices: list[TaskDeviceConfig],
        parameters: dict[str, Any] | None = None,
        containers: dict[str, Container] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Create a new task instance for a specific task type that is associated with an experiment.

        :param experiment_id: The id of the experiment.
        :param task_id: The id of the task in the experiment task sequence.
        :param task_type: The type of the task as defined in the configuration.
        :param devices: The devices required for the task.
        :param parameters: The input parameters for the task.
        :param containers: The input containers for the task.
        :param metadata: Additional metadata to be stored with the task.
        """
        if self._tasks.get_one(experiment_id=experiment_id, id=task_id):
            raise EosTaskExistsError(f"Cannot create task '{task_id}' as a task with that ID already exists.")

        task_spec = self._configuration_manager.task_specs.get_spec_by_type(task_type)
        if not task_spec:
            raise EosTaskStateError(f"Task type '{task_type}' does not exist.")

        task_input = TaskInput(parameters=parameters or {}, containers=containers or {})

        task = Task(
            id=task_id,
            type=task_type,
            experiment_id=experiment_id,
            devices=[TaskDeviceConfig(id=device.id, lab_id=device.lab_id) for device in devices],
            input=task_input,
            metadata=metadata or {},
        )
        self._tasks.create(task.model_dump())

    def delete_task(self, experiment_id: str, task_id: str) -> None:
        """
        Delete an experiment task instance.
        """
        self._validate_task_exists(experiment_id, task_id)

        self._tasks.delete(experiment_id=experiment_id, id=task_id)

        self._experiments.delete_running_task(experiment_id, task_id)
        log.info(f"Deleted task '{task_id}' from experiment '{experiment_id}'.")

    def start_task(self, experiment_id: str, task_id: str) -> None:
        """
        Add a task to the running tasks list and update its status to running.
        """
        self._validate_task_exists(experiment_id, task_id)
        self._experiments.add_running_task(experiment_id, task_id)
        self._set_task_status(experiment_id, task_id, TaskStatus.RUNNING)

    def complete_task(self, experiment_id: str, task_id: str) -> None:
        """
        Remove a task from the running tasks list and add it to the completed tasks list.
        """
        self._validate_task_exists(experiment_id, task_id)
        self._experiments.move_task_queue(experiment_id, task_id, "running_tasks", "completed_tasks")
        self._set_task_status(experiment_id, task_id, TaskStatus.COMPLETED)

    def fail_task(self, experiment_id: str, task_id: str) -> None:
        """
        Remove a task from the running tasks list and do not add it to the executed tasks list. Update the task status
        to failed.
        """
        self._validate_task_exists(experiment_id, task_id)
        self._experiments.delete_running_task(experiment_id, task_id)
        self._set_task_status(experiment_id, task_id, TaskStatus.FAILED)

    def cancel_task(self, experiment_id: str, task_id: str) -> None:
        """
        Remove a task from the running tasks list and do not add it to the executed tasks list. Update the task status
        to cancelled.
        """
        self._validate_task_exists(experiment_id, task_id)
        self._experiments.delete_running_task(experiment_id, task_id)
        self._set_task_status(experiment_id, task_id, TaskStatus.CANCELLED)
        log.warning(f"EXP '{experiment_id}' - Cancelled task '{task_id}'.")

    def get_task(self, experiment_id: str, task_id: str) -> Task | None:
        """
        Get a task by its ID and experiment ID.
        """
        task = self._tasks.get_one(experiment_id=experiment_id, id=task_id)
        return Task(**task) if task else None

    def get_tasks(self, **query: dict[str, Any]) -> list[Task]:
        """
        Query tasks with arbitrary parameters.

        :param query: Dictionary of query parameters.
        """
        tasks = self._tasks.get_all(**query)
        return [Task(**task) for task in tasks]

    def add_task_output(self, experiment_id: str, task_id: str, task_output: TaskOutput) -> None:
        """
        Add the output of a task to the database.
        """
        self._tasks.update({"output": task_output.model_dump()}, experiment_id=experiment_id, id=task_id)

    def get_task_output(self, experiment_id: str, task_id: str) -> TaskOutput | None:
        """
        Get the output of a task by its ID and experiment ID.
        """
        result = self._tasks.get_one(experiment_id=experiment_id, id=task_id)
        if not result:
            return None

        task = Task(**result)
        if not task.output:
            return None

        return task.output

    def add_task_output_file(self, experiment_id: str, task_id: str, file_name: str, file_data: bytes) -> None:
        """
        Add a file output from a task to the file database.
        """
        path = f"{experiment_id}/{task_id}/{file_name}"
        self._file_db_manager.store_file(path, file_data)

    def get_task_output_file(self, experiment_id: str, task_id: str, file_name: str) -> bytes:
        """
        Get a file output from a task from the file database.
        """
        path = f"{experiment_id}/{task_id}/{file_name}"
        return self._file_db_manager.get_file(path)

    def stream_task_output_file(
        self, experiment_id: str, task_id: str, file_name: str, chunk_size: int = 3 * 1024 * 1024
    ) -> AsyncIterable[bytes]:
        """
        Stream a file output from a task from the file database.
        """
        path = f"{experiment_id}/{task_id}/{file_name}"
        return self._file_db_manager.stream_file(path, chunk_size)

    def list_task_output_files(self, experiment_id: str, task_id: str) -> list[str]:
        """
        List all file outputs from a task in the file database.
        """
        prefix = f"{experiment_id}/{task_id}/"
        return self._file_db_manager.list_files(prefix)

    def delete_task_output_file(self, experiment_id: str, task_id: str, file_name: str) -> None:
        """
        Delete a file output from a task in the file database.
        """
        path = f"{experiment_id}/{task_id}/{file_name}"
        self._file_db_manager.delete_file(path)

    def _set_task_status(self, experiment_id: str, task_id: str, new_status: TaskStatus) -> None:
        """
        Update the status of a task.
        """
        self._validate_task_exists(experiment_id, task_id)

        update_fields = {"status": new_status.value}
        if new_status == TaskStatus.RUNNING:
            update_fields["start_time"] = datetime.now(tz=timezone.utc)
        elif new_status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
            update_fields["end_time"] = datetime.now(tz=timezone.utc)

        self._tasks.update(update_fields, experiment_id=experiment_id, id=task_id)

    def _validate_task_exists(self, experiment_id: str, task_id: str) -> None:
        """
        Check if a task exists in an experiment.
        """
        if not self._tasks.exists(experiment_id=experiment_id, id=task_id):
            raise EosTaskStateError(f"Task '{task_id}' does not exist in experiment '{experiment_id}'.")
