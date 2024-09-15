from datetime import datetime, timezone
from typing import Any

from eos.configuration.configuration_manager import ConfigurationManager
from eos.experiments.entities.experiment import Experiment, ExperimentStatus, ExperimentExecutionParameters
from eos.experiments.exceptions import EosExperimentStateError
from eos.experiments.repositories.experiment_repository import ExperimentRepository
from eos.logging.logger import log
from eos.persistence.db_manager import DbManager
from eos.tasks.entities.task import TaskStatus
from eos.tasks.repositories.task_repository import TaskRepository


class ExperimentManager:
    """
    Responsible for managing the state of all experiments in EOS and tracking their execution.
    """

    def __init__(self, configuration_manager: ConfigurationManager, db_manager: DbManager):
        self._configuration_manager = configuration_manager
        self._experiments = ExperimentRepository("experiments", db_manager)
        self._experiments.create_indices([("id", 1)], unique=True)
        self._tasks = TaskRepository("tasks", db_manager)

        log.debug("Experiment manager initialized.")

    def create_experiment(
        self,
        experiment_id: str,
        experiment_type: str,
        execution_parameters: ExperimentExecutionParameters | None = None,
        dynamic_parameters: dict[str, dict[str, Any]] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Create a new experiment of a given type with a unique id.

        :param experiment_id: A unique id for the experiment.
        :param experiment_type: The type of the experiment as defined in the configuration.
        :param dynamic_parameters: Dictionary of the dynamic parameters per task and their provided values.
        :param execution_parameters: Parameters for the execution of the experiment.
        :param metadata: Additional metadata to be stored with the experiment.
        """
        if self._experiments.get_one(id=experiment_id):
            raise EosExperimentStateError(f"Experiment '{experiment_id}' already exists.")

        experiment_config = self._configuration_manager.experiments.get(experiment_type)
        if not experiment_config:
            raise EosExperimentStateError(f"Experiment type '{experiment_type}' not found in the configuration.")

        labs = experiment_config.labs

        experiment = Experiment(
            id=experiment_id,
            type=experiment_type,
            execution_parameters=execution_parameters or ExperimentExecutionParameters(),
            labs=labs,
            dynamic_parameters=dynamic_parameters or {},
            metadata=metadata or {},
        )
        self._experiments.create(experiment.model_dump())

        log.info(f"Created experiment '{experiment_id}'.")

    def delete_experiment(self, experiment_id: str) -> None:
        """
        Delete an experiment.
        """
        if not self._experiments.exists(id=experiment_id):
            raise EosExperimentStateError(f"Experiment '{experiment_id}' does not exist.")

        self._experiments.delete(id=experiment_id)
        self._tasks.delete(experiment_id=experiment_id)

        log.info(f"Deleted experiment '{experiment_id}'.")

    def start_experiment(self, experiment_id: str) -> None:
        """
        Start an experiment.
        """
        self._set_experiment_status(experiment_id, ExperimentStatus.RUNNING)

    def complete_experiment(self, experiment_id: str) -> None:
        """
        Complete an experiment.
        """
        self._set_experiment_status(experiment_id, ExperimentStatus.COMPLETED)

    def cancel_experiment(self, experiment_id: str) -> None:
        """
        Cancel an experiment.
        """
        self._set_experiment_status(experiment_id, ExperimentStatus.CANCELLED)

    def suspend_experiment(self, experiment_id: str) -> None:
        """
        Suspend an experiment.
        """
        self._set_experiment_status(experiment_id, ExperimentStatus.SUSPENDED)

    def fail_experiment(self, experiment_id: str) -> None:
        """
        Fail an experiment.
        """
        self._set_experiment_status(experiment_id, ExperimentStatus.FAILED)

    def get_experiment(self, experiment_id: str) -> Experiment | None:
        """
        Get an experiment.
        """
        experiment = self._experiments.get_one(id=experiment_id)
        return Experiment(**experiment) if experiment else None

    def get_experiments(self, **query: dict[str, Any]) -> list[Experiment]:
        """
        Get experiments with a custom query.

        :param query: Dictionary of query parameters.
        """
        experiments = self._experiments.get_all(**query)
        return [Experiment(**experiment) for experiment in experiments]

    def get_lab_experiments(self, lab: str) -> list[Experiment]:
        """
        Get all experiments associated with a lab.
        """
        experiments = self._experiments.get_experiments_by_lab(lab)
        return [Experiment(**experiment) for experiment in experiments]

    def get_running_tasks(self, experiment_id: str | None) -> set[str]:
        """
        Get the list of currently running tasks constrained by experiment ID.
        """
        experiment = self._experiments.get_one(id=experiment_id)
        return set(experiment.get("running_tasks", {})) if experiment else {}

    def get_completed_tasks(self, experiment_id: str) -> set[str]:
        """
        Get the list of completed tasks constrained by experiment ID.
        """
        experiment = self._experiments.get_one(id=experiment_id)
        return set(experiment.get("completed_tasks", {})) if experiment else {}

    def delete_non_completed_tasks(self, experiment_id: str) -> None:
        """
        Delete all tasks that are not completed in the given experiment.
        """
        experiment = self.get_experiment(experiment_id)

        for task_id in experiment.running_tasks:
            self._tasks.delete(experiment_id=experiment_id, id=task_id)
        self._experiments.clear_running_tasks(experiment_id)

        self._tasks.delete(experiment_id=experiment_id, status=TaskStatus.FAILED.value)
        self._tasks.delete(experiment_id=experiment_id, status=TaskStatus.CANCELLED.value)

    def _set_experiment_status(self, experiment_id: str, new_status: ExperimentStatus) -> None:
        """
        Set the status of an experiment.
        """
        if not self._experiments.exists(id=experiment_id):
            raise EosExperimentStateError(f"Experiment '{experiment_id}' does not exist.")

        update_fields = {"status": new_status.value}
        if new_status == ExperimentStatus.RUNNING:
            update_fields["start_time"] = datetime.now(tz=timezone.utc)
        elif new_status in [
            ExperimentStatus.COMPLETED,
            ExperimentStatus.CANCELLED,
            ExperimentStatus.FAILED,
        ]:
            update_fields["end_time"] = datetime.now(tz=timezone.utc)

        self._experiments.update(update_fields, id=experiment_id)
