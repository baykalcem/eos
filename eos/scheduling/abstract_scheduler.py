from abc import ABC, abstractmethod

from eos.configuration.experiment_graph.experiment_graph import ExperimentGraph
from eos.scheduling.entities.scheduled_task import ScheduledTask


class AbstractScheduler(ABC):
    @abstractmethod
    def register_experiment(self, experiment_id: str, experiment_type: str, experiment_graph: ExperimentGraph) -> None:
        """
        Register an experiment with the scheduler.

        :param experiment_id: The ID of the experiment.
        :param experiment_type: The type of the experiment.
        :param experiment_graph: The task graph of the experiment's task sequence.
        """

    @abstractmethod
    def unregister_experiment(self, experiment_id: str) -> None:
        """
        Unregister an experiment from the scheduler.

        :param experiment_id: The ID of the experiment.
        """

    @abstractmethod
    async def request_tasks(self, experiment_id: str) -> list[ScheduledTask]:
        """
        Request the next tasks to be executed for a specific experiment.

        :param experiment_id: The ID of the experiment.
        :return: A list of tasks to be executed next. Returns an empty list if no new tasks are available.
        """

    @abstractmethod
    def is_experiment_completed(self, experiment_id: str) -> bool:
        """
        Check if an experiment has been completed.

        :param experiment_id: The ID of the experiment.
        :return: True if the experiment has been completed, False otherwise.
        """
