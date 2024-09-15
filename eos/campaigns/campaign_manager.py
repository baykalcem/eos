from datetime import datetime, timezone
from typing import Any

from eos.campaigns.entities.campaign import Campaign, CampaignStatus, CampaignExecutionParameters
from eos.campaigns.exceptions import EosCampaignStateError
from eos.campaigns.repositories.campaign_repository import CampaignRepository
from eos.configuration.configuration_manager import ConfigurationManager
from eos.experiments.entities.experiment import ExperimentStatus
from eos.experiments.repositories.experiment_repository import ExperimentRepository
from eos.logging.logger import log
from eos.persistence.db_manager import DbManager
from eos.tasks.repositories.task_repository import TaskRepository


class CampaignManager:
    """
    Responsible for managing the state of all experiment campaigns in EOS and tracking their execution.
    """

    def __init__(self, configuration_manager: ConfigurationManager, db_manager: DbManager):
        self._configuration_manager = configuration_manager
        self._campaigns = CampaignRepository("campaigns", db_manager)
        self._campaigns.create_indices([("id", 1)], unique=True)
        self._experiments = ExperimentRepository("experiments", db_manager)
        self._tasks = TaskRepository("tasks", db_manager)

        log.debug("Campaign manager initialized.")

    def create_campaign(
        self,
        campaign_id: str,
        experiment_type: str,
        execution_parameters: CampaignExecutionParameters,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Create a new campaign of a given experiment type with a unique id.

        :param campaign_id: A unique id for the campaign.
        :param experiment_type: The type of the experiment as defined in the configuration.
        :param execution_parameters: Parameters for the execution of the campaign.
        :param metadata: Additional metadata to be stored with the campaign.
        """
        if self._campaigns.get_one(id=campaign_id):
            raise EosCampaignStateError(f"Campaign '{campaign_id}' already exists.")

        experiment_config = self._configuration_manager.experiments.get(experiment_type)
        if not experiment_config:
            raise EosCampaignStateError(f"Experiment type '{experiment_type}' not found in the configuration.")

        campaign = Campaign(
            id=campaign_id,
            experiment_type=experiment_type,
            execution_parameters=execution_parameters,
            metadata=metadata or {},
        )
        self._campaigns.create(campaign.model_dump())

        log.info(f"Created campaign '{campaign_id}'.")

    def delete_campaign(self, campaign_id: str) -> None:
        """
        Delete a campaign.
        """
        if not self._campaigns.exists(id=campaign_id):
            raise EosCampaignStateError(f"Campaign '{campaign_id}' does not exist.")

        self._campaigns.delete(id=campaign_id)

        log.info(f"Deleted campaign '{campaign_id}'.")

    def start_campaign(self, campaign_id: str) -> None:
        """
        Start a campaign.
        """
        self._set_campaign_status(campaign_id, CampaignStatus.RUNNING)

    def complete_campaign(self, campaign_id: str) -> None:
        """
        Complete a campaign.
        """
        self._set_campaign_status(campaign_id, CampaignStatus.COMPLETED)

    def cancel_campaign(self, campaign_id: str) -> None:
        """
        Cancel a campaign.
        """
        self._set_campaign_status(campaign_id, CampaignStatus.CANCELLED)

    def suspend_campaign(self, campaign_id: str) -> None:
        """
        Suspend a campaign.
        """
        self._set_campaign_status(campaign_id, CampaignStatus.SUSPENDED)

    def fail_campaign(self, campaign_id: str) -> None:
        """
        Fail a campaign.
        """
        self._set_campaign_status(campaign_id, CampaignStatus.FAILED)

    def get_campaign(self, campaign_id: str) -> Campaign | None:
        """
        Get a campaign.
        """
        campaign = self._campaigns.get_one(id=campaign_id)
        return Campaign(**campaign) if campaign else None

    def get_campaigns(self, **query: dict[str, Any]) -> list[Campaign]:
        """
        Query campaigns with arbitrary parameters.

        :param query: Dictionary of query parameters.
        """
        campaigns = self._campaigns.get_all(**query)
        return [Campaign(**campaign) for campaign in campaigns]

    def _set_campaign_status(self, campaign_id: str, new_status: CampaignStatus) -> None:
        """
        Set the status of a campaign.
        """
        update_fields = {"status": new_status.value}
        if new_status == CampaignStatus.RUNNING:
            update_fields["start_time"] = datetime.now(tz=timezone.utc)
        elif new_status in [
            CampaignStatus.COMPLETED,
            CampaignStatus.CANCELLED,
            CampaignStatus.FAILED,
        ]:
            update_fields["end_time"] = datetime.now(tz=timezone.utc)

        self._campaigns.update(update_fields, id=campaign_id)

    def increment_iteration(self, campaign_id: str) -> None:
        """
        Increment the iteration count of a campaign.
        """
        self._campaigns.increment_campaign_iteration(campaign_id)

    def add_campaign_experiment(self, campaign_id: str, experiment_id: str) -> None:
        """
        Add an experiment to a campaign.
        """
        self._campaigns.add_current_experiment(campaign_id, experiment_id)

    def delete_campaign_experiment(self, campaign_id: str, experiment_id: str) -> None:
        """
        Remove an experiment from a campaign.
        """
        self._campaigns.remove_current_experiment(campaign_id, experiment_id)

    def delete_current_campaign_experiments(self, campaign_id: str) -> None:
        """
        Delete all current experiments from a campaign.
        """
        campaign = self.get_campaign(campaign_id)

        for experiment_id in campaign.current_experiment_ids:
            self._experiments.delete(id=experiment_id)
            self._tasks.delete(experiment_id=experiment_id)

        self._campaigns.clear_current_experiments(campaign_id)

    def get_campaign_experiment_ids(self, campaign_id: str, status: ExperimentStatus | None = None) -> list[str]:
        """
        Get all experiment IDs of a campaign with an optional status filter.

        :param campaign_id: The ID of the campaign.
        :param status: Optional status to filter experiments.
        :return: A list of experiment IDs.
        """
        return self._experiments.get_experiment_ids_by_campaign(campaign_id, status)

    def set_pareto_solutions(self, campaign_id: str, pareto_solutions: dict[str, Any]) -> None:
        """
        Set the Pareto solutions for a campaign.
        """
        self._campaigns.update({"pareto_solutions": pareto_solutions}, id=campaign_id)
