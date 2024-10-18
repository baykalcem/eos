import asyncio

import pandas as pd
import ray
from ray.actor import ActorHandle

from eos.campaigns.entities.campaign import CampaignSample
from eos.campaigns.repositories.campaign_samples_repository import CampaignSamplesRepository
from eos.configuration.configuration_manager import ConfigurationManager
from eos.logging.logger import log
from eos.optimization.sequential_optimizer_actor import SequentialOptimizerActor
from eos.persistence.async_mongodb_interface import AsyncMongoDbInterface


class CampaignOptimizerManager:
    """
    Responsible for managing the optimizers associated with experiment campaigns.
    """

    def __init__(self, configuration_manager: ConfigurationManager, db_interface: AsyncMongoDbInterface):
        self._session_factory = db_interface.session_factory
        self._campaign_optimizer_plugin_registry = configuration_manager.campaign_optimizers
        self._optimizer_actors: dict[str, ActorHandle] = {}
        self._campaign_samples = None

    async def initialize(self, db_interface: AsyncMongoDbInterface) -> None:
        self._campaign_samples = CampaignSamplesRepository(db_interface)
        await self._campaign_samples.initialize()

        log.debug("Campaign optimizer manager initialized.")

    def create_campaign_optimizer_actor(self, experiment_type: str, campaign_id: str, computer_ip: str) -> ActorHandle:
        """
        Create a new campaign optimizer Ray actor.

        :param experiment_type: The type of the experiment.
        :param campaign_id: The ID of the campaign.
        :param computer_ip: The IP address of the optimizer computer on which the actor will run.
        """
        constructor_args, optimizer_type = (
            self._campaign_optimizer_plugin_registry.get_campaign_optimizer_creation_parameters(experiment_type)
        )

        resources = {"eos-core": 0.01} if computer_ip in ["localhost", "127.0.0.1"] else {f"node:{computer_ip}": 0.01}

        optimizer_actor = SequentialOptimizerActor.options(name=f"{campaign_id}_optimizer", resources=resources).remote(
            constructor_args, optimizer_type
        )

        self._optimizer_actors[campaign_id] = optimizer_actor

        return optimizer_actor

    def terminate_campaign_optimizer_actor(self, campaign_id: str) -> None:
        """
        Terminate the Ray actor associated with the optimizer for a campaign.

        :param campaign_id: The ID of the campaign.
        """
        optimizer_actor = self._optimizer_actors.pop(campaign_id, None)

        if optimizer_actor is not None:
            ray.kill(optimizer_actor)

    def get_campaign_optimizer_actor(self, campaign_id: str) -> ActorHandle:
        """
        Get an existing Ray actor associated with the optimizer for a campaign.

        :param campaign_id: The ID of the campaign.
        :return: The Ray actor associated with the optimizer.
        """
        return self._optimizer_actors[campaign_id]

    async def get_input_and_output_names(self, campaign_id: str) -> tuple[list[str], list[str]]:
        """
        Get the input and output names from an optimizer associated with a campaign.

        :param campaign_id: The ID of the campaign associated with the optimizer.
        :return: A tuple containing the input and output names.
        """
        optimizer_actor = self._optimizer_actors[campaign_id]

        input_names, output_names = await asyncio.gather(
            optimizer_actor.get_input_names.remote(), optimizer_actor.get_output_names.remote()
        )

        return input_names, output_names

    async def record_campaign_samples(
        self,
        campaign_id: str,
        experiment_ids: list[str],
        inputs: pd.DataFrame,
        outputs: pd.DataFrame,
    ) -> None:
        """
        Record one or more campaign samples (experiment results) for the given campaign.
        Each sample is a data point for the optimizer to learn from.

        :param campaign_id: The ID of the campaign.
        :param experiment_ids: The IDs of the experiments.
        :param inputs: The input data.
        :param outputs: The output data.
        """
        inputs_dict = inputs.to_dict(orient="records")
        outputs_dict = outputs.to_dict(orient="records")

        campaign_samples = [
            CampaignSample(
                campaign_id=campaign_id,
                experiment_id=experiment_id,
                inputs=inputs_dict[i],
                outputs=outputs_dict[i],
            )
            for i, experiment_id in enumerate(experiment_ids)
        ]

        for campaign_sample in campaign_samples:
            await self._campaign_samples.create(campaign_sample.model_dump())

    async def delete_campaign_samples(self, campaign_id: str) -> None:
        """
        Delete all campaign samples for a campaign.

        :param campaign_id: The ID of the campaign.
        """
        await self._campaign_samples.delete_many(campaign_id=campaign_id)
