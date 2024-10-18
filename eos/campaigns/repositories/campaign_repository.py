from motor.core import AgnosticClientSession

from eos.campaigns.exceptions import EosCampaignStateError
from eos.persistence.async_mongodb_interface import AsyncMongoDbInterface
from eos.persistence.mongodb_async_repository import MongoDbAsyncRepository


class CampaignRepository(MongoDbAsyncRepository):
    def __init__(self, db_interface: AsyncMongoDbInterface):
        super().__init__("campaigns", db_interface)

    async def initialize(self) -> None:
        await self.create_indices([("id", 1)], unique=True)

    async def increment_campaign_iteration(
        self, campaign_id: str, session: AgnosticClientSession | None = None
    ) -> None:
        result = await self._collection.update_one(
            {"id": campaign_id}, {"$inc": {"experiments_completed": 1}}, session=session
        )

        if result.matched_count == 0:
            raise EosCampaignStateError(
                f"Cannot increment the iteration of campaign '{campaign_id}' as it does not exist."
            )

    async def add_current_experiment(
        self, campaign_id: str, experiment_id: str, session: AgnosticClientSession | None = None
    ) -> None:
        await self._collection.update_one(
            {"id": campaign_id}, {"$addToSet": {"current_experiment_ids": experiment_id}}, session=session
        )

    async def remove_current_experiment(
        self, campaign_id: str, experiment_id: str, session: AgnosticClientSession | None = None
    ) -> None:
        await self._collection.update_one(
            {"id": campaign_id}, {"$pull": {"current_experiment_ids": experiment_id}}, session=session
        )

    async def clear_current_experiments(self, campaign_id: str, session: AgnosticClientSession | None = None) -> None:
        await self._collection.update_one(
            {"id": campaign_id}, {"$set": {"current_experiment_ids": []}}, session=session
        )
