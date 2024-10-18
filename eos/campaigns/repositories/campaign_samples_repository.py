from eos.persistence.async_mongodb_interface import AsyncMongoDbInterface
from eos.persistence.mongodb_async_repository import MongoDbAsyncRepository


class CampaignSamplesRepository(MongoDbAsyncRepository):
    def __init__(self, db_interface: AsyncMongoDbInterface):
        super().__init__("campaign_samples", db_interface)

    async def initialize(self) -> None:
        await self.create_indices([("campaign_id", 1), ("experiment_id", 1)], unique=True)
