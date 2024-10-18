from eos.persistence.async_mongodb_interface import AsyncMongoDbInterface
from eos.persistence.mongodb_async_repository import MongoDbAsyncRepository


class ContainerAllocationRepository(MongoDbAsyncRepository):
    def __init__(self, db_interface: AsyncMongoDbInterface):
        super().__init__("container_allocations", db_interface)

    async def initialize(self) -> None:
        await self.create_indices([("id", 1)], unique=True)
