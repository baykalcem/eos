from eos.persistence.async_mongodb_interface import AsyncMongoDbInterface
from eos.persistence.mongodb_async_repository import MongoDbAsyncRepository


class DeviceAllocationRepository(MongoDbAsyncRepository):
    def __init__(self, db_interface: AsyncMongoDbInterface):
        super().__init__("device_allocations", db_interface)

    async def initialize(self) -> None:
        await self.create_indices([("lab_id", 1), ("id", 1)], unique=True)
