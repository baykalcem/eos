from motor.core import AgnosticClientSession

from eos.experiments.entities.experiment import ExperimentStatus
from eos.persistence.async_mongodb_interface import AsyncMongoDbInterface
from eos.persistence.mongodb_async_repository import MongoDbAsyncRepository


class ExperimentRepository(MongoDbAsyncRepository):
    def __init__(self, db_interface: AsyncMongoDbInterface):
        super().__init__("experiments", db_interface)

    async def initialize(self) -> None:
        await self.create_indices([("id", 1)], unique=True)

    async def get_experiments_by_lab(self, lab_type: str, session: AgnosticClientSession | None = None) -> list[dict]:
        return await self._collection.find({"labs": {"$in": [lab_type]}}, session=session).to_list(length=None)

    async def add_running_task(
        self, experiment_id: str, task_id: str, session: AgnosticClientSession | None = None
    ) -> None:
        await self._collection.update_one(
            {"id": experiment_id},
            {"$addToSet": {"running_tasks": task_id}},
            session=session,
        )

    async def delete_running_task(
        self, experiment_id: str, task_id: str, session: AgnosticClientSession | None = None
    ) -> None:
        await self._collection.update_one(
            {"id": experiment_id},
            {"$pull": {"running_tasks": task_id}},
            session=session,
        )

    async def clear_running_tasks(self, experiment_id: str, session: AgnosticClientSession | None = None) -> None:
        await self._collection.update_one(
            {"id": experiment_id},
            {"$set": {"running_tasks": []}},
            session=session,
        )

    async def move_task_queue(
        self, experiment_id: str, task_id: str, source: str, target: str, session: AgnosticClientSession | None = None
    ) -> None:
        await self._collection.update_one(
            {"id": experiment_id},
            {"$pull": {source: task_id}, "$addToSet": {target: task_id}},
            session=session,
        )

    async def get_experiment_ids_by_campaign(
        self, campaign_id: str, status: ExperimentStatus | None = None, session: AgnosticClientSession | None = None
    ) -> list[str]:
        query = {"id": {"$regex": f"^{campaign_id}"}}
        if status:
            query["status"] = status.value

        return [
            doc["id"] for doc in await self._collection.find(query, {"id": 1}, session=session).to_list(length=None)
        ]
