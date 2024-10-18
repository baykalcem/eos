from motor.core import AgnosticClientSession

from eos.persistence.async_mongodb_interface import AsyncMongoDbInterface
from eos.persistence.mongodb_async_repository import MongoDbAsyncRepository
from eos.tasks.entities.task import TaskStatus


class TaskRepository(MongoDbAsyncRepository):
    def __init__(self, db_interface: AsyncMongoDbInterface):
        super().__init__("tasks", db_interface)

    async def initialize(self) -> None:
        await self.create_indices([("experiment_id", 1), ("id", 1)], unique=True)

    async def delete_running_tasks(
        self, experiment_id: str, task_ids: list[str], session: AgnosticClientSession | None = None
    ) -> None:
        """
        Delete all running tasks for a given experiment in a single operation.
        """
        await self._collection.delete_many({"experiment_id": experiment_id, "id": {"$in": task_ids}}, session=session)

    async def delete_failed_and_cancelled_tasks(
        self, experiment_id: str, session: AgnosticClientSession | None = None
    ) -> None:
        """
        Delete all non-completed tasks for a given experiment in a single operation.
        This includes tasks with FAILED, CANCELLED, and any other non-completed status.
        """
        await self._collection.delete_many(
            {
                "experiment_id": experiment_id,
                "status": {
                    "$in": [
                        TaskStatus.FAILED.value,
                        TaskStatus.CANCELLED.value,
                    ]
                },
            },
            session=session,
        )
