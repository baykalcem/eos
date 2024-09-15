from eos.experiments.entities.experiment import ExperimentStatus
from eos.persistence.mongo_repository import MongoRepository


class ExperimentRepository(MongoRepository):
    def get_experiments_by_lab(self, lab_type: str) -> list[dict]:
        return self._collection.find({"labs": {"$in": [lab_type]}})

    def add_running_task(self, experiment_id: str, task_id: str) -> None:
        self._collection.update_one(
            {"id": experiment_id},
            {"$addToSet": {"running_tasks": task_id}},
        )

    def delete_running_task(self, experiment_id: str, task_id: str) -> None:
        self._collection.update_one(
            {"id": experiment_id},
            {"$pull": {"running_tasks": task_id}},
        )

    def clear_running_tasks(self, experiment_id: str) -> None:
        self._collection.update_one(
            {"id": experiment_id},
            {"$set": {"running_tasks": []}},
        )

    def move_task_queue(self, experiment_id: str, task_id: str, source: str, target: str) -> None:
        self._collection.update_one(
            {"id": experiment_id},
            {"$pull": {source: task_id}, "$addToSet": {target: task_id}},
        )

    def get_experiment_ids_by_campaign(self, campaign_id: str, status: ExperimentStatus | None = None) -> list[str]:
        """
        Get all experiment IDs of a campaign with an optional status filter.

        :param campaign_id: The ID of the campaign.
        :param status: Optional status to filter experiments.
        :return: A list of experiment IDs.
        """
        query = {"id": {"$regex": f"^{campaign_id}"}}
        if status:
            query["status"] = status.value

        return [doc["id"] for doc in self._collection.find(query, {"id": 1})]
