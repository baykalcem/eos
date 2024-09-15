from eos.campaigns.exceptions import EosCampaignStateError
from eos.persistence.mongo_repository import MongoRepository


class CampaignRepository(MongoRepository):
    def increment_campaign_iteration(self, campaign_id: str) -> None:
        result = self._collection.update_one({"id": campaign_id}, {"$inc": {"experiments_completed": 1}})

        if result.matched_count == 0:
            raise EosCampaignStateError(
                f"Cannot increment the iteration of campaign '{campaign_id}' as it does not exist."
            )

    def add_current_experiment(self, campaign_id: str, experiment_id: str) -> None:
        self._collection.update_one(
            {"id": campaign_id},
            {"$addToSet": {"current_experiment_ids": experiment_id}},
        )

    def remove_current_experiment(self, campaign_id: str, experiment_id: str) -> None:
        self._collection.update_one(
            {"id": campaign_id},
            {"$pull": {"current_experiment_ids": experiment_id}},
        )

    def clear_current_experiments(self, campaign_id: str) -> None:
        self._collection.update_one(
            {"id": campaign_id},
            {"$set": {"current_experiment_ids": []}},
        )
