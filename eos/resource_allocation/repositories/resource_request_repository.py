from eos.persistence.mongo_repository import MongoRepository
from eos.resource_allocation.entities.resource_request import (
    ResourceAllocationRequest,
    ResourceRequestAllocationStatus,
)


class ResourceRequestRepository(MongoRepository):
    def get_requests_prioritized(self, status: ResourceRequestAllocationStatus) -> list[dict]:
        return self._collection.find({"status": status.value}).sort("request.priority", 1)

    def get_existing_request(self, request: ResourceAllocationRequest) -> dict:
        query = {
            "request.resources": [r.model_dump() for r in request.resources],
            "request.requester": request.requester,
            "status": {
                "$in": [
                    ResourceRequestAllocationStatus.PENDING.value,
                    ResourceRequestAllocationStatus.ALLOCATED.value,
                ]
            },
        }

        return self._collection.find_one(query)

    def clean_requests(self) -> None:
        self._collection.delete_many(
            {
                "status": {
                    "$in": [
                        ResourceRequestAllocationStatus.COMPLETED.value,
                        ResourceRequestAllocationStatus.ABORTED.value,
                    ]
                }
            }
        )
