from collections.abc import Callable
from datetime import datetime, timezone
from threading import Lock

from bson import ObjectId

from eos.configuration.configuration_manager import ConfigurationManager
from eos.logging.logger import log
from eos.persistence.db_manager import DbManager
from eos.resource_allocation.container_allocation_manager import ContainerAllocationManager
from eos.resource_allocation.device_allocation_manager import DeviceAllocationManager
from eos.resource_allocation.entities.resource_request import (
    ResourceAllocationRequest,
    ActiveResourceAllocationRequest,
    ResourceRequestAllocationStatus,
    ResourceType,
)
from eos.resource_allocation.exceptions import EosResourceRequestError
from eos.resource_allocation.repositories.resource_request_repository import (
    ResourceRequestRepository,
)


class ResourceAllocationManager:
    """
    Provides facilities to request allocation of resources.
    """

    def __init__(
        self,
        configuration_manager: ConfigurationManager,
        db_manager: DbManager,
    ):
        self._device_allocation_manager = DeviceAllocationManager(configuration_manager, db_manager)
        self._container_allocation_manager = ContainerAllocationManager(configuration_manager, db_manager)
        self._active_requests = ResourceRequestRepository("resource_requests", db_manager)

        # Callbacks for when resource allocation requests are processed
        self._request_callbacks: dict[ObjectId, Callable[[ActiveResourceAllocationRequest], None]] = {}

        self._lock = Lock()

        self._delete_all_requests()
        self._delete_all_allocations()

        log.debug("Resource allocation manager initialized.")

    def request_resources(
        self,
        request: ResourceAllocationRequest,
        callback: Callable[[ActiveResourceAllocationRequest], None],
    ) -> ActiveResourceAllocationRequest:
        """
        Request allocation of resources. A callback function is called when the resource allocation requests are
        processed. If a resource allocation request already exists, the existing request is used instead of creating
        a new one.

        :param request: The resource allocation request.
        :param callback: Callback function to be called when the resource allocation request is processed.
        :return: List of active resource allocation requests.
        """
        with self._lock:
            existing_request = self._find_existing_request(request)
            if existing_request:
                if existing_request.status in [
                    ResourceRequestAllocationStatus.PENDING,
                    ResourceRequestAllocationStatus.ALLOCATED,
                ]:
                    self._request_callbacks[existing_request.id] = callback
                return existing_request

            active_request = ActiveResourceAllocationRequest(request=request)
            result = self._active_requests.create(active_request.model_dump(by_alias=True))
            active_request.id = result.inserted_id
            self._request_callbacks[active_request.id] = callback
            return active_request

    def release_resources(self, active_request: ActiveResourceAllocationRequest) -> None:
        """
        Release the resources allocated for an active resource allocation request.

        :param active_request: The active resource allocation request.
        """
        with self._lock:
            for resource in active_request.request.resources:
                if resource.resource_type == ResourceType.DEVICE:
                    self._device_allocation_manager.deallocate(resource.lab_id, resource.id)
                elif resource.resource_type == ResourceType.CONTAINER:
                    self._container_allocation_manager.deallocate(resource.id)
                else:
                    raise EosResourceRequestError(f"Unknown resource type: {resource.resource_type}")

            self._update_request_status(active_request.id, ResourceRequestAllocationStatus.COMPLETED)

    def process_active_requests(self) -> None:
        with self._lock:
            self._clean_completed_and_aborted_requests()

            active_requests = self._get_all_active_requests_prioritized()

            for active_request in active_requests:
                if active_request.status != ResourceRequestAllocationStatus.PENDING:
                    continue

                allocation_success = self._try_allocate(active_request)

                if allocation_success:
                    self._invoke_request_callback(active_request)

    def abort_active_request(self, request_id: ObjectId) -> None:
        """
        Abort an active resource allocation request.
        """
        with self._lock:
            request = self.get_active_request(request_id)
            for resource in request.request.resources:
                if resource.resource_type == ResourceType.DEVICE:
                    self._device_allocation_manager.deallocate(resource.lab_id, resource.id)
                elif resource.resource_type == ResourceType.CONTAINER:
                    self._container_allocation_manager.deallocate(resource.id)
            self._update_request_status(request_id, ResourceRequestAllocationStatus.ABORTED)
            active_request = self.get_active_request(request_id)
            self._invoke_request_callback(active_request)

    def _get_all_active_requests_prioritized(self) -> list[ActiveResourceAllocationRequest]:
        """
        Get all active resource allocation requests prioritized by the request priority in ascending order.
        """
        active_requests = []
        active_requests_count = self._active_requests.count(status=ResourceRequestAllocationStatus.PENDING.value)

        if active_requests_count > 0:
            active_requests = self._active_requests.get_requests_prioritized(ResourceRequestAllocationStatus.PENDING)

        return [ActiveResourceAllocationRequest(**request) for request in active_requests]

    def get_all_active_requests(
        self,
        requester: str | None = None,
        lab_id: str | None = None,
        experiment_id: str | None = None,
        status: ResourceRequestAllocationStatus | None = None,
    ) -> list[ActiveResourceAllocationRequest]:
        """
        Get all active resource allocation requests.

        :param requester: Filter by the requester.
        :param lab_id: Filter by the lab ID.
        :param experiment_id: Filter by the experiment ID.
        :param status: Filter by the status.
        """
        query = {"requester": requester}
        if lab_id:
            query["request.lab_id"] = lab_id
        if experiment_id:
            query["request.experiment_id"] = experiment_id
        if status:
            query["status"] = status.value
        active_requests = self._active_requests.get_all(**query)
        return [ActiveResourceAllocationRequest(**request) for request in active_requests]

    def get_active_request(self, request_id: ObjectId) -> ActiveResourceAllocationRequest | None:
        """
        Get an active resource allocation request by ID. If the request does not exist, returns None.
        """
        request = self._active_requests.get_one(_id=request_id)
        return ActiveResourceAllocationRequest(**request) if request else None

    @property
    def device_allocation_manager(self) -> DeviceAllocationManager:
        return self._device_allocation_manager

    @property
    def container_allocation_manager(self) -> ContainerAllocationManager:
        return self._container_allocation_manager

    def _update_request_status(self, request_id: ObjectId, status: ResourceRequestAllocationStatus) -> None:
        """
        Update the status of an active resource allocation request.
        """
        update_data = {"status": status.value}
        if status == ResourceRequestAllocationStatus.ALLOCATED:
            update_data["allocated_at"] = datetime.now(tz=timezone.utc)

        self._active_requests.update(update_data, _id=request_id)

    def _find_existing_request(self, request: ResourceAllocationRequest) -> ActiveResourceAllocationRequest | None:
        """
        Find an existing active resource allocation request that matches the given request.
        """
        existing_request = self._active_requests.get_existing_request(request)
        return ActiveResourceAllocationRequest(**existing_request) if existing_request else None

    def _invoke_request_callback(self, active_request: ActiveResourceAllocationRequest) -> None:
        """
        Invoke the allocation callback for an active resource allocation request.
        """
        callback = self._request_callbacks.pop(active_request.id, None)
        if callback:
            callback(active_request)

    def _try_allocate(self, active_request: ActiveResourceAllocationRequest) -> bool:
        temp_allocations = []
        all_available = True

        for resource in active_request.request.resources:
            if resource.resource_type == ResourceType.DEVICE:
                if not self._device_allocation_manager.is_allocated(resource.lab_id, resource.id):
                    temp_allocations.append(("device", resource.lab_id, resource.id))
                else:
                    all_available = False
                    break
            elif resource.resource_type == ResourceType.CONTAINER:
                if not self._container_allocation_manager.is_allocated(resource.id):
                    temp_allocations.append(("container", resource.id))
                else:
                    all_available = False
                    break
            else:
                raise EosResourceRequestError(f"Unknown resource type: {resource.resource_type}")

        if all_available:
            for allocation in temp_allocations:
                if allocation[0] == "device":
                    self._device_allocation_manager.allocate(
                        allocation[1],
                        allocation[2],
                        active_request.request.requester,
                        experiment_id=active_request.request.experiment_id,
                    )
                else:  # container
                    self._container_allocation_manager.allocate(
                        allocation[1],
                        active_request.request.requester,
                        experiment_id=active_request.request.experiment_id,
                    )

            self._update_request_status(active_request.id, ResourceRequestAllocationStatus.ALLOCATED)
            active_request.status = ResourceRequestAllocationStatus.ALLOCATED
            return True

        return False

    def _clean_completed_and_aborted_requests(self) -> None:
        """
        Remove completed or aborted active resource allocation requests.
        """
        self._active_requests.clean_requests()

    def _delete_all_requests(self) -> None:
        """
        Delete all active resource allocation requests.
        """
        self._active_requests.delete()

    def _delete_all_allocations(self) -> None:
        """
        Delete all device and container allocations.
        """
        self._device_allocation_manager.deallocate_all()
        self._container_allocation_manager.deallocate_all()
