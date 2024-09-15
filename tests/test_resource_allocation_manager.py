from bson import ObjectId

from eos.resource_allocation.entities.resource_request import (
    ResourceAllocationRequest,
    ActiveResourceAllocationRequest,
    ResourceType,
    ResourceRequestAllocationStatus,
)
from eos.resource_allocation.exceptions import EosDeviceNotFoundError
from tests.fixtures import *

LAB_ID = "small_lab"


@pytest.mark.parametrize("setup_lab_experiment", [(LAB_ID, "water_purification")], indirect=True)
class TestResourceAllocationManager:
    def test_request_resources(self, resource_allocation_manager):
        request = ResourceAllocationRequest(
            requester="test_requester",
            reason="Needed for experiment",
            experiment_id="water_purification_1",
        )
        request.add_resource("magnetic_mixer", LAB_ID, ResourceType.DEVICE)
        request.add_resource("026749f8f40342b38157f9824ae2f512", "", ResourceType.CONTAINER)

        def callback(active_request: ActiveResourceAllocationRequest):
            assert active_request.status == ResourceRequestAllocationStatus.ALLOCATED
            assert len(active_request.request.resources) == 2
            assert any(r.id == "magnetic_mixer" for r in active_request.request.resources)
            assert any(r.id == "026749f8f40342b38157f9824ae2f512" for r in active_request.request.resources)

        active_request = resource_allocation_manager.request_resources(request, callback)

        assert active_request.request == request
        assert active_request.status == ResourceRequestAllocationStatus.PENDING

        resource_allocation_manager.process_active_requests()

    def test_request_resources_priority(self, resource_allocation_manager):
        requests = [
            ResourceAllocationRequest(
                requester=f"test_requester{i}",
                reason="Needed for experiment",
                experiment_id="water_purification_1",
                priority=100 + i,
            )
            for i in range(1, 4)
        ]
        for request in requests:
            request.add_resource("magnetic_mixer", LAB_ID, ResourceType.DEVICE)

        active_requests = [resource_allocation_manager.request_resources(req, lambda x: None) for req in requests]
        resource_allocation_manager.process_active_requests()

        # Ensure that requests[0] is allocated and the rest are pending
        active_request_3 = resource_allocation_manager.get_active_request(active_requests[2].id)
        assert active_request_3.status == ResourceRequestAllocationStatus.PENDING
        assert active_request_3.request.requester == "test_requester3"
        assert active_request_3.request.priority == 103

        active_request_2 = resource_allocation_manager.get_active_request(active_requests[1].id)
        assert active_request_2.status == ResourceRequestAllocationStatus.PENDING
        assert active_request_2.request.requester == "test_requester2"
        assert active_request_2.request.priority == 102

        active_request_1 = resource_allocation_manager.get_active_request(active_requests[0].id)
        assert active_request_1.status == ResourceRequestAllocationStatus.ALLOCATED
        assert active_request_1.request.requester == "test_requester1"
        assert active_request_1.request.priority == 101

        resource_allocation_manager.release_resources(active_request_1)

        resource_allocation_manager.process_active_requests()

        # Ensure that requests[1] is now allocated and requests[2] is still pending
        active_request_3 = resource_allocation_manager.get_active_request(active_requests[2].id)
        assert active_request_3.status == ResourceRequestAllocationStatus.PENDING
        assert active_request_3.request.requester == "test_requester3"
        assert active_request_3.request.priority == 103

        active_request_2 = resource_allocation_manager.get_active_request(active_requests[1].id)
        assert active_request_2.status == ResourceRequestAllocationStatus.ALLOCATED
        assert active_request_2.request.requester == "test_requester2"
        assert active_request_2.request.priority == 102

    def test_release_resources(self, resource_allocation_manager):
        request = ResourceAllocationRequest(
            requester="test_requester",
            reason="Needed for experiment",
            experiment_id="water_purification_1",
            priority=1,
        )
        request.add_resource("magnetic_mixer", LAB_ID, ResourceType.DEVICE)
        request.add_resource("026749f8f40342b38157f9824ae2f512", "", ResourceType.CONTAINER)

        active_request = resource_allocation_manager.request_resources(request, lambda x: None)

        resource_allocation_manager.process_active_requests()

        resource_allocation_manager.release_resources(active_request)

        assert (
            resource_allocation_manager.get_active_request(active_request.id).status
            == ResourceRequestAllocationStatus.COMPLETED
        )

    def test_process_active_requests(self, resource_allocation_manager):
        requests = [
            ResourceAllocationRequest(
                requester=f"test_requester{i}",
                reason="Needed for experiment",
                experiment_id="water_purification_1",
            )
            for i in range(1, 3)
        ]
        for request in requests:
            request.add_resource("magnetic_mixer", LAB_ID, ResourceType.DEVICE)

        active_requests = [resource_allocation_manager.request_resources(req, lambda x: None) for req in requests]

        resource_allocation_manager.process_active_requests()

        assert (
            resource_allocation_manager.get_active_request(active_requests[0].id).status
            == ResourceRequestAllocationStatus.ALLOCATED
        )
        assert (
            resource_allocation_manager.get_active_request(active_requests[1].id).status
            == ResourceRequestAllocationStatus.PENDING
        )

    def test_abort_active_request(self, resource_allocation_manager):
        request = ResourceAllocationRequest(
            requester="test_requester",
            reason="Needed for experiment",
            experiment_id="water_purification_1",
        )
        request.add_resource("magnetic_mixer", LAB_ID, ResourceType.DEVICE)
        request.add_resource("magnetic_mixer_2", LAB_ID, ResourceType.DEVICE)

        active_request = resource_allocation_manager.request_resources(request, lambda x: None)

        resource_allocation_manager.abort_active_request(active_request.id)

        assert resource_allocation_manager.get_active_request(active_request.id).status == (
            ResourceRequestAllocationStatus.ABORTED
        )

        assert not resource_allocation_manager._device_allocation_manager.is_allocated(LAB_ID, "magnetic_mixer")
        assert not resource_allocation_manager._device_allocation_manager.is_allocated(LAB_ID, "magnetic_mixer_2")

    def test_get_all_active_requests(self, resource_allocation_manager):
        requests = [
            ResourceAllocationRequest(
                requester=f"test_requester{i}",
                reason="Needed for experiment",
                experiment_id="water_purification_1",
            )
            for i in range(1, 3)
        ]
        requests[0].add_resource("magnetic_mixer", LAB_ID, ResourceType.DEVICE)
        requests[1].add_resource("026749f8f40342b38157f9824ae2f512", "", ResourceType.CONTAINER)

        for request in requests:
            resource_allocation_manager.request_resources(request, lambda x: None)

        all_active_requests = resource_allocation_manager.get_all_active_requests()
        assert len(all_active_requests) == 2
        assert all_active_requests[0].request == requests[0]
        assert all_active_requests[1].request == requests[1]

    def test_get_active_request_nonexistent(self, resource_allocation_manager):
        nonexistent_id = ObjectId()
        assert resource_allocation_manager.get_active_request(nonexistent_id) is None

    def test_clean_requests(self, resource_allocation_manager):
        request = ResourceAllocationRequest(
            requester="test_requester",
            reason="Needed for experiment",
            experiment_id="water_purification_1",
        )
        request.add_resource("magnetic_mixer", LAB_ID, ResourceType.DEVICE)

        active_request = resource_allocation_manager.request_resources(request, lambda x: None)
        resource_allocation_manager.process_active_requests()
        resource_allocation_manager.release_resources(active_request)

        assert (
            resource_allocation_manager.get_active_request(active_request.id).status
            == ResourceRequestAllocationStatus.COMPLETED
        )

        resource_allocation_manager._clean_completed_and_aborted_requests()

        assert len(resource_allocation_manager.get_all_active_requests()) == 0

    def test_all_or_nothing_allocation(self, resource_allocation_manager):
        request = ResourceAllocationRequest(
            requester="test_requester",
            reason="Needed for experiment",
            experiment_id="water_purification_1",
        )
        request.add_resource("magnetic_mixer", LAB_ID, ResourceType.DEVICE)
        request.add_resource("nonexistent_device", LAB_ID, ResourceType.DEVICE)

        with pytest.raises(EosDeviceNotFoundError):
            active_request = resource_allocation_manager.request_resources(request, lambda x: None)
            resource_allocation_manager.process_active_requests()

        assert active_request.status == ResourceRequestAllocationStatus.PENDING

        # Verify that neither resource was allocated
        assert not resource_allocation_manager._device_allocation_manager.is_allocated(LAB_ID, "magnetic_mixer")

        with pytest.raises(EosDeviceNotFoundError):
            assert not resource_allocation_manager._device_allocation_manager.is_allocated(LAB_ID, "nonexistent_device")
