from eos.resource_allocation.exceptions import (
    EosDeviceAllocatedError,
    EosDeviceNotFoundError,
)
from tests.fixtures import *

LAB_ID = "small_lab"


@pytest.mark.parametrize("setup_lab_experiment", [(LAB_ID, "water_purification")], indirect=True)
class TestDeviceAllocator:
    def test_allocate_device(self, device_allocator):
        device_id = "magnetic_mixer"
        device_allocator.allocate(LAB_ID, device_id, "owner", "water_purification_1")

        allocation = device_allocator.get_allocation(LAB_ID, device_id)

        assert allocation.id == device_id
        assert allocation.lab_id == LAB_ID
        assert allocation.device_type == "magnetic_mixer"
        assert allocation.owner == "owner"
        assert allocation.experiment_id == "water_purification_1"

    def test_allocate_device_already_allocated(self, device_allocator):
        device_id = "magnetic_mixer"
        device_allocator.allocate(LAB_ID, device_id, "owner", "water_purification_1")

        with pytest.raises(EosDeviceAllocatedError):
            device_allocator.allocate(LAB_ID, device_id, "owner", "water_purification_1")

    def test_allocate_nonexistent_device(self, device_allocator):
        device_id = "nonexistent_device_id"
        with pytest.raises(EosDeviceNotFoundError):
            device_allocator.allocate(LAB_ID, device_id, "owner", "water_purification_1")

    def test_deallocate_device(self, device_allocator):
        device_id = "magnetic_mixer"
        device_allocator.allocate(LAB_ID, device_id, "owner", "water_purification_1")

        device_allocator.deallocate(LAB_ID, device_id)
        allocation = device_allocator.get_allocation(LAB_ID, device_id)

        assert allocation is None

    def test_deallocate_device_not_allocated(self, device_allocator):
        device_id = "magnetic_mixer"
        device_allocator.deallocate(LAB_ID, device_id)
        assert device_allocator.get_allocation(LAB_ID, device_id) is None

    def test_is_allocated(self, device_allocator):
        device_id = "magnetic_mixer"
        assert not device_allocator.is_allocated(LAB_ID, device_id)

        device_allocator.allocate(LAB_ID, device_id, "owner", "water_purification_1")
        assert device_allocator.is_allocated(LAB_ID, device_id)

    def test_get_allocations_by_owner(self, device_allocator):
        device_id_1 = "magnetic_mixer"
        device_id_2 = "evaporator"
        device_id_3 = "substance_fridge"

        device_allocator.allocate(LAB_ID, device_id_1, "owner1", "water_purification_1")
        device_allocator.allocate(LAB_ID, device_id_2, "owner1", "water_purification_1")
        device_allocator.allocate(LAB_ID, device_id_3, "owner2", "water_purification_1")

        allocations = device_allocator.get_allocations(owner="owner1")

        assert len(allocations) == 2
        assert device_id_1 in [allocation.id for allocation in allocations]
        assert device_id_2 in [allocation.id for allocation in allocations]

    def test_get_all_allocations(self, device_allocator):
        device_id_1 = "magnetic_mixer"
        device_id_2 = "evaporator"
        device_id_3 = "substance_fridge"

        device_allocator.allocate(LAB_ID, device_id_1, "owner", "water_purification_1")
        device_allocator.allocate(LAB_ID, device_id_2, "owner", "water_purification_1")
        device_allocator.allocate(LAB_ID, device_id_3, "owner", "water_purification_1")

        allocations = device_allocator.get_allocations()

        assert len(allocations) == 3
        assert device_id_1 in [allocation.id for allocation in allocations]
        assert device_id_2 in [allocation.id for allocation in allocations]
        assert device_id_3 in [allocation.id for allocation in allocations]

    def test_get_all_unallocated(self, device_allocator):
        device_id_1 = "magnetic_mixer"
        device_id_2 = "evaporator"
        device_id_3 = "substance_fridge"

        initial_unallocated_devices = device_allocator.get_all_unallocated()

        device_allocator.allocate(LAB_ID, device_id_1, "owner", "water_purification_1")
        device_allocator.allocate(LAB_ID, device_id_2, "owner", "water_purification_1")

        new_unallocated_devices = device_allocator.get_all_unallocated()

        assert len(new_unallocated_devices) == len(initial_unallocated_devices) - 2
        assert device_id_1 not in new_unallocated_devices
        assert device_id_2 not in new_unallocated_devices
        assert device_id_3 in new_unallocated_devices

    def test_deallocate_all(self, device_allocator):
        device_id_1 = "magnetic_mixer"
        device_id_2 = "evaporator"
        device_id_3 = "substance_fridge"

        device_allocator.allocate(LAB_ID, device_id_1, "owner", "water_purification_1")
        device_allocator.allocate(LAB_ID, device_id_2, "owner", "water_purification_1")
        device_allocator.allocate(LAB_ID, device_id_3, "owner", "water_purification_1")

        assert device_allocator.get_allocations() != []

        device_allocator.deallocate_all()

        assert device_allocator.get_allocations() == []

    def test_deallocate_all_by_owner(self, device_allocator):
        device_id_1 = "magnetic_mixer"
        device_id_2 = "evaporator"
        device_id_3 = "substance_fridge"

        device_allocator.allocate(LAB_ID, device_id_1, "owner1", "water_purification_1")
        device_allocator.allocate(LAB_ID, device_id_2, "owner2", "water_purification_1")
        device_allocator.allocate(LAB_ID, device_id_3, "owner2", "water_purification_1")

        device_allocator.deallocate_all_by_owner("owner2")

        owner2_allocations = device_allocator.get_allocations(owner="owner2")
        assert owner2_allocations == []
        assert device_allocator.get_allocations() == [
            device_allocator.get_allocation(LAB_ID, device_id_1)
        ]
