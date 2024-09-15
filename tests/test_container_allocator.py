from eos.resource_allocation.exceptions import (
    EosContainerAllocatedError,
    EosContainerNotFoundError,
)
from tests.fixtures import *


@pytest.mark.parametrize(
    "setup_lab_experiment", [("small_lab", "water_purification")], indirect=True
)
class TestContainerAllocator:
    def test_allocate_container(self, container_allocator):
        container_id = "ec1ca48cd5d14c0c8cde376476e0d98d"
        container_allocator.allocate(container_id, "owner", "water_purification_1")
        container = container_allocator.get_allocation(container_id)

        assert container.id == container_id
        assert container.owner == "owner"
        assert container.experiment_id == "water_purification_1"

    def test_allocate_container_already_allocated(self, container_allocator):
        container_id = "ec1ca48cd5d14c0c8cde376476e0d98d"
        container_allocator.allocate(container_id, "owner", "water_purification_1")

        with pytest.raises(EosContainerAllocatedError):
            container_allocator.allocate(container_id, "owner", "water_purification_1")

    def test_allocate_nonexistent_container(self, container_allocator):
        container_id = "nonexistent_container_id"
        with pytest.raises(EosContainerNotFoundError):
            container_allocator.allocate(container_id, "owner", "water_purification_1")

    def test_deallocate_container(self, container_allocator):
        container_id = "ec1ca48cd5d14c0c8cde376476e0d98d"
        container_allocator.allocate(container_id, "owner", "water_purification_1")

        container_allocator.deallocate(container_id)
        container = container_allocator.get_allocation(container_id)

        assert container is None

    def test_deallocate_container_not_allocated(self, container_allocator):
        container_id = "ec1ca48cd5d14c0c8cde376476e0d98d"
        container_allocator.deallocate(container_id)
        assert container_allocator.get_allocation(container_id) is None

    def test_is_allocated(self, container_allocator):
        container_id = "ec1ca48cd5d14c0c8cde376476e0d98d"
        assert not container_allocator.is_allocated(container_id)

        container_allocator.allocate(container_id, "owner", "water_purification_1")
        assert container_allocator.is_allocated(container_id)

    def test_get_allocations_by_owner(self, container_allocator):
        container_id_1 = "ec1ca48cd5d14c0c8cde376476e0d98d"
        container_id_2 = "84eb17d61e884ffd9d1fdebcbad1532b"
        container_id_3 = "a3b958aea8bd435386cdcbab20a2d3ec"

        container_allocator.allocate(container_id_1, "owner", "water_purification_1")
        container_allocator.allocate(container_id_2, "owner", "water_purification_1")
        container_allocator.allocate(container_id_3, "another_owner", "water_purification_1")

        allocations = container_allocator.get_allocations(owner="owner")
        assert allocations[0].id == container_id_1
        assert allocations[1].id == container_id_2
        assert len(allocations) == 2

        allocations = container_allocator.get_allocations(owner="another_owner")
        assert allocations[0].id == container_id_3
        assert len(allocations) == 1

    def test_get_all_allocations(self, container_allocator):
        container_id_1 = "ec1ca48cd5d14c0c8cde376476e0d98d"
        container_id_2 = "84eb17d61e884ffd9d1fdebcbad1532b"
        container_id_3 = "a3b958aea8bd435386cdcbab20a2d3ec"

        container_allocator.allocate(container_id_1, "owner", "water_purification_1")
        container_allocator.allocate(container_id_2, "owner", "water_purification_1")
        container_allocator.allocate(container_id_3, "another_owner", "water_purification_1")

        allocations = container_allocator.get_allocations()
        assert len(allocations) == 3
        assert {allocation.id for allocation in allocations} == {
            container_id_1,
            container_id_2,
            container_id_3,
        }

    def test_get_all_unallocated_containers(self, container_allocator):
        container_id_1 = "ec1ca48cd5d14c0c8cde376476e0d98d"
        container_id_2 = "84eb17d61e884ffd9d1fdebcbad1532b"
        container_id_3 = "a3b958aea8bd435386cdcbab20a2d3ec"

        initial_unallocated_containers = container_allocator.get_all_unallocated()

        container_allocator.allocate(container_id_1, "owner1", "water_purification_1")
        container_allocator.allocate(container_id_2, "owner2", "water_purification_1")

        new_unallocated_containers = container_allocator.get_all_unallocated()
        assert len(new_unallocated_containers) == len(initial_unallocated_containers) - 2
        assert container_id_1 not in new_unallocated_containers
        assert container_id_2 not in new_unallocated_containers
        assert container_id_3 in new_unallocated_containers

    def test_deallocate_all_containers(self, container_allocator):
        container_id_1 = "ec1ca48cd5d14c0c8cde376476e0d98d"
        container_id_2 = "84eb17d61e884ffd9d1fdebcbad1532b"
        container_id_3 = "a3b958aea8bd435386cdcbab20a2d3ec"

        container_allocator.allocate(container_id_1, "owner1", "water_purification_1")
        container_allocator.allocate(container_id_2, "owner2", "water_purification_1")
        container_allocator.allocate(container_id_3, "owner3", "water_purification_1")

        assert container_allocator.get_allocations() != []

        container_allocator.deallocate_all()

        assert container_allocator.get_allocations() == []

    def test_deallocate_all_containers_by_owner(self, container_allocator):
        container_id_1 = "ec1ca48cd5d14c0c8cde376476e0d98d"
        container_id_2 = "84eb17d61e884ffd9d1fdebcbad1532b"
        container_id_3 = "a3b958aea8bd435386cdcbab20a2d3ec"

        container_allocator.allocate(container_id_1, "owner1", "water_purification_1")
        container_allocator.allocate(container_id_2, "owner2", "water_purification_1")
        container_allocator.allocate(container_id_3, "owner2", "water_purification_1")

        container_allocator.deallocate_all_by_owner("owner2")

        owner2_allocations = container_allocator.get_allocations(owner="owner2")
        assert owner2_allocations == []
        assert container_allocator.get_allocations() == [
            container_allocator.get_allocation(container_id_1)
        ]
