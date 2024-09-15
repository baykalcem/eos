from eos.resource_allocation.entities.resource_allocation import (
    ResourceAllocation,
)


class ContainerAllocation(ResourceAllocation):
    container_type: str
