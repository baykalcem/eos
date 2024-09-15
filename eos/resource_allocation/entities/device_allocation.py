from eos.resource_allocation.entities.resource_allocation import (
    ResourceAllocation,
)


class DeviceAllocation(ResourceAllocation):
    lab_id: str
    device_type: str
