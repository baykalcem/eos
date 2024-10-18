from typing import Any

from eos.configuration.configuration_manager import ConfigurationManager
from eos.logging.logger import log
from eos.persistence.async_mongodb_interface import AsyncMongoDbInterface
from eos.resource_allocation.entities.device_allocation import (
    DeviceAllocation,
)
from eos.resource_allocation.exceptions import (
    EosDeviceAllocatedError,
    EosDeviceNotFoundError,
)
from eos.resource_allocation.repositories.device_allocation_repository import DeviceAllocationRepository


class DeviceAllocator:
    """
    Responsible for allocating devices to "owners".
    An owner may be an experiment task, a human, etc. A device can only be held by one owner at a time.
    """

    def __init__(
        self,
        configuration_manager: ConfigurationManager,
        db_interface: AsyncMongoDbInterface,
    ):
        self._configuration_manager = configuration_manager
        self._session_factory = db_interface.session_factory
        self._allocations = None

    async def initialize(self, db_interface: AsyncMongoDbInterface) -> None:
        self._allocations = DeviceAllocationRepository(db_interface)
        await self._allocations.initialize()
        log.debug("Device allocator initialized.")

    async def allocate(self, lab_id: str, device_id: str, owner: str, experiment_id: str | None = None) -> None:
        """
        Allocate a device to an owner.
        """
        if await self.is_allocated(lab_id, device_id):
            raise EosDeviceAllocatedError(f"Device '{device_id}' in lab '{lab_id}' is already allocated.")

        device_config = self._get_device_config(lab_id, device_id)
        allocation = DeviceAllocation(
            id=device_id,
            lab_id=device_config["lab_id"],
            owner=owner,
            device_type=device_config["type"],
            experiment_id=experiment_id,
        )
        await self._allocations.create(allocation.model_dump())

    async def deallocate(self, lab_id: str, device_id: str) -> None:
        """
        Deallocate a device.
        """
        result = await self._allocations.delete_one(lab_id=lab_id, id=device_id)
        if result.deleted_count == 0:
            log.warning(f"Device '{device_id}' in lab '{lab_id}' is not allocated. No action taken.")
        else:
            log.debug(f"Deallocated device '{device_id}' in lab '{lab_id}'.")

    async def is_allocated(self, lab_id: str, device_id: str) -> bool:
        """
        Check if a device is allocated.
        """
        self._get_device_config(lab_id, device_id)
        return await self._allocations.exists(lab_id=lab_id, id=device_id)

    async def get_allocation(self, lab_id: str, device_id: str) -> DeviceAllocation | None:
        """
        Get the allocation details of a device.
        """
        self._get_device_config(lab_id, device_id)
        allocation = await self._allocations.get_one(lab_id=lab_id, id=device_id)
        return DeviceAllocation(**allocation) if allocation else None

    async def get_allocations(self, **query: dict[str, Any]) -> list[DeviceAllocation]:
        """
        Query device allocations with arbitrary parameters.
        """
        allocations = await self._allocations.get_all(**query)
        return [DeviceAllocation(**allocation) for allocation in allocations]

    async def get_all_unallocated(self) -> list[str]:
        """
        Get all unallocated devices.
        """
        allocated_devices = [allocation.id for allocation in await self.get_allocations()]
        all_devices = [
            device_id for lab_config in self._configuration_manager.labs.values() for device_id in lab_config.devices
        ]
        return list(set(all_devices) - set(allocated_devices))

    async def deallocate_all_by_owner(self, owner: str) -> None:
        """
        Deallocate all devices allocated to an owner.
        """
        result = await self._allocations.delete_many(owner=owner)
        if result.deleted_count == 0:
            log.warning(f"Owner '{owner}' has no devices allocated. No action taken.")
        else:
            log.debug(f"Deallocated {result.deleted_count} devices for owner '{owner}'.")

    async def deallocate_all(self) -> None:
        """
        Deallocate all devices.
        """
        result = await self._allocations.delete_all()
        log.debug(f"Deallocated all {result.deleted_count} devices.")

    def _get_device_config(self, lab_id: str, device_id: str) -> dict[str, Any]:
        lab = self._configuration_manager.labs.get(lab_id)
        for dev_id, device_config in lab.devices.items():
            if dev_id == device_id:
                return {
                    "lab_id": lab.type,
                    "type": device_config.type,
                }

        raise EosDeviceNotFoundError(f"Device '{device_id}' in lab '{lab_id}' not found in the configuration.")
