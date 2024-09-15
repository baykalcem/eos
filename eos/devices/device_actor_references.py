from dataclasses import dataclass

from ray.actor import ActorHandle

from eos.utils.ray_utils import RayActorWrapper


@dataclass(frozen=True)
class DeviceRayActorReference:
    id: str
    lab_id: str
    type: str
    actor_handle: ActorHandle


@dataclass(frozen=True)
class DeviceRayActorWrapperReference:
    id: str
    lab_id: str
    type: str
    ray_actor_wrapper: RayActorWrapper


class DeviceRayActorWrapperReferences:
    def __init__(self, devices: list[DeviceRayActorReference]):
        self._devices_by_lab_and_id: dict[tuple[str, str], DeviceRayActorWrapperReference] = {}
        self._devices_by_lab_id: dict[str, list[DeviceRayActorWrapperReference]] = {}
        self._devices_by_type: dict[str, list[DeviceRayActorWrapperReference]] = {}

        for device in devices:
            device_actor_wrapper_reference = DeviceRayActorWrapperReference(
                id=device.id,
                lab_id=device.lab_id,
                type=device.type,
                ray_actor_wrapper=RayActorWrapper(device.actor_handle),
            )
            self._devices_by_lab_and_id[(device.lab_id, device.id)] = device_actor_wrapper_reference

            if device.lab_id not in self._devices_by_lab_id:
                self._devices_by_lab_id[device.lab_id] = []
            self._devices_by_lab_id[device.lab_id].append(device_actor_wrapper_reference)

            if device.type not in self._devices_by_type:
                self._devices_by_type[device.type] = []
            self._devices_by_type[device.type].append(device_actor_wrapper_reference)

    def get(self, lab_id: str, device_id: str) -> RayActorWrapper | None:
        device = self._devices_by_lab_and_id.get((lab_id, device_id))
        return device.ray_actor_wrapper if device else None

    def get_all_by_lab_id(self, lab_id: str) -> list[RayActorWrapper]:
        devices = self._devices_by_lab_id.get(lab_id, [])
        return [device.ray_actor_wrapper for device in devices]

    def get_all_by_type(self, device_type: str) -> list[RayActorWrapper]:
        devices = self._devices_by_type.get(device_type, [])
        return [device.ray_actor_wrapper for device in devices]
