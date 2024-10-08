from dataclasses import dataclass

from ray.actor import ActorHandle

from eos.utils.ray_utils import RayActorWrapper


@dataclass(frozen=True)
class DeviceActorReference:
    id: str
    lab_id: str
    type: str
    actor_handle: ActorHandle


@dataclass(frozen=True)
class DeviceActorWrapperReference:
    id: str
    lab_id: str
    type: str
    actor_wrapper: RayActorWrapper


class DeviceActorWrapperRegistry:
    def __init__(self, devices: list[DeviceActorReference]):
        self._devices_by_lab_and_id: dict[tuple[str, str], DeviceActorWrapperReference] = {}
        self._devices_by_lab_id: dict[str, list[DeviceActorWrapperReference]] = {}
        self._devices_by_type: dict[str, list[DeviceActorWrapperReference]] = {}

        for device in devices:
            device_wrapper_reference = DeviceActorWrapperReference(
                id=device.id,
                lab_id=device.lab_id,
                type=device.type,
                actor_wrapper=RayActorWrapper(device.actor_handle),
            )
            self._devices_by_lab_and_id[(device.lab_id, device.id)] = device_wrapper_reference
            self._devices_by_lab_id.setdefault(device.lab_id, []).append(device_wrapper_reference)
            self._devices_by_type.setdefault(device.type, []).append(device_wrapper_reference)

    def get(self, lab_id: str, device_id: str) -> RayActorWrapper | None:
        device = self._devices_by_lab_and_id.get((lab_id, device_id))
        return device.actor_wrapper if device else None

    def get_all_by_lab_id(self, lab_id: str) -> list[RayActorWrapper]:
        return [device.actor_wrapper for device in self._devices_by_lab_id.get(lab_id, [])]

    def get_all_by_type(self, device_type: str) -> list[RayActorWrapper]:
        return [device.actor_wrapper for device in self._devices_by_type.get(device_type, [])]
