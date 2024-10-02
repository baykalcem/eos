from typing import Any

import ray
from omegaconf import OmegaConf
from ray.actor import ActorHandle

from eos.configuration.configuration_manager import ConfigurationManager
from eos.configuration.constants import EOS_COMPUTER_NAME
from eos.devices.entities.device import Device, DeviceStatus
from eos.devices.exceptions import EosDeviceStateError, EosDeviceInitializationError
from eos.logging.batch_error_logger import batch_error, raise_batched_errors
from eos.logging.logger import log
from eos.persistence.db_manager import DbManager
from eos.persistence.mongo_repository import MongoRepository


class DeviceManager:
    """
    Provides methods for interacting with the devices in a lab.
    """

    def __init__(self, configuration_manager: ConfigurationManager, db_manager: DbManager):
        self._configuration_manager = configuration_manager

        self._devices = MongoRepository("devices", db_manager)
        self._devices.create_indices([("lab_id", 1), ("id", 1)], unique=True)

        self._device_plugin_registry = configuration_manager.devices
        self._device_actor_handles: dict[str, ActorHandle] = {}
        self._device_actor_computer_ips: dict[str, str] = {}

        log.debug("Device manager initialized.")

    def get_device(self, lab_id: str, device_id: str) -> Device | None:
        """
        Get a device by its ID.
        """
        device = self._devices.get_one(lab_id=lab_id, id=device_id)
        if not device:
            return None
        return Device(**device)

    def get_devices(self, **query: dict[str, Any]) -> list[Device]:
        """
        Query devices with arbitrary parameters.

        :param query: Dictionary of query parameters.
        """
        devices = self._devices.get_all(**query)
        return [Device(**device) for device in devices]

    def set_device_status(self, lab_id: str, device_id: str, status: DeviceStatus) -> None:
        """
        Set the status of a device.
        """
        if not self._devices.exists(lab_id=lab_id, id=device_id):
            raise EosDeviceStateError(f"Device '{device_id}' in lab '{lab_id}' does not exist.")

        self._devices.update({"status": status.value}, lab_id=lab_id, id=device_id)

    def get_device_actor(self, lab_id: str, device_id: str) -> ActorHandle:
        """
        Get the actor handle of a device.
        """
        actor_id = f"{lab_id}.{device_id}"
        if actor_id not in self._device_actor_handles:
            raise EosDeviceInitializationError(f"Device actor '{actor_id}' does not exist.")

        return self._device_actor_handles.get(actor_id)

    def update_devices(self, loaded_labs: set[str] | None = None, unloaded_labs: set[str] | None = None) -> None:
        if unloaded_labs:
            for lab_id in unloaded_labs:
                self._remove_devices_for_lab(lab_id)

        if loaded_labs:
            for lab_id in loaded_labs:
                self._create_devices_for_lab(lab_id)

        self._check_device_actors_healthy()
        log.debug("Devices have been updated.")

    def cleanup_device_actors(self) -> None:
        for actor in self._device_actor_handles.values():
            ray.kill(actor)
        self._device_actor_handles.clear()
        self._device_actor_computer_ips.clear()
        self._devices.delete()
        log.info("All device actors have been cleaned up.")

    def _remove_devices_for_lab(self, lab_id: str) -> None:
        devices_to_remove = self.get_devices(lab_id=lab_id)
        for device in devices_to_remove:
            actor_id = device.get_actor_id()
            if actor_id in self._device_actor_handles:
                ray.kill(self._device_actor_handles[actor_id])
                del self._device_actor_handles[actor_id]
                del self._device_actor_computer_ips[actor_id]
        self._devices.delete(lab_id=lab_id)
        log.debug(f"Removed devices for lab '{lab_id}'")

    def _create_devices_for_lab(self, lab_id: str) -> None:
        lab_config = self._configuration_manager.labs[lab_id]
        for device_id, device_config in lab_config.devices.items():
            device = self.get_device(lab_id, device_id)

            if device and device.get_actor_id() in self._device_actor_handles:
                continue

            if device and device.actor_handle:
                self._restore_device_actor(device)
            else:
                device = Device(
                    lab_id=lab_id,
                    id=device_id,
                    type=device_config.type,
                    location=device_config.location,
                    computer=device_config.computer,
                )
                self._devices.update(device.model_dump(), lab_id=lab_id, id=device_id)
                self._create_device_actor(device)

        log.debug(f"Created devices for lab '{lab_id}'")

    def _restore_device_actor(self, device: Device) -> None:
        """
        Restore a device actor registered in the database by looking up its actor in the Ray cluster.
        """
        device_actor_id = device.get_actor_id()
        device_config = self._configuration_manager.labs[device.lab_id].devices[device.id]
        self._device_actor_handles[device_actor_id] = ray.get_actor(device_actor_id)
        self._device_actor_computer_ips[device_actor_id] = (
            self._configuration_manager.labs[device.lab_id].computers[device_config.computer].ip
        )
        log.debug(f"Restored device actor {device_actor_id}")

    def _create_device_actor(self, device: Device) -> None:
        lab_config = self._configuration_manager.labs[device.lab_id]
        device_config = lab_config.devices[device.id]
        computer_name = device_config.computer.lower()

        computer_ip = "127.0.0.1" if computer_name == EOS_COMPUTER_NAME else lab_config.computers[computer_name].ip

        device_actor_id = device.get_actor_id()
        self._device_actor_computer_ips[device_actor_id] = computer_ip

        spec_initialization_parameters = (
            self._configuration_manager.device_specs.get_spec_by_type(device.type).initialization_parameters or {}
        )
        if spec_initialization_parameters:
            spec_initialization_parameters = OmegaConf.to_object(spec_initialization_parameters)

        device_config_initialization_parameters = device_config.initialization_parameters or {}
        if device_config_initialization_parameters:
            device_config_initialization_parameters = OmegaConf.to_object(device_config_initialization_parameters)

        initialization_parameters: dict[str, Any] = {
            **spec_initialization_parameters,
            **device_config_initialization_parameters,
        }

        resources = (
            {"eos-core": 0.0001} if computer_ip in ["localhost", "127.0.0.1"] else {f"node:{computer_ip}": 0.0001}
        )

        device_class = ray.remote(self._device_plugin_registry.get_device_class_type(device.type))
        self._device_actor_handles[device_actor_id] = device_class.options(
            name=device_actor_id,
            num_cpus=0,
            resources=resources,
        ).remote(device.id, device.lab_id, device.type, initialization_parameters)

    def _check_device_actors_healthy(self) -> None:
        status_reports = [actor_handle.report_status.remote() for actor_handle in self._device_actor_handles.values()]
        status_report_to_device_actor_id = {
            status_report: device_actor_id
            for device_actor_id, status_report in zip(self._device_actor_handles.keys(), status_reports, strict=False)
        }

        ready_status_reports, not_ready_status_reports = ray.wait(
            status_reports,
            num_returns=len(self._device_actor_handles),
            timeout=5,
        )

        for not_ready_ref in not_ready_status_reports:
            device_actor_id = status_report_to_device_actor_id[not_ready_ref]
            actor_handle = self._device_actor_handles[device_actor_id]
            computer_ip = self._device_actor_computer_ips[device_actor_id]

            ray.kill(actor_handle)

            batch_error(
                f"Device actor '{device_actor_id}' could not be reached on the computer {computer_ip}",
                EosDeviceInitializationError,
            )
        raise_batched_errors(EosDeviceInitializationError)
