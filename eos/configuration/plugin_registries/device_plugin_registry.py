from eos.configuration.constants import DEVICE_CONFIG_FILE_NAME, DEVICE_IMPLEMENTATION_FILE_NAME
from eos.configuration.package_manager import PackageManager
from eos.configuration.plugin_registries.plugin_registry import PluginRegistry, PluginRegistryConfig
from eos.configuration.spec_registries.device_specification_registry import DeviceSpecificationRegistry
from eos.devices.base_device import BaseDevice
from eos.devices.exceptions import EosDeviceClassNotFoundError


class DevicePluginRegistry(PluginRegistry[BaseDevice, DeviceSpecificationRegistry]):
    def __init__(self, package_manager: PackageManager):
        config = PluginRegistryConfig(
            spec_registry=DeviceSpecificationRegistry(),
            base_class=BaseDevice,
            config_file_name=DEVICE_CONFIG_FILE_NAME,
            implementation_file_name=DEVICE_IMPLEMENTATION_FILE_NAME,
            class_suffix="Device",
            error_class=EosDeviceClassNotFoundError,
            directory_name="devices_dir",
        )
        super().__init__(package_manager, config)

    def get_device_class_type(self, device_type: str) -> type[BaseDevice]:
        return self.get_plugin_class_type(device_type)
