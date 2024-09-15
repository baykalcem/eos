from eos.configuration.entities.device_specification import DeviceSpecification
from eos.configuration.entities.lab import LabDeviceConfig
from eos.configuration.spec_registries.specification_registry import SpecificationRegistry


class DeviceSpecificationRegistry(SpecificationRegistry[DeviceSpecification, LabDeviceConfig]):
    """
    The device specification registry stores the specifications for all devices that are available in EOS.
    """
