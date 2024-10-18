from typing import Any
from unittest.mock import Mock

import pytest
import ray

from eos.devices.base_device import BaseDevice, DeviceStatus
from eos.devices.exceptions import EosDeviceError, EosDeviceCleanupError, EosDeviceInitializationError


class MockDevice(BaseDevice):
    def __init__(self, device_id: str, lab_id: str, device_type: str, initialization_parameters: dict[str, Any]):
        self.mock_resource = None
        super().__init__(device_id, lab_id, device_type, initialization_parameters)

    def _initialize(self, initialization_parameters: dict[str, Any]) -> None:
        self.mock_resource = Mock()

    def _cleanup(self) -> None:
        if self.mock_resource:
            self.mock_resource.close()
            self.mock_resource = None

    def _report(self) -> dict[str, Any]:
        return {"mock_resource": str(self.mock_resource)}

    def raise_exception(self):
        raise ValueError("Test exception")


class TestBaseDevice:
    @pytest.fixture
    def mock_device(self):
        return MockDevice("test_device", "test_lab", "mock", {})

    def test_initialize(self, mock_device):
        assert mock_device.id == "test_device"
        assert mock_device.type == "mock"
        assert mock_device.status == DeviceStatus.IDLE
        assert mock_device.mock_resource is not None

    def test_cleanup(self, mock_device):
        mock_device.cleanup()
        assert mock_device.status == DeviceStatus.DISABLED
        assert mock_device.mock_resource is None

    def test_enable_disable(self, mock_device):
        mock_device.disable()
        assert mock_device.status == DeviceStatus.DISABLED
        mock_device.enable()
        assert mock_device.status == DeviceStatus.IDLE

    def test_report(self, mock_device):
        report = mock_device.report()
        assert "mock_resource" in report

    def test_report_status(self, mock_device):
        status_report = mock_device.report_status()
        assert status_report["id"] == "test_device"
        assert status_report["status"] == DeviceStatus.IDLE

    def test_exception_handling(self, mock_device):
        with pytest.raises(EosDeviceError):
            mock_device.raise_exception()
        assert mock_device.status == DeviceStatus.ERROR

    def test_initialization_error(self):
        class FailingDevice(MockDevice):
            def _initialize(self, initialization_parameters: dict[str, Any]) -> None:
                raise ValueError("Initialization failed")

        with pytest.raises(EosDeviceInitializationError):
            FailingDevice("fail_device", "test_lab", "failing", {})

    def test_cleanup_error(self, mock_device):
        mock_device.mock_resource.close.side_effect = Exception("Cleanup failed")
        with pytest.raises(EosDeviceError):
            mock_device.cleanup()
        assert mock_device.status == DeviceStatus.ERROR

    def test_busy_status_cleanup(self, mock_device):
        mock_device._status = DeviceStatus.BUSY
        with pytest.raises(EosDeviceCleanupError):
            mock_device.cleanup()
        assert mock_device.status == DeviceStatus.BUSY

    def test_double_initialization(self, mock_device):
        with pytest.raises(EosDeviceInitializationError):
            mock_device.initialize({})
        assert mock_device.status == DeviceStatus.IDLE
