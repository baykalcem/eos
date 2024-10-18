import atexit
import threading
from abc import ABC, abstractmethod, ABCMeta
from enum import Enum
from typing import Any

from eos.devices.exceptions import (
    EosDeviceInitializationError,
    EosDeviceCleanupError,
    EosDeviceError,
)


class DeviceStatus(Enum):
    DISABLED = "DISABLED"
    IDLE = "IDLE"
    BUSY = "BUSY"
    ERROR = "ERROR"


def capture_exceptions(func: callable) -> callable:
    def wrapper(self, *args, **kwargs) -> Any:
        try:
            return func(self, *args, **kwargs)

        except (
            EosDeviceInitializationError,
            EosDeviceCleanupError,
        ) as e:
            raise e
        except Exception as e:
            self._status = DeviceStatus.ERROR
            raise EosDeviceError(f"Error in the function '{func.__name__}' in device '{self._device_id}'.") from e

    return wrapper


class DeviceMeta(ABCMeta):
    def __new__(cls, name: str, bases: tuple, dct: dict):
        cls._add_exception_capture_to_child_methods(bases, dct)
        return super().__new__(cls, name, bases, dct)

    @staticmethod
    def _add_exception_capture_to_child_methods(bases: tuple, dct: dict) -> None:
        base_methods = set()
        for base in bases:
            if isinstance(base, DeviceMeta):
                base_methods.update(base.__dict__.keys())

        for attr, value in dct.items():
            if callable(value) and not attr.startswith("__") and attr not in base_methods:
                dct[attr] = capture_exceptions(value)


class BaseDevice(ABC, metaclass=DeviceMeta):
    """
    The base class for all devices in EOS.
    """

    def __init__(
        self,
        device_id: str,
        lab_id: str,
        device_type: str,
        initialization_parameters: dict[str, Any],
    ):
        self._device_id = device_id
        self._lab_id = lab_id
        self._device_type = device_type
        self._status = DeviceStatus.DISABLED
        self._initialization_parameters = initialization_parameters

        self._lock = threading.Lock()

        atexit.register(self.cleanup)
        self.initialize(initialization_parameters)

    def initialize(self, initialization_parameters: dict[str, Any]) -> None:
        """
        Initialize the device. After calling this method, the device is ready to be used for tasks
        and the status is IDLE.
        """
        with self._lock:
            if self._status != DeviceStatus.DISABLED:
                raise EosDeviceInitializationError(f"Device {self._device_id} is already initialized.")

            try:
                self._initialize(initialization_parameters)
                self._status = DeviceStatus.IDLE
            except Exception as e:
                self._status = DeviceStatus.ERROR
                raise EosDeviceInitializationError(
                    f"Error initializing device {self._device_id}: {e!s}",
                ) from e

    def cleanup(self) -> None:
        """
        Clean up the device. After calling this method, the device can no longer be used for tasks and the status is
        DISABLED.
        """
        with self._lock:
            if self._status == DeviceStatus.DISABLED:
                return

            if self._status == DeviceStatus.BUSY:
                raise EosDeviceCleanupError(
                    f"Device {self._device_id} is busy. Cannot perform cleanup.",
                )

            try:
                self._cleanup()
                self._status = DeviceStatus.DISABLED
            except Exception as e:
                self._status = DeviceStatus.ERROR
                raise EosDeviceCleanupError(f"Error cleaning up device {self._device_id}: {e!s}") from e

    def enable(self) -> None:
        """
        Enable the device. The status should be IDLE after calling this method.
        """
        if self._status == DeviceStatus.DISABLED:
            self.initialize(self._initialization_parameters)

    def disable(self) -> None:
        """
        Disable the device. The status should be DISABLED after calling this method.
        """
        if self._status != DeviceStatus.DISABLED:
            self.cleanup()

    def report(self) -> dict[str, Any]:
        """
        Return a dictionary with any member variables needed for logging purposes and progress tracking.
        """
        return self._report()

    def report_status(self) -> dict[str, Any]:
        """
        Return a dictionary with the id and status of the task handler.
        """
        return {
            "id": self._device_id,
            "status": self._status,
        }

    @property
    def id(self) -> str:
        return self._device_id

    @property
    def type(self) -> str:
        return self._device_type

    @property
    def status(self) -> DeviceStatus:
        return self._status

    @abstractmethod
    def _initialize(self, initialization_parameters: dict[str, Any]) -> None:
        """
        Implementation for the initialization of the device.
        """

    @abstractmethod
    def _cleanup(self) -> None:
        """
        Implementation for the cleanup of the device.
        """

    @abstractmethod
    def _report(self) -> dict[str, Any]:
        """
        Implementation for the report method.
        """
