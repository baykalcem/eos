from typing import Any

from eos.devices.base_device import BaseDevice


class MultiplierDevice(BaseDevice):
    def _initialize(self, initialization_parameters: dict[str, Any]) -> None:
        pass

    def _cleanup(self) -> None:
        pass

    def _report(self) -> dict[str, Any]:
        pass

    def multiply(self, a: int, b: int) -> int:
        return a * b
