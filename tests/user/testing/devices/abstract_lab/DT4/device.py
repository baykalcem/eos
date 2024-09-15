from typing import Any

from eos.devices.base_device import BaseDevice


class DT4Device(BaseDevice):
    def _initialize(self, initialization_parameters: dict[str, Any]) -> None:
        pass

    def _cleanup(self) -> None:
        pass

    def _report(self) -> dict[str, Any]:
        pass
