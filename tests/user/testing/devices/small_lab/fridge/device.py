from typing import Any

from eos.devices.base_device import BaseDevice


class FridgeDevice(BaseDevice):
    async def _initialize(self, initialization_parameters: dict[str, Any]) -> None:
        pass

    async def _cleanup(self) -> None:
        pass

    async def _report(self) -> dict[str, Any]:
        pass
