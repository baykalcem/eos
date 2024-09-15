from typing import Any

from eos.devices.base_device import BaseDevice


class AnalyzerDevice(BaseDevice):
    def _initialize(self, initialization_parameters: dict[str, Any]) -> None:
        pass

    def _cleanup(self) -> None:
        pass

    def _report(self) -> dict[str, Any]:
        pass

    def analyze_result(self, number: int, product: int) -> int:
        return number + 100 * abs(product - 1024)
