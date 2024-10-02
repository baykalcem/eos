from pathlib import Path

from eos.configuration.exceptions import EosMissingConfigurationError
from eos.configuration.packages.package import Package


class PackageValidator:
    """
    Responsible for validating user-defined packages.
    """

    def __init__(self, user_dir: Path, packages: dict[str, Package]):
        self._user_dir = user_dir
        self._packages = packages

    def validate(self) -> None:
        if not self._packages:
            raise EosMissingConfigurationError(f"No valid packages found in the user directory '{self._user_dir}'")
