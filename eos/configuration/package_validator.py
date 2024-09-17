from eos.configuration.exceptions import EosMissingConfigurationError
from eos.configuration.package import Package


class PackageValidator:
    """
    Responsible for validating user-defined packages.
    """

    def __init__(self, user_dir: str, packages: dict[str, Package]):
        self.user_dir = user_dir
        self.packages = packages

    def validate(self) -> None:
        if not self.packages:
            raise EosMissingConfigurationError(f"No valid packages found in the user directory '{self.user_dir}'")
