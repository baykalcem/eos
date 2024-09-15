import os
from pathlib import Path

from eos.configuration.constants import (
    COMMON_DIR,
    EXPERIMENTS_DIR,
    LABS_DIR,
    DEVICES_DIR,
    TASKS_DIR,
    LAB_CONFIG_FILE_NAME,
    EXPERIMENT_CONFIG_FILE_NAME,
    TASK_CONFIG_FILE_NAME,
    TASK_IMPLEMENTATION_FILE_NAME,
    DEVICE_CONFIG_FILE_NAME,
    DEVICE_IMPLEMENTATION_FILE_NAME,
)
from eos.configuration.exceptions import EosMissingConfigurationError, EosConfigurationError
from eos.configuration.package import Package
from eos.logging.logger import log


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

        for package in self.packages.values():
            self._validate_package_structure(package)

    def _validate_package_structure(self, package: Package) -> None:
        """
        Validate the structure of a single package.
        """
        if not any(
            [
                package.common_dir.is_dir(),
                package.experiments_dir.is_dir(),
                package.labs_dir.is_dir(),
                package.devices_dir.is_dir(),
                package.tasks_dir.is_dir(),
            ]
        ):
            raise EosMissingConfigurationError(
                f"Package '{package.name}' does not contain any of the directories: "
                f"{COMMON_DIR}, {EXPERIMENTS_DIR}, {LABS_DIR}, {DEVICES_DIR}, {TASKS_DIR}"
            )

        if package.labs_dir.is_dir():
            self._validate_labs_dir(package)

        if package.experiments_dir.is_dir():
            self._validate_experiments_dir(package)

        if package.devices_dir.is_dir():
            self._validate_devices_dir(package)

        if package.tasks_dir.is_dir():
            self._validate_tasks_dir(package)

    @staticmethod
    def _validate_labs_dir(package: Package) -> None:
        """
        Validate the structure of the labs directory.
        """
        for file in os.listdir(package.labs_dir):
            file_path = package.labs_dir / file
            if not file_path.is_dir():
                raise EosConfigurationError(
                    f"Non-directory file found in '{package.labs_dir}'. Only lab directories are allowed."
                )

        for lab in os.listdir(package.labs_dir):
            lab_file_path = package.labs_dir / lab / LAB_CONFIG_FILE_NAME
            if not lab_file_path.is_file():
                raise EosMissingConfigurationError(f"Lab file '{LAB_CONFIG_FILE_NAME}' does not exist for lab '{lab}'")

            log.debug(f"Detected lab '{lab}' in package '{package.name}'")

    @staticmethod
    def _validate_experiments_dir(package: Package) -> None:
        """
        Validate the structure of the experiments directory.
        """
        for file in os.listdir(package.experiments_dir):
            file_path = package.experiments_dir / file
            if not file_path.is_dir():
                raise EosConfigurationError(
                    f"Non-directory file found in '{package.experiments_dir}'. Only experiment directories "
                    f"are allowed."
                )

            experiment_config_file = file_path / EXPERIMENT_CONFIG_FILE_NAME
            if not experiment_config_file.is_file():
                raise EosMissingConfigurationError(
                    f"Experiment configuration file '{EXPERIMENT_CONFIG_FILE_NAME}' does not exist for "
                    f"experiment '{file}'"
                )

            log.debug(f"Detected experiment '{file}' in package '{package.name}'")

    @staticmethod
    def _validate_tasks_dir(package: Package) -> None:
        """
        Validate the structure of the tasks directory.
        Ensure each subdirectory represents a task and contains the necessary files.
        """
        task_types = []
        for current_dir, _, files in os.walk(package.tasks_dir):
            if TASK_CONFIG_FILE_NAME not in files:
                continue

            task_dir = Path(current_dir)
            task_name = task_dir.relative_to(package.tasks_dir)

            config_file = task_dir / TASK_CONFIG_FILE_NAME
            implementation_file = task_dir / TASK_IMPLEMENTATION_FILE_NAME

            if not config_file.is_file():
                raise EosMissingConfigurationError(
                    f"Task configuration file '{TASK_CONFIG_FILE_NAME}' not found for task '{task_name}' "
                    f"in package '{package.name}'."
                )

            if not implementation_file.is_file():
                raise EosMissingConfigurationError(
                    f"Task implementation file '{TASK_IMPLEMENTATION_FILE_NAME}' not found for task "
                    f"'{task_name}' in package '{package.name}'."
                )

            task_types.append(task_dir)

        log.debug(f"Detected tasks '{task_types}' in package '{package.name}'")

    @staticmethod
    def _validate_devices_dir(package: Package) -> None:
        """
        Validate the structure of the devices directory.
        Ensure each subdirectory represents a device and contains the necessary files.
        """
        device_types = []
        for current_dir, _, files in os.walk(package.devices_dir):
            if DEVICE_CONFIG_FILE_NAME not in files:
                continue

            device_dir = Path(current_dir)
            device_name = device_dir.relative_to(package.devices_dir)

            config_file = device_dir / DEVICE_CONFIG_FILE_NAME
            implementation_file = device_dir / DEVICE_IMPLEMENTATION_FILE_NAME

            if not config_file.is_file():
                raise EosMissingConfigurationError(
                    f"Device configuration file '{DEVICE_CONFIG_FILE_NAME}' not found for device "
                    f"'{device_name}' in package '{package.name}'."
                )

            if not implementation_file.is_file():
                raise EosMissingConfigurationError(
                    f"Device implementation file '{DEVICE_IMPLEMENTATION_FILE_NAME}' not found for device "
                    f"'{device_name}' in package '{package.name}'."
                )

            device_types.append(device_dir)

        log.debug("Detected devices '%S' in package '%s'", device_types, package.name)
