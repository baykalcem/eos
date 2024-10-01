import os
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import TypeVar, Generic, Any

import jinja2
import yaml
from omegaconf import OmegaConf, ValidationError

from eos.configuration.constants import (
    LABS_DIR,
    EXPERIMENTS_DIR,
    TASKS_DIR,
    DEVICES_DIR,
    LAB_CONFIG_FILE_NAME,
    EXPERIMENT_CONFIG_FILE_NAME,
    DEVICE_CONFIG_FILE_NAME,
    TASK_CONFIG_FILE_NAME,
)
from eos.configuration.entities.device_specification import DeviceSpecification
from eos.configuration.entities.experiment import ExperimentConfig
from eos.configuration.entities.lab import LabConfig
from eos.configuration.entities.task_specification import TaskSpecification
from eos.configuration.exceptions import EosConfigurationError, EosMissingConfigurationError
from eos.configuration.package import Package
from eos.configuration.package_validator import PackageValidator
from eos.logging.logger import log

T = TypeVar("T")


class EntityType(Enum):
    LAB = auto()
    EXPERIMENT = auto()
    TASK = auto()
    DEVICE = auto()


@dataclass
class EntityInfo:
    dir_name: str
    config_file_name: str
    config_type: type


@dataclass
class EntityLocationInfo:
    package_name: str
    entity_path: str


ENTITY_INFO: dict[EntityType, EntityInfo] = {
    EntityType.LAB: EntityInfo(LABS_DIR, LAB_CONFIG_FILE_NAME, LabConfig),
    EntityType.EXPERIMENT: EntityInfo(EXPERIMENTS_DIR, EXPERIMENT_CONFIG_FILE_NAME, ExperimentConfig),
    EntityType.TASK: EntityInfo(TASKS_DIR, TASK_CONFIG_FILE_NAME, TaskSpecification),
    EntityType.DEVICE: EntityInfo(DEVICES_DIR, DEVICE_CONFIG_FILE_NAME, DeviceSpecification),
}
ConfigType = LabConfig | ExperimentConfig | TaskSpecification | DeviceSpecification


class EntityConfigReader(Generic[T]):
    """
    Reads and parses entity configurations from files.

    The EntityConfigReader class provides static methods to read and parse configuration
    files for various entity types (labs, experiments, tasks, and devices) in the EOS system.
    It handles the loading, validation, and structuring of configuration data using OmegaConf.
    """

    @staticmethod
    def read_entity_config(user_dir: str, file_path: str, entity_type: EntityType) -> ConfigType:
        entity_info = ENTITY_INFO[entity_type]
        return EntityConfigReader._read_config(user_dir, file_path, entity_info.config_type, f"{entity_type.name}")

    @staticmethod
    def read_all_entity_configs(user_dir: str, base_dir: str, entity_type: EntityType) -> tuple[
        dict[str, ConfigType], dict[str, str]]:
        entity_info = ENTITY_INFO[entity_type]
        configs = {}
        dirs_to_types = {}

        for root, _, files in os.walk(base_dir):
            if entity_info.config_file_name not in files:
                continue

            entity_subdir = Path(root).relative_to(base_dir)
            config_file_path = Path(root) / entity_info.config_file_name

            try:
                structured_config = EntityConfigReader.read_entity_config(user_dir, str(config_file_path), entity_type)
                entity_type_name = structured_config.type
                configs[entity_type_name] = structured_config
                dirs_to_types[entity_subdir] = entity_type_name

                log.debug(
                    f"Loaded {entity_type.name.capitalize()} specification from directory '{entity_subdir}' of type "
                    f"'{entity_type_name}'"
                )
                log.debug(f"{entity_type.name.capitalize()} configuration '{entity_type_name}': {structured_config}")
            except EosConfigurationError as e:
                log.error(f"Error loading {entity_type.name.lower()} configuration from '{config_file_path}': {e}")
                raise

        return configs, dirs_to_types

    @staticmethod
    def _read_config(user_dir: str, file_path: str, config_type: type[ConfigType], config_name: str) -> ConfigType:
        try:
            config_data = EntityConfigReader._process_jinja_yaml(user_dir, file_path)

            structured_config = OmegaConf.merge(OmegaConf.structured(config_type), OmegaConf.create(config_data))
            _ = OmegaConf.to_object(structured_config)

            return structured_config
        except OSError as e:
            raise EosConfigurationError(f"Error reading configuration file '{file_path}': {e!s}") from e
        except ValidationError as e:
            raise EosConfigurationError(f"Configuration is invalid: {e!s}") from e
        except jinja2.exceptions.TemplateError as e:
            raise EosConfigurationError(f"Error in Jinja2 template processing for '{config_name}': {e!s}") from e
        except Exception as e:
            raise EosConfigurationError(f"Error processing {config_name} configuration: {e!s}") from e

    @staticmethod
    def _process_jinja_yaml(user_dir: str, file_path: str) -> dict[str, Any]:
        """
        Process a YAML file with Jinja2 templating, without passing any variables.

        This method:
        1. Reads the YAML file
        2. Renders the Jinja2 template without any variables
        3. Parses the rendered content back into a Python dictionary
        """
        try:
            with Path(file_path).open() as f:
                raw_content = f.read()
        except OSError as e:
            raise EosConfigurationError(f"Error reading file '{file_path}': {e}") from e

        try:
            env = jinja2.Environment(
                loader=jinja2.FileSystemLoader(Path(user_dir)),  # user directory
                undefined=jinja2.StrictUndefined,
                autoescape=True,
            )

            template = env.from_string(raw_content)
            rendered_content = template.render()

            return yaml.safe_load(rendered_content)
        except yaml.YAMLError as e:
            raise EosConfigurationError(f"Error parsing YAML in {file_path}: {e}") from e
        except jinja2.exceptions.TemplateError as e:
            raise EosConfigurationError(f"Error in Jinja2 template processing: {e}") from e


class PackageDiscoverer:
    """
    Discovers packages in the user directory.
    """

    def __init__(self, user_dir: str):
        self.user_dir = Path(user_dir)

    def discover_packages(self) -> dict[str, Package]:
        packages = {}
        if not self.user_dir.is_dir():
            raise EosMissingConfigurationError(f"User directory '{self.user_dir}' does not exist")

        for item in os.listdir(self.user_dir):
            package_path = self.user_dir / item

            if package_path.is_dir():
                packages[item] = Package(item, str(package_path))

        return packages


class PackageManager:
    """
    Manages packages and entity configurations within the user directory.

    The PackageManager class provides facilities to discover, read, add, and remove packages,
    as well as read entity configurations (labs, experiments, tasks, and devices) from these packages.
    It also maintains efficient lookup indices for quick access to entities across all packages.
    """

    def __init__(self, user_dir: str):
        self.user_dir = user_dir

        self.packages: dict[str, Package] = {}
        self.entity_indices: dict[EntityType, dict[str, EntityLocationInfo]] = {
            entity_type: {} for entity_type in EntityType
        }

        self._discover_packages()
        log.info(f"Found packages: {', '.join(self.packages.keys())}")

        log.debug("Package manager initialized")

    def read_lab_config(self, lab_name: str) -> LabConfig:
        entity_location = self._get_entity_location(lab_name, EntityType.LAB)
        config_file_path = self._get_config_file_path(entity_location, EntityType.LAB)
        return EntityConfigReader.read_entity_config(self.user_dir, config_file_path, EntityType.LAB)

    def read_experiment_config(self, experiment_name: str) -> ExperimentConfig:
        entity_location = self._get_entity_location(experiment_name, EntityType.EXPERIMENT)
        config_file_path = self._get_config_file_path(entity_location, EntityType.EXPERIMENT)
        return EntityConfigReader.read_entity_config(self.user_dir, config_file_path, EntityType.EXPERIMENT)

    def read_task_configs(self) -> tuple[dict[str, TaskSpecification], dict[str, str]]:
        return self._read_all_entity_configs(EntityType.TASK)

    def read_device_configs(self) -> tuple[dict[str, DeviceSpecification], dict[str, str]]:
        return self._read_all_entity_configs(EntityType.DEVICE)

    def get_package(self, name: str) -> Package | None:
        return self.packages.get(name)

    def get_all_packages(self) -> list[Package]:
        return list(self.packages.values())

    def _find_package_for_entity(self, entity_name: str, entity_type: EntityType) -> Package | None:
        entity_location = self.entity_indices[entity_type].get(entity_name)
        if entity_location:
            return self.packages.get(entity_location.package_name)
        return None

    def find_package_for_lab(self, lab_name: str) -> Package | None:
        return self._find_package_for_entity(lab_name, EntityType.LAB)

    def find_package_for_experiment(self, experiment_name: str) -> Package | None:
        return self._find_package_for_entity(experiment_name, EntityType.EXPERIMENT)

    def find_package_for_task(self, task_name: str) -> Package | None:
        return self._find_package_for_entity(task_name, EntityType.TASK)

    def find_package_for_device(self, device_name: str) -> Package | None:
        return self._find_package_for_entity(device_name, EntityType.DEVICE)

    def get_entity_dir(self, entity_name: str, entity_type: EntityType) -> Path:
        entity_location = self._get_entity_location(entity_name, entity_type)
        package = self.packages[entity_location.package_name]
        return Path(getattr(package, f"{ENTITY_INFO[entity_type].dir_name}_dir") / entity_location.entity_path)

    def get_experiments_in_package(self, package_name: str) -> list[str]:
        package = self.get_package(package_name)
        if not package:
            raise EosMissingConfigurationError(f"Package '{package_name}' not found")

        return [
            entity_name
            for entity_name, location in self.entity_indices[EntityType.EXPERIMENT].items()
            if location.package_name == package_name
        ]

    def get_labs_in_package(self, package_name: str) -> list[str]:
        package = self.get_package(package_name)
        if not package:
            raise EosMissingConfigurationError(f"Package '{package_name}' not found")

        return [
            entity_name
            for entity_name, location in self.entity_indices[EntityType.LAB].items()
            if location.package_name == package_name
        ]

    def get_tasks_in_package(self, package_name: str) -> list[str]:
        package = self.get_package(package_name)
        if not package:
            raise EosMissingConfigurationError(f"Package '{package_name}' not found")

        return [
            entity_name
            for entity_name, location in self.entity_indices[EntityType.TASK].items()
            if location.package_name == package_name
        ]

    def get_devices_in_package(self, package_name: str) -> list[str]:
        package = self.get_package(package_name)
        if not package:
            raise EosMissingConfigurationError(f"Package '{package_name}' not found")

        return [
            entity_name
            for entity_name, location in self.entity_indices[EntityType.DEVICE].items()
            if location.package_name == package_name
        ]

    def add_package(self, package_name: str) -> None:
        package_path = Path(self.user_dir) / package_name
        if not package_path.is_dir():
            raise EosMissingConfigurationError(f"Package directory '{package_path}' does not exist")

        new_package = Package(package_name, str(package_path))
        PackageValidator(self.user_dir, {package_name: new_package}).validate()

        self.packages[package_name] = new_package
        self._update_entity_indices(new_package)

        log.info(f"Added package '{package_name}'")

    def remove_package(self, package_name: str) -> None:
        if package_name not in self.packages:
            raise EosMissingConfigurationError(f"Package '{package_name}' not found")

        package = self.packages[package_name]
        del self.packages[package_name]
        self._remove_package_from_indices(package)

        log.info(f"Removed package '{package_name}'")

    def _discover_packages(self) -> None:
        self.packages = PackageDiscoverer(self.user_dir).discover_packages()
        PackageValidator(self.user_dir, self.packages).validate()
        self._build_entity_indices()

    def _build_entity_indices(self) -> None:
        for entity_type in EntityType:
            self.entity_indices[entity_type] = {}
            for package_name, package in self.packages.items():
                entity_dir = Path(getattr(package, f"{ENTITY_INFO[entity_type].dir_name}_dir"))
                if entity_dir.is_dir():
                    self._index_entities(entity_type, package_name, str(entity_dir))

    def _index_entities(self, entity_type: EntityType, package_name: str, entity_dir: str) -> None:
        for root, _, files in os.walk(entity_dir):
            if ENTITY_INFO[entity_type].config_file_name in files:
                entity_path = Path(root).relative_to(Path(entity_dir))
                entity_name = Path(entity_path).name
                self.entity_indices[entity_type][entity_name] = EntityLocationInfo(package_name, str(entity_path))

    def _update_entity_indices(self, package: Package) -> None:
        for entity_type in EntityType:
            entity_dir = Path(getattr(package, f"{ENTITY_INFO[entity_type].dir_name}_dir"))
            if entity_dir.is_dir():
                self._index_entities(entity_type, package.name, str(entity_dir))

    def _remove_package_from_indices(self, package: Package) -> None:
        for entity_type in EntityType:
            self.entity_indices[entity_type] = {
                entity_name: location
                for entity_name, location in self.entity_indices[entity_type].items()
                if location.package_name != package.name
            }

    def _get_entity_location(self, entity_name: str, entity_type: EntityType) -> EntityLocationInfo:
        entity_location = self.entity_indices[entity_type].get(entity_name)
        if not entity_location:
            raise EosMissingConfigurationError(f"{entity_type.name.capitalize()} '{entity_name}' not found")
        return entity_location

    def _get_config_file_path(self, entity_location: EntityLocationInfo, entity_type: EntityType) -> str:
        entity_info = ENTITY_INFO[entity_type]
        package = self.packages[entity_location.package_name]
        config_file_path = (
            Path(getattr(package, f"{entity_info.dir_name}_dir"))
            / entity_location.entity_path
            / entity_info.config_file_name
        )

        if not config_file_path.is_file():
            raise EosMissingConfigurationError(
                f"{entity_type.name.capitalize()} file '{entity_info.config_file_name}' does not exist for "
                f"'{entity_location.entity_path}'",
                EosMissingConfigurationError,
            )

        return str(config_file_path)

    def _read_all_entity_configs(self, entity_type: EntityType) -> tuple[dict[str, T], dict[str, str]]:
        all_configs = {}
        all_dirs_to_types = {}
        for package in self.packages.values():
            entity_dir = Path(getattr(package, f"{ENTITY_INFO[entity_type].dir_name}_dir"))
            if not entity_dir.is_dir():
                continue
            configs, dirs_to_types = EntityConfigReader.read_all_entity_configs(self.user_dir, str(entity_dir),
                                                                                entity_type)
            all_configs.update(configs)
            all_dirs_to_types.update({Path(package.name) / k: v for k, v in dirs_to_types.items()})
        return all_configs, all_dirs_to_types
