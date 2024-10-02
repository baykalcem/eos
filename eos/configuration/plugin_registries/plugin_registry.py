import importlib
import inspect
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Generic, TypeVar

from eos.configuration.packages.entities import EntityType
from eos.configuration.packages.package_manager import PackageManager
from eos.logging.batch_error_logger import batch_error, raise_batched_errors
from eos.logging.logger import log

T = TypeVar("T")
S = TypeVar("S")  # Specification registry type


@dataclass
class PluginRegistryConfig:
    spec_registry: S
    base_class: type[T]
    config_file_name: str
    implementation_file_name: str
    class_suffix: str
    not_found_exception_class: type[Exception]
    entity_type: EntityType


class PluginRegistry(Generic[T, S]):
    """
    A generic registry for dynamically discovering and loading plugin-like implementation classes.
    """

    def __init__(self, package_manager: PackageManager, config: PluginRegistryConfig):
        self._package_manager = package_manager
        self._config = config
        self._plugin_types: dict[str, type[T]] = {}
        self._plugin_modules: dict[str, str] = {}  # Maps type_name to module path

        self._load_plugin_modules()

    def get_plugin_class_type(self, type_name: str) -> type[T]:
        """Get the plugin class type for the given type name."""
        if type_name not in self._plugin_types:
            raise self._config.not_found_exception_class(f"Plugin implementation for '{type_name}' not found.")

        return self._plugin_types[type_name]

    def _load_plugin_modules(self) -> None:
        self._plugin_types.clear()
        self._plugin_modules.clear()

        for package in self._package_manager.get_all_packages():
            self._load_package_plugins(package)

        raise_batched_errors(root_exception_type=self._config.not_found_exception_class)

    def _load_package_plugins(self, package) -> None:
        directory = package.get_entity_dir(self._config.entity_type)
        if not Path(directory).is_dir():
            return

        for current_dir, _, files in os.walk(directory):
            if self._config.config_file_name not in files:
                continue

            dir_path = Path(current_dir).relative_to(Path(directory))
            implementation_file = Path(current_dir) / self._config.implementation_file_name

            self._load_single_plugin(package.name, dir_path, implementation_file)

    def _load_single_plugin(self, package_name: str, dir_path: Path, implementation_file: Path) -> None:
        if not implementation_file.exists():
            batch_error(
                f"Implementation file '{implementation_file}' for package '{package_name}' not found.",
                self._config.not_found_exception_class,
            )
            return

        module_name = Path(dir_path).name

        try:
            spec = importlib.util.spec_from_file_location(module_name, implementation_file)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        except Exception as e:
            batch_error(
                f"Failed to load module '{module_name}' from '{implementation_file}': {e!s}",
                self._config.not_found_exception_class,
            )
            return

        found_implementation_class = False
        for name, obj in module.__dict__.items():
            if inspect.isclass(obj) and obj is not self._config.base_class and name.endswith(self._config.class_suffix):
                type_name = self._config.spec_registry.get_spec_by_dir(Path(package_name) / dir_path)
                self._plugin_types[type_name] = obj
                self._plugin_modules[type_name] = str(implementation_file)
                found_implementation_class = True
                log.debug(
                    f"Loaded {self._config.class_suffix.lower()} plugin '{name}' for type '{type_name}' from package "
                    f"'{package_name}'"
                )
                break

        if not found_implementation_class:
            batch_error(
                f"{self._config.class_suffix} implementation class for '{module_name}' in package '{package_name}'"
                f" not found. Make sure that its name ends in '{self._config.class_suffix}'.",
                self._config.not_found_exception_class,
            )

    def reload_plugin(self, type_name: str) -> None:
        if type_name not in self._plugin_modules:
            raise self._config.not_found_exception_class(f"Plugin '{type_name}' not found.")

        implementation_file = Path(self._plugin_modules[type_name])
        package_name = Path(implementation_file).parent.parent.name
        dir_path = Path(os.path.relpath(Path(implementation_file).parent, Path(implementation_file).parent.parent))

        self._load_single_plugin(package_name, dir_path, implementation_file)
        log.info(f"Reloaded plugin '{type_name}'")

    def reload_all_plugins(self) -> None:
        self._load_plugin_modules()
        log.info("Reloaded all plugins")
