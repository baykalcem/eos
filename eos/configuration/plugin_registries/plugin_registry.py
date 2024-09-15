import importlib
import inspect
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Generic, TypeVar

from eos.configuration.package_manager import PackageManager
from eos.logging.batch_error_logger import batch_error, raise_batched_errors
from eos.logging.logger import log
from eos.utils.singleton import Singleton

T = TypeVar("T")
S = TypeVar("S")  # Specification registry type


@dataclass
class PluginRegistryConfig:
    spec_registry: S
    base_class: type[T]
    config_file_name: str
    implementation_file_name: str
    class_suffix: str
    error_class: type[Exception]
    directory_name: str


class PluginRegistry(Generic[T, S], metaclass=Singleton):
    """
    A generic registry for dynamically discovering and managing plugin-like implementation classes.
    Supports on-demand reloading of plugins.
    """

    def __init__(self, package_manager: PackageManager, config: PluginRegistryConfig):
        self._package_manager = package_manager
        self._config = config
        self._plugin_types: dict[str, type[T]] = {}
        self._plugin_modules: dict[str, str] = {}  # Maps type_name to module path

        self._load_plugin_modules()

    def get_plugin_class_type(self, type_name: str) -> type[T]:
        """
        Get the plugin class type for the given type name.
        """
        if type_name in self._plugin_types:
            return self._plugin_types[type_name]

        raise self._config.error_class(f"Plugin implementation for '{type_name}' not found.")

    def _load_plugin_modules(self) -> None:
        self._plugin_types.clear()
        self._plugin_modules.clear()

        for package in self._package_manager.get_all_packages():
            directory = getattr(package, self._config.directory_name)

            if not Path(directory).is_dir():
                continue

            for current_dir, _, files in os.walk(directory):
                if self._config.config_file_name not in files:
                    continue

                dir_path = Path(current_dir).relative_to(Path(directory))

                implementation_file = Path(current_dir) / self._config.implementation_file_name

                self._load_single_plugin(package.name, dir_path, implementation_file)

        raise_batched_errors(root_exception_type=self._config.error_class)

    def _load_single_plugin(self, package_name: str, dir_path: Path, implementation_file: Path) -> None:
        module_name = Path(dir_path).name
        spec = importlib.util.spec_from_file_location(module_name, implementation_file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        found_implementation = False
        for name, obj in module.__dict__.items():
            if inspect.isclass(obj) and obj is not self._config.base_class and name.endswith(self._config.class_suffix):
                type_name = self._config.spec_registry.get_spec_by_dir(Path(package_name) / dir_path)
                self._plugin_types[type_name] = obj
                self._plugin_modules[type_name] = implementation_file
                found_implementation = True
                log.debug(
                    f"Loaded {self._config.class_suffix.lower()} plugin '{name}' for type '{type_name}' from package "
                    f"'{package_name}'"
                )
                break

        if not found_implementation:
            batch_error(
                f"{self._config.class_suffix} plugin for '{module_name}' in package '{package_name}' not found."
                f" Make sure that its name ends in '{self._config.class_suffix}'.",
                self._config.error_class,
            )

    def reload_plugin(self, type_name: str) -> None:
        """
        Reload a specific plugin by its type name.
        """
        if type_name not in self._plugin_modules:
            raise self._config.error_class(f"Plugin '{type_name}' not found.")

        implementation_file = self._plugin_modules[type_name]
        package_name = Path(implementation_file).parent.parent.name
        dir_path = os.path.relpath(Path(implementation_file).parent, Path(implementation_file).parent.parent)

        self._load_single_plugin(package_name, dir_path, implementation_file)
        log.info(f"Reloaded plugin '{type_name}'")

    def reload_all_plugins(self) -> None:
        """
        Reload all plugins.
        """
        self._load_plugin_modules()
        log.info("Reloaded all plugins")
