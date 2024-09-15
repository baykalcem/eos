import importlib.util
from collections.abc import Callable
from pathlib import Path
from typing import Any

from eos.configuration.constants import (
    CAMPAIGN_OPTIMIZER_FILE_NAME,
    CAMPAIGN_OPTIMIZER_CREATION_FUNCTION_NAME,
)
from eos.configuration.package_manager import PackageManager
from eos.configuration.plugin_registries.plugin_registry import PluginRegistry, PluginRegistryConfig
from eos.logging.logger import log
from eos.optimization.abstract_sequential_optimizer import AbstractSequentialOptimizer


class CampaignOptimizerPluginRegistry(
    PluginRegistry[Callable[[], tuple[dict[str, Any], type[AbstractSequentialOptimizer]]], Any]
):
    """
    Responsible for dynamically loading campaign optimizers from all packages
    and providing references to them for later use.
    """

    def __init__(self, package_manager: PackageManager):
        config = PluginRegistryConfig(
            spec_registry=None,  # Campaign optimizers don't use a specification registry
            base_class=None,  # Campaign optimizers don't have a base class
            config_file_name=None,  # Campaign optimizers don't have a separate config file
            implementation_file_name=CAMPAIGN_OPTIMIZER_FILE_NAME,
            class_suffix="",  # Campaign optimizers don't use a class suffix
            error_class=Exception,  # Using generic Exception for simplicity
            directory_name="experiments_dir",
        )
        super().__init__(package_manager, config)

    def get_campaign_optimizer_creation_parameters(
        self, experiment_type: str
    ) -> tuple[dict[str, Any], type[AbstractSequentialOptimizer]] | None:
        """
        Get a function that can be used to get the constructor arguments and the optimizer type so it can be
        constructed later.

        :param experiment_type: The type of the experiment.
        :return: A tuple containing the constructor arguments and the optimizer type, or None if not found.
        """
        optimizer_function = self.get_plugin_class_type(experiment_type)
        if optimizer_function:
            return optimizer_function()
        return None

    def _load_single_plugin(self, package_name: str, dir_path: str, implementation_file: str) -> None:
        module_name = Path(dir_path).name
        spec = importlib.util.spec_from_file_location(module_name, implementation_file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        if CAMPAIGN_OPTIMIZER_CREATION_FUNCTION_NAME in module.__dict__:
            experiment_type = module_name
            self._plugin_types[experiment_type] = module.__dict__[CAMPAIGN_OPTIMIZER_CREATION_FUNCTION_NAME]
            self._plugin_modules[experiment_type] = implementation_file
            log.info(f"Loaded campaign optimizer for experiment '{experiment_type}' from package '{package_name}'.")
        else:
            log.warning(
                f"Optimizer configuration function '{CAMPAIGN_OPTIMIZER_CREATION_FUNCTION_NAME}' not found in the "
                f"campaign optimizer file '{self._config.implementation_file_name}' of experiment "
                f"'{Path(dir_path).name}' in package '{package_name}'."
            )

    def load_campaign_optimizer(self, experiment_type: str) -> None:
        """
        Load the optimizer configuration function for the given experiment from the appropriate package.
        If the optimizer doesn't exist, log a warning and return without raising an error.
        """
        experiment_package = self._package_manager.find_package_for_experiment(experiment_type)
        if not experiment_package:
            log.warning(f"No package found for experiment '{experiment_type}'.")
            return

        optimizer_file = (
            Path(experiment_package.experiments_dir) / experiment_type / self._config.implementation_file_name
        )

        if not Path(optimizer_file).exists():
            log.warning(
                f"No campaign optimizer found for experiment '{experiment_type}' in package "
                f"'{experiment_package.name}'."
            )
            return

        self._load_single_plugin(experiment_package.name, experiment_type, optimizer_file)

    def unload_campaign_optimizer(self, experiment_type: str) -> None:
        """
        Unload the optimizer configuration function for the given experiment.
        """
        if experiment_type in self._plugin_types:
            del self._plugin_types[experiment_type]
            del self._plugin_modules[experiment_type]
            log.info(f"Unloaded campaign optimizer for experiment '{experiment_type}'.")

    def reload_plugin(self, experiment_type: str) -> None:
        """
        Reload a specific campaign optimizer by its experiment type.
        """
        self.unload_campaign_optimizer(experiment_type)
        self.load_campaign_optimizer(experiment_type)
        log.info(f"Reloaded campaign optimizer for experiment '{experiment_type}'.")

    def reload_all_plugins(self) -> None:
        """
        Reload all campaign optimizers.
        """
        experiment_types = list(self._plugin_types.keys())
        for experiment_type in experiment_types:
            self.reload_plugin(experiment_type)
        log.info("Reloaded all campaign optimizers.")
