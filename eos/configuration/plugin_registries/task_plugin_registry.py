from eos.configuration.constants import TASK_CONFIG_FILE_NAME, TASK_IMPLEMENTATION_FILE_NAME
from eos.configuration.exceptions import EosTaskHandlerClassNotFoundError
from eos.configuration.package_manager import PackageManager
from eos.configuration.plugin_registries.plugin_registry import PluginRegistry, PluginRegistryConfig
from eos.configuration.spec_registries.task_specification_registry import TaskSpecificationRegistry
from eos.tasks.base_task import BaseTask


class TaskPluginRegistry(PluginRegistry[BaseTask, TaskSpecificationRegistry]):
    def __init__(self, package_manager: PackageManager):
        config = PluginRegistryConfig(
            spec_registry=TaskSpecificationRegistry(),
            base_class=BaseTask,
            config_file_name=TASK_CONFIG_FILE_NAME,
            implementation_file_name=TASK_IMPLEMENTATION_FILE_NAME,
            class_suffix="Task",
            error_class=EosTaskHandlerClassNotFoundError,
            directory_name="tasks_dir",
        )
        super().__init__(package_manager, config)

    def get_task_class_type(self, task_type: str) -> type[BaseTask]:
        return self.get_plugin_class_type(task_type)
