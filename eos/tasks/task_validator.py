from eos.configuration.entities.task import TaskConfig
from eos.configuration.entities.task_specification import TaskSpecification
from eos.configuration.spec_registries.task_specification_registry import TaskSpecificationRegistry
from eos.tasks.task_input_parameter_validator import TaskInputParameterValidator


class TaskValidator:
    def __init__(self):
        self.task_spec_registry = TaskSpecificationRegistry()

    def validate(self, task_config: TaskConfig) -> None:
        task_spec = self.task_spec_registry.get_spec_by_config(task_config)
        self._validate_parameters(task_config, task_spec)

    @staticmethod
    def _validate_parameters(task_config: TaskConfig, task_spec: TaskSpecification) -> None:
        validator = TaskInputParameterValidator(task_config, task_spec)
        validator.validate_input_parameters()
