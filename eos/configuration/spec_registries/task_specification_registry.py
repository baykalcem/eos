from eos.configuration.entities.task import TaskConfig
from eos.configuration.entities.task_specification import TaskSpecification
from eos.configuration.spec_registries.specification_registry import SpecificationRegistry


class TaskSpecificationRegistry(SpecificationRegistry[TaskSpecification, TaskConfig]):
    """
    The task specification registry stores the specifications for all tasks that are available in EOS.
    """

    def __init__(
        self,
        task_specifications: dict[str, TaskSpecification],
        task_dirs_to_task_types: dict[str, str],
    ):
        updated_specs = self._update_output_containers(task_specifications)
        super().__init__(updated_specs, task_dirs_to_task_types)

    @staticmethod
    def _update_output_containers(specs: dict[str, TaskSpecification]) -> dict[str, TaskSpecification]:
        for spec in specs.values():
            if not spec.output_containers:
                spec.output_containers = spec.input_containers.copy()
        return specs
