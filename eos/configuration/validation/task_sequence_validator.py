import re
from collections import Counter

from eos.configuration.entities.experiment import ExperimentConfig
from eos.configuration.entities.lab import LabConfig
from eos.configuration.exceptions import EosTaskValidationError
from eos.configuration.validation.task_sequence.base_task_sequence_validator import BaseTaskSequenceValidator
from eos.configuration.validation.task_sequence.task_sequence_input_container_validator import (
    TaskSequenceInputContainerValidator,
)
from eos.configuration.validation.task_sequence.task_sequence_input_parameter_validator import (
    TaskSequenceInputParameterValidator,
)
from eos.logging.batch_error_logger import batch_error, raise_batched_errors


class TaskSequenceValidator(BaseTaskSequenceValidator):
    def __init__(
        self,
        experiment_config: ExperimentConfig,
        lab_configs: list[LabConfig],
    ):
        super().__init__(experiment_config, lab_configs)
        self._valid_task_id_pattern = re.compile("^[A-Za-z0-9_]+$")

    def validate(self) -> None:
        self._validate_tasks_exist()
        self._validate_task_dependencies_exist()
        self._validate_unique_task_ids()
        self._validate_task_id_format()
        self._validate_devices()
        TaskSequenceInputContainerValidator(self._experiment_config, self._lab_configs).validate()
        TaskSequenceInputParameterValidator(self._experiment_config, self._lab_configs).validate()

    def _validate_tasks_exist(self) -> None:
        for task in self._experiment_config.tasks:
            if not self._tasks.spec_exists_by_config(task):
                raise EosTaskValidationError(
                    f"Task '{task.id}' in experiment '{self._experiment_config.type}' does not exist."
                )

    def _validate_task_dependencies_exist(self) -> None:
        for task in self._experiment_config.tasks:
            for task_id in task.dependencies:
                if not any(task.id == task_id for task in self._experiment_config.tasks):
                    raise EosTaskValidationError(
                        f"Task '{task_id}' in experiment '{self._experiment_config.type}' does not exist."
                    )

    def _validate_unique_task_ids(self) -> None:
        task_ids = [task.id for task in self._experiment_config.tasks]
        if len(task_ids) != len(set(task_ids)):
            raise EosTaskValidationError("All task IDs in the task sequence must be unique.")

    def _validate_task_id_format(self) -> None:
        for task in self._experiment_config.tasks:
            if not self._valid_task_id_pattern.match(task.id):
                raise EosTaskValidationError(
                    f"Task ID '{task.id}' is invalid. Task IDs can only contain letters, numbers, "
                    f"and underscores, with no spaces."
                )

    def _validate_devices(self) -> None:
        experiment_type = self._experiment_config.type

        for task in self._experiment_config.tasks:
            task_spec = self._tasks.get_spec_by_config(task)
            required_device_types = Counter(task_spec.device_types or [])
            provided_device_types = Counter()
            used_devices = set()

            for device in task.devices:
                lab_id = device.lab_id
                device_id = device.id
                if device_id in used_devices:
                    batch_error(
                        f"Duplicate device '{device_id}' in lab '{lab_id}' requested by task '{task.id}' of experiment "
                        f"'{experiment_type}' is not allowed.",
                        EosTaskValidationError,
                    )
                    continue

                lab_config = self._find_lab_by_id(lab_id)
                if not lab_config or device_id not in lab_config.devices:
                    batch_error(
                        f"Device '{device_id}' in lab '{lab_id}' requested by task '{task.id}' of experiment "
                        f"{experiment_type} does not exist.",
                        EosTaskValidationError,
                    )
                    continue

                device_type = lab_config.devices[device_id].type
                provided_device_types[device_type] += 1
                used_devices.add(device_id)

            # Check if all required device types are provided
            missing_device_types = required_device_types - provided_device_types
            if missing_device_types:
                missing_counts_str = ", ".join(
                    [f"\n{count}x '{device_type}'" for device_type, count in missing_device_types.items()]
                )
                batch_error(
                    f"Task '{task.id}' of experiment '{experiment_type}' does not have all required device types "
                    f"satisfied. Missing device types: {missing_counts_str}",
                    EosTaskValidationError,
                )

        raise_batched_errors(EosTaskValidationError)

    def _find_lab_by_id(self, lab_id: str) -> LabConfig:
        return next((lab for lab in self._lab_configs if lab.type == lab_id), None)
