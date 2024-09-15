import pytest
from omegaconf import DictConfig

from eos.configuration.entities.parameters import ParameterType
from eos.configuration.entities.task import TaskConfig
from eos.configuration.entities.task_specification import TaskSpecification
from eos.tasks.exceptions import EosTaskValidationError
from eos.tasks.task_input_parameter_validator import TaskInputParameterValidator


class TestTaskInputParameterValidator:
    @pytest.fixture
    def task_spec(self):
        return TaskSpecification(
            type="test_task",
            description="A test task",
            input_parameters={
                "integer_param": DictConfig(
                    {"type": "integer", "unit": "n/a", "description": "An integer parameter", "min": 0, "max": 100}
                ),
                "decimal_param": DictConfig(
                    {"type": "decimal", "unit": "n/a", "description": "A float parameter", "min": 0.0, "max": 1.0}
                ),
                "string_param": DictConfig({"type": "string", "description": "A string parameter"}),
                "boolean_param": DictConfig({"type": "boolean", "value": False, "description": "A boolean parameter"}),
                "list_param": DictConfig(
                    {"type": "list", "description": "A list parameter", "element_type": "integer", "length": 3}
                ),
                "dictionary_param": DictConfig({"type": "dictionary", "description": "A dictionary parameter"}),
                "choice_param": DictConfig(
                    {"type": "choice", "value": "A", "description": "A choice parameter", "choices": ["A", "B", "C"]}
                ),
            },
        )

    @pytest.fixture
    def task_config(self, task_spec):
        return TaskConfig(
            id="test_task_1",
            type="test_task",
            parameters={
                "integer_param": 50,
                "decimal_param": 0.5,
                "string_param": "test",
                "boolean_param": True,
                "list_param": [1, 2, 3],
                "dictionary_param": {"key": "value"},
                "choice_param": "A",
            },
        )

    @pytest.fixture
    def validator(self, task_config, task_spec):
        return TaskInputParameterValidator(task_config, task_spec)

    def test_valid_input_parameters(self, validator):
        validator.validate_input_parameters()  # Should not raise any exceptions

    @pytest.mark.parametrize(
        ("param_name", "invalid_value"),
        [
            ("integer_param", "not_an_int"),
            ("decimal_param", "not_a_float"),
            ("boolean_param", "not_a_bool"),
            ("list_param", "not_a_list"),
            ("dictionary_param", "not_a_dict"),
            ("choice_param", "D"),
        ],
    )
    def test_invalid_input_parameters(self, validator, task_config, param_name, invalid_value):
        task_config.parameters[param_name] = invalid_value
        with pytest.raises(EosTaskValidationError):
            validator.validate_input_parameters()

    def test_missing_required_parameter(self, validator, task_config):
        del task_config.parameters["integer_param"]
        with pytest.raises(EosTaskValidationError):
            validator.validate_input_parameters()

    def test_extra_parameter(self, validator, task_config):
        task_config.parameters["extra_param"] = "extra"
        with pytest.raises(EosTaskValidationError):
            validator.validate_input_parameters()

    @pytest.mark.parametrize(
        ("param_type", "valid_values", "invalid_values"),
        [
            (ParameterType.integer, [0, 50, 100, "50"], [-1, 101, "fifty"]),
            (ParameterType.decimal, [0.0, 0.5, 1.0, "0.5"], [-0.1, 1.1, "half"]),
            (ParameterType.boolean, [True, False, "true", "false"], ["yes", "no", 2]),
            (ParameterType.string, ["test", "123", ""], []),
            (ParameterType.list, [[1, 2, 3], [1, 2, 62]], [[1, 2], [1, 2, 3, 4], "not_a_list"]),
            (ParameterType.dictionary, [{"key": "value"}, {}], ["not_a_dict", [1, 2, 3]]),
            (ParameterType.choice, ["A", "B", "C"], ["D", 1, True]),
        ],
    )
    def test_parameter_type_conversion(
        self, validator, task_config, task_spec, param_type, valid_values, invalid_values
    ):
        param_name = f"{param_type.value}_param"
        task_spec.input_parameters[param_name]["type"] = param_type.value
        if param_type == ParameterType.choice:
            task_spec.input_parameters[param_name]["choices"] = ["A", "B", "C"]
        elif param_type == ParameterType.list:
            task_spec.input_parameters[param_name]["element_type"] = "integer"
            task_spec.input_parameters[param_name]["length"] = 3

        for valid_value in valid_values:
            task_config.parameters[param_name] = valid_value
            validator.validate_input_parameters()  # Should not raise any exceptions

        for invalid_value in invalid_values:
            task_config.parameters[param_name] = invalid_value
            with pytest.raises(EosTaskValidationError):
                validator.validate_input_parameters()

    @pytest.mark.parametrize(
        ("param_name", "invalid_value", "expected_error"),
        [
            ("integer_param", "$.some_reference", EosTaskValidationError),
            ("integer_param", "eos_dynamic", EosTaskValidationError),
            ("integer_param", 150, EosTaskValidationError),
            ("list_param", [1, 2, 3, 4], EosTaskValidationError),
        ],
    )
    def test_specific_validation_cases(self, validator, task_config, param_name, invalid_value, expected_error):
        task_config.parameters[param_name] = invalid_value
        with pytest.raises(expected_error):
            validator.validate_input_parameters()
