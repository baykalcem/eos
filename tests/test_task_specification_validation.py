from eos.configuration.entities.parameters import (
    ParameterFactory,
    ParameterType,
)
from eos.configuration.entities.task_specification import (
    TaskSpecificationOutputParameter,
    TaskSpecification,
)
from eos.configuration.exceptions import EosConfigurationError
from tests.fixtures import *


class TestTaskSpecifications:
    def test_invalid_parameter_type(self):
        with pytest.raises(ValueError):
            ParameterFactory.create_parameter(
                "invalid_type",
                value=120,
                description="Duration of evaporation in seconds.",
            )

    def test_numeric_parameter_unit_not_specified(self):
        with pytest.raises(EosConfigurationError):
            ParameterFactory.create_parameter(
                ParameterType.integer,
                unit="",
                value=120,
                min=60,
                description="Duration of evaporation in seconds.",
            )

    def test_numeric_parameter_value_not_numeric(self):
        with pytest.raises(EosConfigurationError):
            ParameterFactory.create_parameter(
                ParameterType.integer,
                unit="sec",
                value="not_a_number",
                min=60,
                description="Duration of evaporation in seconds.",
            )

    def test_numeric_parameter_min_greater_than_max(self):
        with pytest.raises(EosConfigurationError):
            ParameterFactory.create_parameter(
                ParameterType.integer,
                unit="sec",
                value=120,
                min=300,
                max=60,
                description="Duration of evaporation in seconds.",
            )

    def test_numeric_parameter_out_of_range_min(self):
        with pytest.raises(EosConfigurationError):
            ParameterFactory.create_parameter(
                ParameterType.integer,
                unit="sec",
                value=5,
                min=60,
                max=300,
                description="Duration of evaporation in seconds.",
            )

    def test_numeric_parameter_out_of_range_max(self):
        with pytest.raises(EosConfigurationError):
            ParameterFactory.create_parameter(
                ParameterType.integer,
                unit="sec",
                value=100,
                min=0,
                max=80,
                description="Duration of evaporation in seconds.",
            )

    def test_boolean_parameter_invalid_value(self):
        with pytest.raises(EosConfigurationError):
            ParameterFactory.create_parameter(
                ParameterType.boolean,
                value="not_a_boolean",
                description="Whether to sparge the evaporation vessel with nitrogen.",
            )

    def test_choice_parameter_choices_not_specified(self):
        with pytest.raises(EosConfigurationError):
            ParameterFactory.create_parameter(
                ParameterType.choice,
                choices=[],
                value="method1",
                description="Method to use",
            )

    def test_choice_parameter_no_value(self):
        with pytest.raises(EosConfigurationError):
            ParameterFactory.create_parameter(
                ParameterType.choice,
                choices=["method1", "method2"],
                value=None,
                description="Method to use",
            )

    def test_choice_parameter_invalid_value(self):
        with pytest.raises(EosConfigurationError):
            ParameterFactory.create_parameter(
                ParameterType.choice,
                choices=["method1", "method2"],
                value="invalid_method",
                description="Method to use",
            )

    def test_list_parameter_invalid_element_type(self):
        with pytest.raises(EosConfigurationError):
            ParameterFactory.create_parameter(
                ParameterType.list,
                length=3,
                element_type="invalid_type",
                value=[1, 2, 3],
                description="List of elements",
            )

    def test_list_parameter_nested_list(self):
        with pytest.raises(EosConfigurationError):
            ParameterFactory.create_parameter(
                ParameterType.list,
                length=3,
                element_type="list",
                value=[[1], [2], [3]],
                description="List of elements",
            )

    def test_list_parameter_invalid_value(self):
        with pytest.raises(EosConfigurationError):
            ParameterFactory.create_parameter(
                ParameterType.list,
                length=3,
                element_type="integer",
                value=4,
                description="List of elements",
            )

    def test_list_parameter_elements_not_same_type(self):
        with pytest.raises(EosConfigurationError):
            ParameterFactory.create_parameter(
                ParameterType.list,
                length=3,
                element_type="integer",
                value=[1, True, "3"],
                description="List of elements",
            )

    def test_list_parameter_invalid_value_element_size(self):
        with pytest.raises(EosConfigurationError):
            ParameterFactory.create_parameter(
                ParameterType.list,
                length=3,
                element_type="integer",
                value=[1, 2],
                description="List of elements",
            )

    def test_list_parameter_invalid_value_element_min(self):
        with pytest.raises(EosConfigurationError):
            ParameterFactory.create_parameter(
                ParameterType.list,
                length=3,
                element_type="integer",
                value=[1, 2, 3],
                min=[2, 2, "INVALID"],
                description="List of elements",
            )

    def test_list_parameter_invalid_value_element_max(self):
        with pytest.raises(EosConfigurationError):
            ParameterFactory.create_parameter(
                ParameterType.list,
                length=3,
                element_type="integer",
                value=[1, 2, 3],
                max=[2, 2, "INVALID"],
                description="List of elements",
            )

    def test_list_parameter_value_less_than_min(self):
        with pytest.raises(EosConfigurationError):
            ParameterFactory.create_parameter(
                ParameterType.list,
                length=3,
                element_type="integer",
                value=[2, 2, 2],
                min=[2, 2, 3],
                description="List of elements",
            )

    def test_list_parameter_value_greater_than_max(self):
        with pytest.raises(EosConfigurationError):
            ParameterFactory.create_parameter(
                ParameterType.list,
                length=3,
                element_type="integer",
                value=[2, 2, 2],
                max=[2, 2, 1],
                description="List of elements",
            )

    def test_list_parameter_invalid_min_max_size(self):
        with pytest.raises(EosConfigurationError):
            ParameterFactory.create_parameter(
                ParameterType.list,
                length=3,
                element_type="integer",
                value=[2, 2, 2],
                min=[2, 2],
                description="List of elements",
            )

        with pytest.raises(EosConfigurationError):
            ParameterFactory.create_parameter(
                ParameterType.list,
                length=3,
                element_type="integer",
                value=[2, 2, 2],
                max=[2, 2],
                description="List of elements",
            )

    def test_parameter_factory_invalid_type(self):
        with pytest.raises(ValueError):
            ParameterFactory.create_parameter(
                "invalid_type",
                value=120,
                description="Duration of evaporation in seconds.",
            )

    def test_parameter_invalid_name(self, configuration_manager):
        task_specs = configuration_manager.task_specs

        task_spec = task_specs.get_spec_by_type("Magnetic Mixing")

        task_spec.input_parameters["invalid_name*"] = {
            "type": "integer",
            "unit": "sec",
            "value": 120,
            "description": "Duration of evaporation in seconds.",
        }

        with pytest.raises(EosConfigurationError):
            TaskSpecification(**task_spec)

    def test_output_numeric_parameter_unit_not_specified(self, configuration_manager):
        with pytest.raises(EosConfigurationError):
            TaskSpecificationOutputParameter(
                type=ParameterType.integer,
                unit="",
                description="Duration of evaporation in seconds.",
            )

    def test_output_non_numeric_parameter_unit_specified(self, configuration_manager):
        with pytest.raises(EosConfigurationError):
            TaskSpecificationOutputParameter(
                type=ParameterType.boolean,
                unit="sec",
                description="Whether to sparge the evaporation vessel with nitrogen.",
            )
