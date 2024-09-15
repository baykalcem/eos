from eos.configuration.entities.parameters import (
    AllowedParameterTypes,
)


def is_parameter_reference(parameter: AllowedParameterTypes) -> bool:
    return (
        isinstance(parameter, str)
        and parameter.count(".") == 1
        and all(component.strip() for component in parameter.split("."))
    )


def is_dynamic_parameter(parameter: AllowedParameterTypes) -> bool:
    return isinstance(parameter, str) and parameter.lower() == "eos_dynamic"


def is_dynamic_container(container_id: str) -> bool:
    """
    Check if the container ID is a dynamic container ID (eos_dynamic).
    """
    return isinstance(container_id, str) and container_id.lower() == "eos_dynamic"


def is_container_reference(container_id: str) -> bool:
    """
    Check if the container ID is a reference.
    """
    return (
        isinstance(container_id, str)
        and container_id.count(".") == 1
        and all(component.strip() for component in container_id.split("."))
    )
