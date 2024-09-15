from collections import defaultdict

from eos.configuration.entities.lab import LabConfig
from eos.configuration.exceptions import EosLabConfigurationError


class MultiLabValidator:
    """
    Cross-checks all lab configuration. It validates that all container IDs are globally unique.
    """

    def __init__(self, lab_configs: list[LabConfig]):
        self._lab_configs = lab_configs

    def validate(self) -> None:
        self._validate_computer_ips_globally_unique()
        self._validate_container_ids_globally_unique()

    def _validate_computer_ips_globally_unique(self) -> None:
        computer_ips = defaultdict(list)

        for lab in self._lab_configs:
            for computer in lab.computers.values():
                computer_ips[computer.ip].append(lab.type)

        duplicate_ips = {ip: labs for ip, labs in computer_ips.items() if len(labs) > 1}

        if duplicate_ips:
            duplicate_ips_str = "\n  ".join(
                f"'{ip}': defined in labs {', '.join(labs)}" for ip, labs in duplicate_ips.items()
            )
            raise EosLabConfigurationError(
                f"The following computer IPs are not globally unique:\n  {duplicate_ips_str}"
            )

    def _validate_container_ids_globally_unique(self) -> None:
        container_ids = defaultdict(list)
        for lab in self._lab_configs:
            for container in lab.containers:
                for container_id in container.ids:
                    container_ids[container_id].append(lab.type)

        duplicate_ids = {container_id: labs for container_id, labs in container_ids.items() if len(labs) > 1}

        if duplicate_ids:
            duplicate_ids_str = "\n  ".join(
                f"'{container_id}': defined in labs {', '.join(labs)}" for container_id, labs in duplicate_ids.items()
            )
            raise EosLabConfigurationError(
                f"The following container IDs are not globally unique:\n  {duplicate_ids_str}"
            )
