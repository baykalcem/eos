import threading
from collections import defaultdict
from typing import Any

from eos.configuration.configuration_manager import ConfigurationManager
from eos.containers.entities.container import Container
from eos.containers.exceptions import EosContainerStateError
from eos.containers.repositories.container_repository import ContainerRepository
from eos.logging.logger import log
from eos.persistence.db_manager import DbManager


class ContainerManager:
    """
    The container manager provides methods for interacting with containers in a lab.
    """

    def __init__(self, configuration_manager: ConfigurationManager, db_manager: DbManager):
        self._configuration_manager = configuration_manager

        self._containers = ContainerRepository("containers", db_manager)
        self._containers.create_indices([("id", 1)], unique=True)
        self._locks = defaultdict(threading.RLock)

        self._create_containers()
        log.debug("Container manager initialized.")

    def get_container(self, container_id: str) -> Container:
        """
        Get a copy of the container with the specified ID.
        """
        container = self._containers.get_one(id=container_id)

        if container:
            return Container(**container)

        raise EosContainerStateError(f"Container '{container_id}' does not exist.")

    def get_containers(self, **query: dict[str, Any]) -> list[Container]:
        """
        Query containers with arbitrary parameters.

        :param query: Dictionary of query parameters.
        """
        containers = self._containers.get_all(**query)
        return [Container(**container) for container in containers]

    def set_location(self, container_id: str, location: str) -> None:
        """
        Set the location of a container.
        """
        with self._get_lock(container_id):
            self._containers.update({"location": location}, id=container_id)

    def set_lab(self, container_id: str, lab: str) -> None:
        """
        Set the lab of a container.
        """
        with self._get_lock(container_id):
            self._containers.update({"lab": lab}, id=container_id)

    def set_metadata(self, container_id: str, metadata: dict[str, Any]) -> None:
        """
        Set metadata for a container.
        """
        with self._get_lock(container_id):
            self._containers.update({"metadata": metadata}, id=container_id)

    def add_metadata(self, container_id: str, metadata: dict[str, Any]) -> None:
        """
        Add metadata to a container.
        """
        container = self.get_container(container_id)
        container.metadata.update(metadata)

        with self._get_lock(container_id):
            self._containers.update({"metadata": container.metadata}, id=container_id)

    def remove_metadata(self, container_id: str, metadata_keys: list[str]) -> None:
        """
        Remove metadata from a container.
        """
        container = self.get_container(container_id)
        for key in metadata_keys:
            container.metadata.pop(key, None)

        with self._get_lock(container_id):
            self._containers.update({"metadata": container.metadata}, id=container_id)

    def update_container(self, container: Container) -> None:
        """
        Update a container in the database.
        """
        self._containers.update(container.model_dump(), id=container.id)

    def update_containers(self, loaded_labs: set[str] | None = None, unloaded_labs: set[str] | None = None) -> None:
        """
        Update containers based on loaded and unloaded labs.
        """
        if unloaded_labs:
            for lab_id in unloaded_labs:
                self._remove_containers_for_lab(lab_id)

        if loaded_labs:
            for lab_id in loaded_labs:
                self._create_containers_for_lab(lab_id)

        log.debug("Containers have been updated.")

    def _remove_containers_for_lab(self, lab_id: str) -> None:
        """
        Remove containers associated with an unloaded lab.
        """
        containers_to_remove = self.get_containers(lab=lab_id)
        for container in containers_to_remove:
            self._containers.delete(id=container.id)
        log.debug(f"Removed containers for lab '{lab_id}'")

    def _create_containers_for_lab(self, lab_id: str) -> None:
        """
        Create containers for a loaded lab.
        """
        lab_config = self._configuration_manager.labs[lab_id]
        for container_config in lab_config.containers:
            for container_id in container_config.ids:
                existing_container = self._containers.get_one(id=container_id)
                if not existing_container:
                    container = Container(
                        id=container_id,
                        type=container_config.type,
                        lab=lab_id,
                        location=container_config.location,
                        metadata=container_config.metadata,
                    )
                    self._containers.update(container.model_dump(), id=container_id)
        log.debug(f"Created containers for lab '{lab_id}'")

    def _create_containers(self) -> None:
        """
        Create containers from the lab configuration and add them to the database.
        """
        for lab_name, lab_config in self._configuration_manager.labs.items():
            for container_config in lab_config.containers:
                for container_id in container_config.ids:
                    container = Container(
                        id=container_id,
                        type=container_config.type,
                        lab=lab_name,
                        location=container_config.location,
                        metadata=container_config.metadata,
                    )
                    self._containers.update(container.model_dump(), id=container_id)
        log.debug("Created containers")

    def _get_lock(self, container_id: str) -> threading.RLock:
        """
        Get the lock for a specific container.
        """
        return self._locks[container_id]
