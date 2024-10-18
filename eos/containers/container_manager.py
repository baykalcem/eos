import asyncio
from collections import defaultdict
from typing import Any

from eos.configuration.configuration_manager import ConfigurationManager
from eos.containers.entities.container import Container
from eos.containers.exceptions import EosContainerStateError
from eos.containers.repositories.container_repository import ContainerRepository
from eos.logging.logger import log
from eos.persistence.async_mongodb_interface import AsyncMongoDbInterface
from eos.utils.async_rlock import AsyncRLock


class ContainerManager:
    """
    The container manager provides methods for interacting with containers in a lab.
    """

    def __init__(self, configuration_manager: ConfigurationManager, db_interface: AsyncMongoDbInterface):
        self._configuration_manager = configuration_manager
        self._session_factory = db_interface.session_factory
        self._locks = defaultdict(AsyncRLock)
        self._containers = None

    async def initialize(self, db_interface: AsyncMongoDbInterface) -> None:
        self._containers = ContainerRepository(db_interface)
        await self._containers.initialize()
        await self._create_containers()
        log.debug("Container manager initialized.")

    async def get_container(self, container_id: str) -> Container:
        """
        Get a copy of the container with the specified ID.
        """
        container = await self._containers.get_one(id=container_id)

        if container:
            return Container(**container)

        raise EosContainerStateError(f"Container '{container_id}' does not exist.")

    async def get_containers(self, **query: dict[str, Any]) -> list[Container]:
        """
        Query containers with arbitrary parameters.

        :param query: Dictionary of query parameters.
        """
        containers = await self._containers.get_all(**query)
        return [Container(**container) for container in containers]

    async def set_location(self, container_id: str, location: str) -> None:
        """
        Set the location of a container.
        """
        async with self._get_lock(container_id):
            await self._containers.update_one({"location": location}, id=container_id)

    async def set_lab(self, container_id: str, lab: str) -> None:
        """
        Set the lab of a container.
        """
        async with self._get_lock(container_id):
            await self._containers.update_one({"lab": lab}, id=container_id)

    async def set_metadata(self, container_id: str, metadata: dict[str, Any]) -> None:
        """
        Set metadata for a container.
        """
        async with self._get_lock(container_id):
            await self._containers.update_one({"metadata": metadata}, id=container_id)

    async def add_metadata(self, container_id: str, metadata: dict[str, Any]) -> None:
        """
        Add metadata to a container.
        """
        container = await self.get_container(container_id)
        container.metadata.update(metadata)

        async with self._get_lock(container_id):
            await self._containers.update_one({"metadata": container.metadata}, id=container_id)

    async def remove_metadata(self, container_id: str, metadata_keys: list[str]) -> None:
        """
        Remove metadata from a container.
        """
        container = await self.get_container(container_id)
        for key in metadata_keys:
            container.metadata.pop(key, None)

        async with self._get_lock(container_id):
            await self._containers.update_one({"metadata": container.metadata}, id=container_id)

    async def update_container(self, container: Container) -> None:
        """
        Update a container in the database.
        """
        await self._containers.update_one(container.model_dump(), id=container.id)

    async def update_containers(
        self, loaded_labs: set[str] | None = None, unloaded_labs: set[str] | None = None
    ) -> None:
        """
        Update containers based on loaded and unloaded labs.
        """
        if unloaded_labs:
            await asyncio.gather(*[self._remove_containers_for_lab(lab_id) for lab_id in unloaded_labs])

        if loaded_labs:
            await asyncio.gather(*[self._create_containers_for_lab(lab_id) for lab_id in loaded_labs])

        log.debug("Containers have been updated.")

    async def _remove_containers_for_lab(self, lab_id: str) -> None:
        """
        Remove containers associated with an unloaded lab.
        """
        containers_to_remove = await self.get_containers(lab=lab_id)
        await asyncio.gather(*[self._containers.delete_one(id=container.id) for container in containers_to_remove])
        log.debug(f"Removed containers for lab '{lab_id}'")

    async def _create_containers_for_lab(self, lab_id: str) -> None:
        """
        Create containers for a loaded lab.
        """
        lab_config = self._configuration_manager.labs[lab_id]
        for container_config in lab_config.containers:
            for container_id in container_config.ids:
                container_exists = await self._containers.exists(id=container_id)
                if not container_exists:
                    container = Container(
                        id=container_id,
                        type=container_config.type,
                        lab=lab_id,
                        location=container_config.location,
                        metadata=container_config.metadata,
                    )
                    await self._containers.update_one(container.model_dump(), id=container_id)
        log.debug(f"Created containers for lab '{lab_id}'")

    async def _create_containers(self) -> None:
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
                    await self._containers.update_one(container.model_dump(), id=container_id)
        log.debug("Created containers")

    def _get_lock(self, container_id: str) -> AsyncRLock:
        """
        Get the lock for a specific container.
        """
        return self._locks[container_id]
