from abc import ABC, abstractmethod
from typing import Any


class AbstractAsyncRepository(ABC):
    """
    Abstract class for a repository that provides CRUD operations for a collection of entities.
    """

    @abstractmethod
    async def create(self, entity: dict) -> None:
        pass

    @abstractmethod
    async def count(self, **query: dict) -> int:
        pass

    @abstractmethod
    async def exists(self, **query: dict) -> bool:
        pass

    @abstractmethod
    async def get_one(self, **query: dict) -> dict:
        pass

    @abstractmethod
    async def get_all(self, **query: dict) -> list[dict]:
        pass

    @abstractmethod
    async def update_one(self, updated_entity: dict[str, Any], **kwargs) -> None:
        pass

    @abstractmethod
    async def delete_one(self, **query: dict) -> None:
        pass

    @abstractmethod
    async def delete_many(self, **query: dict) -> None:
        pass

    @abstractmethod
    async def delete_all(self) -> None:
        pass
