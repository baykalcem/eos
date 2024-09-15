from abc import ABC, abstractmethod


class AbstractRepository(ABC):
    """
    Abstract class for a repository that provides CRUD operations for a collection of entities.
    """

    @abstractmethod
    def create(self, entity: dict) -> None:
        pass

    @abstractmethod
    def count(self, **query: dict) -> int:
        pass

    @abstractmethod
    def exists(self, count: int = 1, **query: dict) -> bool:
        pass

    @abstractmethod
    def get_one(self, **query: dict) -> dict:
        pass

    @abstractmethod
    def get_all(self, **query: dict) -> list[dict]:
        pass

    @abstractmethod
    def update(self, entity_id: str, entity: dict) -> None:
        pass

    @abstractmethod
    def delete(self, entity_id: str) -> None:
        pass
