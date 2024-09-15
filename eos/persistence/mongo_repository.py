from typing import Any

from pymongo.results import DeleteResult, UpdateResult, InsertOneResult

from eos.persistence.abstract_repository import AbstractRepository
from eos.persistence.db_manager import DbManager


class MongoRepository(AbstractRepository):
    """
    Provides CRUD operations for a MongoDB collection.
    """

    def __init__(self, collection_name: str, db_manager: DbManager):
        self._collection = db_manager.get_db().get_collection(collection_name)

    def create_indices(self, indices: list[tuple[str, int]], unique: bool = False) -> None:
        """
        Create indices on the collection.

        :param indices: List of tuples of field names and order (1 for ascending, -1 for descending).
        :param unique: Whether the index should be unique.
        """
        index_name = "_".join(f"{field}_{order}" for field, order in indices)
        if index_name not in self._collection.index_information():
            self._collection.create_index(indices, unique=unique, name=index_name)

    def create(self, entity: dict[str, Any]) -> InsertOneResult:
        """
        Create a new entity in the collection.

        :param entity: The entity to create.
        :return: The result of the insert operation.
        """
        return self._collection.insert_one(entity)

    def count(self, **kwargs) -> int:
        """
        Count the number of entities that match the query in the collection.

        :param kwargs: Query parameters.
        :return: The number of entities.
        """
        return self._collection.count_documents(kwargs)

    def exists(self, count: int = 1, **kwargs) -> bool:
        """
        Check if the number of entities that match the query exist in the collection.

        :param count: The number of entities to check for.
        :param kwargs: Query parameters.
        :return: Whether the entity exists.
        """
        return self.count(**kwargs) >= count

    def get_one(self, **kwargs) -> dict[str, Any]:
        """
        Get a single entity from the collection.

        :param kwargs: Query parameters.
        :return: The entity as a dictionary.
        """
        return self._collection.find_one(kwargs)

    def get_all(self, **kwargs) -> list[dict[str, Any]]:
        """
        Get all entities from the collection.

        :param kwargs: Query parameters.
        :return: List of entities as dictionaries.
        """
        return list(self._collection.find(kwargs))

    def update(self, entity: dict[str, Any], **kwargs) -> UpdateResult:
        """
        Update an entity in the collection.

        :param entity: The updated entity (or some of its fields).
        :param kwargs: Query parameters.
        :return: The result of the update operation.
        """
        return self._collection.update_one(kwargs, {"$set": entity}, upsert=True)

    def delete(self, **kwargs) -> DeleteResult:
        """
        Delete entities from the collection.

        :param kwargs: Query parameters.
        :return: The result of the delete operation.
        """
        return self._collection.delete_many(kwargs)
