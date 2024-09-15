from pymongo import MongoClient
from pymongo.client_session import ClientSession
from pymongo.database import Database

from eos.logging.logger import log
from eos.persistence.service_credentials import ServiceCredentials


class DbManager:
    """
    Responsible for giving access to a MongoDB database.
    """

    def __init__(
        self,
        db_credentials: ServiceCredentials,
        db_name: str = "eos",
    ):
        self._db_credentials = db_credentials

        self._db_client = MongoClient(
            host=self._db_credentials.host,
            port=self._db_credentials.port,
            username=self._db_credentials.username,
            password=self._db_credentials.password,
            serverSelectionTimeoutMS=10000,
        )

        self._db: Database = self._db_client[db_name]

        log.debug(f"Db manager initialized with database '{db_name}'.")

    def get_db(self) -> Database:
        """Get the database."""
        return self._db

    def create_collection_index(self, collection: str, index: list[tuple[str, int]], unique: bool = False) -> None:
        """
        Create an index for a collection in the database if it doesn't already exist.
        :param collection: The collection name.
        :param index: The index to create. A list of tuples of the field names and index orders.
        :param unique: Whether the index should be unique.
        """
        index_name = "_".join(f"{field}_{order}" for field, order in index)
        if index_name not in self._db[collection].index_information():
            self._db[collection].create_index(index, unique=unique, name=index_name)

    def start_session(self) -> ClientSession:
        """Start a new client session."""
        return self._db_client.start_session()

    def clean_db(self) -> None:
        """Clean the database."""
        for collection in self._db.list_collection_names():
            self._db[collection].drop()
