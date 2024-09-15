from eos.logging.logger import log
from eos.persistence.db_manager import DbManager
from eos.persistence.mongo_repository import MongoRepository
from eos.utils.singleton import Singleton


class GracefulTerminationMonitor(metaclass=Singleton):
    """
    The graceful termination monitor is responsible for tracking whether EOS has been terminated gracefully.
    """

    def __init__(self, db_manager: DbManager):
        self._globals = MongoRepository("globals", db_manager)
        self._globals.create_indices([("key", 1)], unique=True)

        graceful_termination = self._globals.get_one(key="graceful_termination")
        if not graceful_termination:
            self._globals.create({"key": "graceful_termination", "terminated_gracefully": False})
            self._terminated_gracefully = False
        else:
            self._terminated_gracefully = graceful_termination["terminated_gracefully"]
            if not self._terminated_gracefully:
                log.warning("EOS did not terminate gracefully!")

    def previously_terminated_gracefully(self) -> bool:
        return self._terminated_gracefully

    def terminated_gracefully(self) -> None:
        self._set_terminated_gracefully(True)
        log.debug("EOS terminated gracefully.")

    def _set_terminated_gracefully(self, value: bool) -> None:
        self._terminated_gracefully = value
        self._globals.update({"terminated_gracefully": value}, key="graceful_termination")
