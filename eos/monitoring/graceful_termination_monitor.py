from eos.logging.logger import log
from eos.monitoring.repositories.global_repository import GlobalRepository
from eos.persistence.async_mongodb_interface import AsyncMongoDbInterface
from eos.utils.singleton import Singleton


class GracefulTerminationMonitor(metaclass=Singleton):
    """
    The graceful termination monitor is responsible for tracking whether EOS has been terminated gracefully.
    """

    def __init__(self, db_interface: AsyncMongoDbInterface):
        self._globals = GlobalRepository(db_interface)
        self._terminated_gracefully = False

    async def initialize(self) -> None:
        await self._globals.initialize()

        graceful_termination = await self._globals.get_one(key="graceful_termination")
        if not graceful_termination:
            await self._globals.create({"key": "graceful_termination", "terminated_gracefully": False})
            self._terminated_gracefully = False
        else:
            self._terminated_gracefully = graceful_termination["terminated_gracefully"]
            if not self._terminated_gracefully:
                log.warning("EOS did not terminate gracefully!")

    async def previously_terminated_gracefully(self) -> bool:
        return self._terminated_gracefully

    async def set_terminated_gracefully(self) -> None:
        await self._set_terminated_gracefully(True)
        log.debug("EOS terminated gracefully.")

    async def _set_terminated_gracefully(self, value: bool) -> None:
        self._terminated_gracefully = value
        await self._globals.update_one({"terminated_gracefully": value}, key="graceful_termination")
