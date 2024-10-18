from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from motor.core import AgnosticClientSession
from motor.motor_asyncio import AsyncIOMotorClient


class AsyncMongoDbSessionFactory:
    def __init__(self, db_client: AsyncIOMotorClient):
        self._db_client = db_client

    @asynccontextmanager
    async def __call__(self) -> AsyncGenerator[AgnosticClientSession, None]:
        """
        Async context manager for MongoDB sessions with transactions.
        Usage:
        async with db_manager.transaction_session_factory() as session:
            # Perform operations within the session and transaction
        """
        session = await self._db_client.start_session()
        try:
            async with session.start_transaction():
                try:
                    yield session
                except Exception:
                    await session.abort_transaction()
                    raise
        finally:
            await session.end_session()
