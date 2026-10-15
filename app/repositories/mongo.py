from typing import Any

from pymongo import AsyncMongoClient
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import PyMongoError

from app.settings import MongoSettings


class MongoManager:
    def __init__(self, settings: MongoSettings | None = None) -> None:
        self.settings = settings or MongoSettings()
        self._client: AsyncMongoClient[dict[str, Any]] | None = None
        self._db: AsyncDatabase[dict[str, Any]] | None = None

    @property
    def db(self) -> AsyncDatabase[dict[str, Any]]:
        if self._db is None:
            raise RuntimeError("MongoDB is not connected")
        return self._db

    async def connect(self) -> None:
        if self._client is not None:
            return

        client: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(self.settings.uri)
        try:
            await client.admin.command("ping")
        except PyMongoError:
            await client.close()
            raise

        self._client = client
        self._db = client.get_database(self.settings.database_name)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None
            self._db = None


mongo_manager = MongoManager()
