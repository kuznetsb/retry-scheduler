from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.settings import RedisSettings


class RedisManager:
    def __init__(self, settings: RedisSettings | None = None) -> None:
        self.settings = settings or RedisSettings()
        self._client: Redis | None = None

    @property
    def client(self) -> Redis:
        if self._client is None:
            raise RuntimeError("Redis is not connected")
        return self._client

    async def connect(self) -> None:
        if self._client is not None:
            return

        client = Redis.from_url(self.settings.url)
        try:
            await client.ping()
        except RedisError:
            await client.aclose()
            raise

        self._client = client

    async def close(self) -> None:
        client = self._client
        self._client = None
        if client is not None:
            await client.aclose()


redis_manager = RedisManager()
