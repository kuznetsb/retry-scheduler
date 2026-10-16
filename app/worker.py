import asyncio

from redis.asyncio import Redis
from saq import Worker
from saq.queue.redis import RedisQueue

from app.repositories.redis import redis_manager
from app.settings import RedisSettings


def worker_settings() -> dict[str, object]:
    settings = RedisSettings()
    queue = RedisQueue(
        Redis.from_url(settings.url),
        name=settings.queue_name,
    )
    return {"queue": queue, "functions": []}


async def main() -> None:
    await redis_manager.connect()
    try:
        queue = RedisQueue(
            redis_manager.client,
            name=redis_manager.settings.queue_name,
        )
        await queue.connect()
        worker = Worker(queue, functions=[])
        await worker.start()
    finally:
        await redis_manager.close()


if __name__ == "__main__":
    asyncio.run(main())
