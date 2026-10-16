import asyncio

from redis.asyncio import Redis
from saq import Worker
from saq.queue.redis import RedisQueue

from app.repositories.mongo import mongo_manager
from app.repositories.redis import redis_manager
from app.settings import RedisSettings
from app.tasks.run_job import run_job


def worker_settings() -> dict[str, object]:
    settings = RedisSettings()
    queue = RedisQueue(
        Redis.from_url(settings.url),
        name=settings.queue_name,
    )
    return {"queue": queue, "functions": [run_job]}


async def main() -> None:
    try:
        await mongo_manager.connect()
        await redis_manager.connect()
        queue = RedisQueue(
            redis_manager.client,
            name=redis_manager.settings.queue_name,
        )
        await queue.connect()
        worker = Worker(queue, functions=[run_job])
        await worker.start()
    finally:
        await redis_manager.close()
        await mongo_manager.close()


if __name__ == "__main__":
    asyncio.run(main())
