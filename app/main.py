from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis
from saq.queue.redis import RedisQueue
from saq.web.starlette import saq_web

from app.repositories.mongo import mongo_manager
from app.routes import router
from app.settings import RedisSettings


def create_app() -> FastAPI:
    settings = RedisSettings()
    queue = RedisQueue(
        Redis.from_url(settings.url),
        name=settings.queue_name,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
        try:
            await mongo_manager.connect()
            await queue.connect()
            await queue.redis.ping()
            app.state.db = mongo_manager.db
            app.state.redis_queue = queue
            yield
        finally:
            app.state.db = None
            app.state.redis_queue = None
            await queue.disconnect()
            await mongo_manager.close()

    app = FastAPI(
        title="Retry Scheduler",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.include_router(router)
    app.mount("/monitor", saq_web("/monitor", queues=[queue]))
    return app


asgi_application = create_app()
