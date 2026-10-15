from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from saq.queue.redis import RedisQueue

from app.repositories.mongo import mongo_manager
from app.routes import router
from app.settings import RedisSettings


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    queue: RedisQueue | None = None
    redis_settings = RedisSettings()
    try:
        await mongo_manager.connect()
        app.state.db = mongo_manager.db
        queue = RedisQueue.from_url(
            redis_settings.url,
            name=redis_settings.queue_name,
        )
        await queue.connect()
        await queue.redis.ping()
        app.state.redis_queue = queue
        yield
    finally:
        app.state.db = None
        app.state.redis_queue = None
        if queue is not None:
            await queue.disconnect()
        await mongo_manager.close()


def create_app() -> FastAPI:
    app = FastAPI(
        title="Retry Scheduler",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.include_router(router)
    return app


asgi_application = create_app()
