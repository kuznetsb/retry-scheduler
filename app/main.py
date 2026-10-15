from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from saq.queue.redis import RedisQueue

from app.repositories.mongo import mongo_manager
from app.repositories.redis import redis_manager
from app.routes import router


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    try:
        await mongo_manager.connect()
        await redis_manager.connect()
        queue = RedisQueue(
            redis_manager.client,
            name=redis_manager.settings.queue_name,
        )
        await queue.connect()
        app.state.db = mongo_manager.db
        app.state.redis_queue = queue
        yield
    finally:
        app.state.db = None
        app.state.redis_queue = None
        await redis_manager.close()
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
