from typing import Any

from fastapi import Request
from pymongo.asynchronous.database import AsyncDatabase
from saq.queue.redis import RedisQueue


def get_db(request: Request) -> AsyncDatabase[dict[str, Any]]:
    return request.app.state.db


def get_redis_queue(request: Request) -> RedisQueue:
    return request.app.state.redis_queue
