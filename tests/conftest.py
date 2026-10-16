import asyncio
from collections.abc import AsyncIterator, Callable
from copy import deepcopy
from types import SimpleNamespace
from typing import Any

import pytest
import pytest_asyncio
from aiohttp import web
from aiohttp.test_utils import TestServer

from app.repositories.models import JobStatus


def _job_document(status: JobStatus = JobStatus.PENDING) -> dict[str, Any]:
    return {
        "_id": "job-test",
        "url": "https://example.test/hook",
        "payload": {"event": "created"},
        "status": status.value,
        "attempts": 0,
        "next_retry_at": None,
    }


class FakeJobsCollection:
    def __init__(self, document: dict[str, Any] | None = None) -> None:
        self.document = deepcopy(document) if document is not None else None
        self.update_count = 0
        self._update_lock = asyncio.Lock()
        self.read_barrier: asyncio.Barrier | None = None

    async def insert_one(self, document: dict[str, Any]) -> SimpleNamespace:
        self.document = deepcopy(document)
        return SimpleNamespace(inserted_id=document["_id"])

    async def find_one(self, query: dict[str, Any]) -> dict[str, Any] | None:
        if self.read_barrier is not None:
            await self.read_barrier.wait()
        if self.document is None or query.get("_id") != self.document["_id"]:
            return None
        return deepcopy(self.document)

    async def update_one(
        self,
        query: dict[str, Any],
        update: dict[str, dict[str, Any]],
    ) -> SimpleNamespace:
        async with self._update_lock:
            self.update_count += 1
            if (
                self.document is None
                or query.get("_id") != self.document["_id"]
                or query.get("status") != self.document["status"]
            ):
                return SimpleNamespace(matched_count=0)

            self.document.update(deepcopy(update["$set"]))
            return SimpleNamespace(matched_count=1)


class FakeDatabase:
    def __init__(
        self,
        status: JobStatus = JobStatus.PENDING,
        *,
        empty: bool = False,
    ) -> None:
        document = None if empty else _job_document(status)
        self.jobs = FakeJobsCollection(document)

    def __getitem__(self, collection: str) -> FakeJobsCollection:
        if collection != "jobs":
            raise KeyError(collection)
        return self.jobs


class FakeQueue:
    def __init__(self) -> None:
        self.enqueued: list[tuple[str, dict[str, Any]]] = []

    async def enqueue(self, function: str, **kwargs: Any) -> object:
        self.enqueued.append((function, kwargs))
        return object()


@pytest.fixture
def make_fake_db() -> Callable[..., FakeDatabase]:
    def factory(
        status: JobStatus = JobStatus.PENDING,
        *,
        empty: bool = False,
    ) -> FakeDatabase:
        return FakeDatabase(status, empty=empty)

    return factory


@pytest.fixture
def fake_queue() -> FakeQueue:
    return FakeQueue()


@pytest_asyncio.fixture
async def http_server() -> AsyncIterator[
    Callable[[web.RequestHandler], Any]
]:
    servers: list[TestServer] = []

    async def serve(handler: web.RequestHandler) -> str:
        app = web.Application()
        app.router.add_post("/hook", handler)
        server = TestServer(app)
        await server.start_server()
        servers.append(server)
        return str(server.make_url("/hook"))

    yield serve

    for server in servers:
        await server.close()
