from typing import Any, cast

import pytest
from fastapi import HTTPException
from fastapi.routing import APIRoute
from pydantic import ValidationError
from pymongo.asynchronous.database import AsyncDatabase

from app.repositories.models import JobStatus
from app.routes import read_job, router, submit_job
from app.schemas import CreateJobRequest


def test_create_request_rejects_invalid_destination_url() -> None:
    with pytest.raises(ValidationError):
        CreateJobRequest(url="not a URL", payload={})


def test_submit_route_is_configured_as_accepted() -> None:
    submit_route = next(
        route
        for route in router.routes
        if isinstance(route, APIRoute)
        and route.path == "/jobs"
        and "POST" in route.methods
    )

    assert submit_route.status_code == 202


@pytest.mark.asyncio
async def test_submit_returns_accepted_job_response(
    make_fake_db: Any,
    fake_queue: Any,
) -> None:
    fake_db = make_fake_db(empty=True)

    response = await submit_job(
        CreateJobRequest(
            url="https://example.test/hook",
            payload={"event": "created"},
        ),
        cast(AsyncDatabase[dict[str, Any]], fake_db),
        cast(Any, fake_queue),
    )

    assert response.status == JobStatus.PENDING
    assert response.attempts == 0
    assert response.next_retry_at is None
    assert response.id == fake_queue.enqueued[0][1]["job_id"]


@pytest.mark.asyncio
async def test_read_returns_existing_job(make_fake_db: Any) -> None:
    fake_db = make_fake_db(JobStatus.COMPLETED)

    response = await read_job(
        "job-test",
        cast(AsyncDatabase[dict[str, Any]], fake_db),
    )

    assert response.id == "job-test"
    assert response.status == JobStatus.COMPLETED


@pytest.mark.asyncio
async def test_read_missing_job_returns_not_found(make_fake_db: Any) -> None:
    fake_db = make_fake_db(empty=True)

    with pytest.raises(HTTPException) as raised:
        await read_job(
            "missing",
            cast(AsyncDatabase[dict[str, Any]], fake_db),
        )

    assert raised.value.status_code == 404
    assert raised.value.detail == "Job not found"
