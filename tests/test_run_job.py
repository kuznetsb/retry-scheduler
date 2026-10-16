from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from aiohttp import (
    ClientConnectionError,
    ClientPayloadError,
    ClientResponseError,
    web,
)
from pymongo.asynchronous.database import AsyncDatabase
from saq.job import Job
from saq.types import Context

from app.repositories.models import JobStatus
from app.repositories.mongo import mongo_manager
from app.tasks.run_job import (
    REQUEST_TIMEOUT_SECONDS,
    _is_transient_failure,
    _is_transient_http_status,
    run_job,
)


@pytest.mark.parametrize(
    ("status", "expected_transient"),
    [
        (408, True),
        (425, True),
        (429, True),
        (500, True),
        (599, True),
        (400, False),
        (401, False),
        (404, False),
        (499, False),
        (600, False),
    ],
)
def test_http_status_retry_classification(
    status: int,
    expected_transient: bool,
) -> None:
    assert _is_transient_http_status(status) is expected_transient


@pytest.mark.parametrize(
    "error",
    [
        TimeoutError(),
        ClientConnectionError(),
        ClientPayloadError(),
    ],
)
def test_transport_errors_are_transient(error: Exception) -> None:
    assert _is_transient_failure(error)


def test_unclassified_error_is_not_transient() -> None:
    assert not _is_transient_failure(ValueError("invalid input"))


def test_request_timeout_is_configured() -> None:
    assert REQUEST_TIMEOUT_SECONDS == 30


@pytest.mark.asyncio
async def test_success_posts_payload_and_marks_job_completed(
    make_fake_db: Any,
    monkeypatch: pytest.MonkeyPatch,
    http_server: Any,
) -> None:
    fake_db = make_fake_db()
    monkeypatch.setattr(
        mongo_manager,
        "_db",
        cast(AsyncDatabase[dict[str, Any]], fake_db),
    )
    received: list[tuple[str, dict[str, Any]]] = []

    async def receive(request: web.Request) -> web.Response:
        received.append((request.method, await request.json()))
        return web.Response(status=202)

    url = await http_server(receive)
    saq_job = Job("run_job", retries=5)
    saq_job.attempts = 1

    await run_job(
        cast(Context, {"job": saq_job}),
        job_id="job-test",
        url=url,
        payload={"event": "created"},
    )

    assert received == [("POST", {"event": "created"})]
    assert fake_db.jobs.document["status"] == JobStatus.COMPLETED.value
    assert fake_db.jobs.document["attempts"] == 1
    assert fake_db.jobs.document["next_retry_at"] is None


@pytest.mark.asyncio
async def test_duplicate_delivery_skips_http_request(
    make_fake_db: Any,
    monkeypatch: pytest.MonkeyPatch,
    http_server: Any,
) -> None:
    fake_db = make_fake_db(JobStatus.COMPLETED)
    monkeypatch.setattr(
        mongo_manager,
        "_db",
        cast(AsyncDatabase[dict[str, Any]], fake_db),
    )
    requests: list[bool] = []

    async def receive(_: web.Request) -> web.Response:
        requests.append(True)
        return web.Response(status=200)

    url = await http_server(receive)
    saq_job = Job("run_job", retries=5)
    saq_job.attempts = 2

    await run_job(
        cast(Context, {"job": saq_job}),
        job_id="job-test",
        url=url,
        payload={},
    )

    assert requests == []
    assert fake_db.jobs.document["status"] == JobStatus.COMPLETED.value


@pytest.mark.parametrize(
    (
        "http_status",
        "attempt",
        "expected_status",
        "retry_delay_seconds",
        "expected_retryable",
    ),
    [
        pytest.param(400, 1, JobStatus.FAILED, None, False, id="permanent-400"),
        pytest.param(404, 1, JobStatus.FAILED, None, False, id="permanent-404"),
        pytest.param(408, 1, JobStatus.FAILED, 60, True, id="retryable-408"),
        pytest.param(425, 1, JobStatus.FAILED, 60, True, id="retryable-425"),
        pytest.param(429, 1, JobStatus.FAILED, 60, True, id="retryable-429"),
        pytest.param(500, 1, JobStatus.FAILED, 60, True, id="retryable-500"),
        pytest.param(503, 1, JobStatus.FAILED, 60, True, id="retryable-503"),
        pytest.param(503, 5, JobStatus.DEAD, None, False, id="attempt-limit"),
    ],
)
@pytest.mark.asyncio
async def test_http_failure_updates_job_and_retry_policy(
    http_status: int,
    attempt: int,
    expected_status: JobStatus,
    retry_delay_seconds: int | None,
    expected_retryable: bool,
    make_fake_db: Any,
    monkeypatch: pytest.MonkeyPatch,
    http_server: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake_db = make_fake_db()
    monkeypatch.setattr(
        mongo_manager,
        "_db",
        cast(AsyncDatabase[dict[str, Any]], fake_db),
    )

    async def respond(_: web.Request) -> web.Response:
        return web.Response(status=http_status)

    url = await http_server(respond)
    saq_job = Job("run_job", retries=5)
    saq_job.attempts = attempt
    before = datetime.now(UTC)

    with (
        caplog.at_level("ERROR", logger="app.tasks.run_job"),
        pytest.raises(ClientResponseError) as raised,
    ):
        await run_job(
            cast(Context, {"job": saq_job}),
            job_id="job-test",
            url=url,
            payload={},
        )

    assert raised.value.status == http_status
    assert fake_db.jobs.document["status"] == expected_status.value
    assert fake_db.jobs.document["attempts"] == attempt
    assert saq_job.retryable is expected_retryable
    assert "payload delivery failed" in caplog.text
    assert fake_db.jobs.update_count == 2

    if retry_delay_seconds is None:
        assert fake_db.jobs.document["next_retry_at"] is None
        assert saq_job.retry_delay == 0
    else:
        retry_at = fake_db.jobs.document["next_retry_at"]
        assert isinstance(retry_at, datetime)
        assert saq_job.retry_delay == retry_delay_seconds
        assert retry_at >= before + timedelta(seconds=retry_delay_seconds)


@pytest.mark.asyncio
async def test_missing_saq_job_context_raises() -> None:
    with pytest.raises(RuntimeError, match="missing the current job"):
        await run_job(
            cast(Context, {}),
            job_id="job-test",
            url="https://example.test/hook",
            payload={},
        )
