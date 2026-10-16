import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from pymongo.asynchronous.database import AsyncDatabase
from saq.job import Job

from app.handlers import job as job_handler
from app.handlers.job import (
    JOB_STATUS_TRANSITIONS,
    claim_job,
    create_job,
    fail_job,
    get_job,
    transition_job,
)
from app.repositories.models import JobStatus
from app.schemas import CreateJobRequest


@pytest.mark.asyncio
async def test_create_job_persists_then_enqueues_with_saq_key_and_arguments(
    make_fake_db: Any,
    fake_queue: Any,
) -> None:
    fake_db = make_fake_db(empty=True)
    db = cast(AsyncDatabase[dict[str, Any]], fake_db)
    request = CreateJobRequest(
        url="https://example.test/hook",
        payload={"event": "created"},
    )

    created = await create_job(db, request, cast(Any, fake_queue))

    assert created.status == JobStatus.PENDING
    assert fake_db.jobs.document["_id"] == created.id
    assert fake_db.jobs.document["status"] == JobStatus.PENDING.value
    assert len(fake_queue.enqueued) == 1
    function, kwargs = fake_queue.enqueued[0]
    assert function == "run_job"
    assert kwargs["key"] == created.id
    assert kwargs["job_id"] == created.id
    assert kwargs["url"] == str(created.url)
    assert kwargs["payload"] == request.payload
    assert kwargs["retries"] == job_handler.MAX_JOB_ATTEMPTS


@pytest.mark.asyncio
async def test_enqueue_error_is_propagated_after_job_is_persisted(
    make_fake_db: Any,
) -> None:
    class FailingQueue:
        async def enqueue(self, function: str, **kwargs: Any) -> None:
            raise ConnectionError("Redis unavailable")

    fake_db = make_fake_db(empty=True)
    db = cast(AsyncDatabase[dict[str, Any]], fake_db)

    with pytest.raises(ConnectionError, match="Redis unavailable"):
        await create_job(
            db,
            CreateJobRequest(url="https://example.test/hook", payload={}),
            cast(Any, FailingQueue()),
        )

    assert fake_db.jobs.document is not None
    assert fake_db.jobs.document["status"] == JobStatus.PENDING.value


@pytest.mark.asyncio
async def test_get_job_returns_document(make_fake_db: Any) -> None:
    fake_db = make_fake_db()

    result = await get_job(cast(AsyncDatabase[dict[str, Any]], fake_db), "job-test")

    assert result is not None
    assert result.id == "job-test"
    assert result.status == JobStatus.PENDING
    assert result.payload == {"event": "created"}


@pytest.mark.asyncio
async def test_get_job_returns_none_for_missing_document(make_fake_db: Any) -> None:
    fake_db = make_fake_db(empty=True)

    assert await get_job(
        cast(AsyncDatabase[dict[str, Any]], fake_db),
        "missing",
    ) is None


@pytest.mark.asyncio
async def test_transition_job_updates_status_and_fields(make_fake_db: Any) -> None:
    fake_db = make_fake_db()

    await transition_job(
        cast(AsyncDatabase[dict[str, Any]], fake_db),
        "job-test",
        JobStatus.RUNNING,
        attempts=1,
        next_retry_at=None,
    )

    assert fake_db.jobs.document["status"] == JobStatus.RUNNING.value
    assert fake_db.jobs.document["attempts"] == 1
    assert fake_db.jobs.document["next_retry_at"] is None


@pytest.mark.asyncio
async def test_transition_rejects_illegal_failed_to_dead_transition(
    make_fake_db: Any,
) -> None:
    fake_db = make_fake_db(JobStatus.FAILED)

    with pytest.raises(ValueError, match="failed -> dead"):
        await transition_job(
            cast(AsyncDatabase[dict[str, Any]], fake_db),
            "job-test",
            JobStatus.DEAD,
        )

    assert fake_db.jobs.update_count == 0
    assert JOB_STATUS_TRANSITIONS[JobStatus.FAILED] == {JobStatus.RUNNING}


@pytest.mark.asyncio
async def test_transition_detects_status_change_between_read_and_update(
    make_fake_db: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_db = make_fake_db()
    monkeypatch.setattr(
        job_handler,
        "update_job",
        AsyncMock(return_value=False),
    )

    with pytest.raises(RuntimeError, match="status changed"):
        await transition_job(
            cast(AsyncDatabase[dict[str, Any]], fake_db),
            "job-test",
            JobStatus.RUNNING,
        )


@pytest.mark.asyncio
async def test_transition_raises_for_missing_job(make_fake_db: Any) -> None:
    fake_db = make_fake_db(empty=True)

    with pytest.raises(LookupError, match="does not exist"):
        await transition_job(
            cast(AsyncDatabase[dict[str, Any]], fake_db),
            "missing",
            JobStatus.RUNNING,
        )


@pytest.mark.asyncio
async def test_pending_job_can_be_claimed_once(make_fake_db: Any) -> None:
    fake_db = make_fake_db()
    db = cast(AsyncDatabase[dict[str, Any]], fake_db)

    assert await claim_job(db, "job-test", 1)
    assert not await claim_job(db, "job-test", 1)
    assert fake_db.jobs.document["status"] == JobStatus.RUNNING.value
    assert fake_db.jobs.document["attempts"] == 1


@pytest.mark.asyncio
async def test_failed_job_is_claimed_when_retry_is_due(make_fake_db: Any) -> None:
    fake_db = make_fake_db(JobStatus.FAILED)
    fake_db.jobs.document["next_retry_at"] = datetime.now(UTC) - timedelta(seconds=1)

    assert await claim_job(
        cast(AsyncDatabase[dict[str, Any]], fake_db),
        "job-test",
        2,
    )
    assert fake_db.jobs.document["status"] == JobStatus.RUNNING.value
    assert fake_db.jobs.document["attempts"] == 2
    assert fake_db.jobs.document["next_retry_at"] is None


@pytest.mark.asyncio
async def test_failed_job_is_not_claimed_before_due_time(make_fake_db: Any) -> None:
    fake_db = make_fake_db(JobStatus.FAILED)
    fake_db.jobs.document["next_retry_at"] = datetime.now(UTC) + timedelta(minutes=1)

    assert not await claim_job(
        cast(AsyncDatabase[dict[str, Any]], fake_db),
        "job-test",
        2,
    )
    assert fake_db.jobs.update_count == 0


@pytest.mark.asyncio
async def test_failed_job_without_retry_time_is_not_claimed(make_fake_db: Any) -> None:
    fake_db = make_fake_db(JobStatus.FAILED)

    assert not await claim_job(
        cast(AsyncDatabase[dict[str, Any]], fake_db),
        "job-test",
        2,
    )
    assert fake_db.jobs.update_count == 0


@pytest.mark.parametrize(
    "status",
    [JobStatus.RUNNING, JobStatus.COMPLETED, JobStatus.DEAD],
)
@pytest.mark.asyncio
async def test_ineligible_jobs_are_not_claimed(
    status: JobStatus,
    make_fake_db: Any,
) -> None:
    fake_db = make_fake_db(status)

    assert not await claim_job(
        cast(AsyncDatabase[dict[str, Any]], fake_db),
        "job-test",
        2,
    )
    assert fake_db.jobs.update_count == 0


@pytest.mark.asyncio
async def test_missing_job_raises_instead_of_silently_skipping(
    make_fake_db: Any,
) -> None:
    fake_db = make_fake_db(empty=True)

    with pytest.raises(LookupError, match="does not exist"):
        await claim_job(
            cast(AsyncDatabase[dict[str, Any]], fake_db),
            "missing",
            1,
        )


@pytest.mark.asyncio
async def test_two_concurrent_claims_only_allow_one_winner(make_fake_db: Any) -> None:
    fake_db = make_fake_db()
    fake_db.jobs.read_barrier = asyncio.Barrier(2)
    db = cast(AsyncDatabase[dict[str, Any]], fake_db)

    results = await asyncio.gather(
        claim_job(db, "job-test", 1),
        claim_job(db, "job-test", 1),
    )

    assert results.count(True) == 1
    assert results.count(False) == 1


@pytest.mark.parametrize(
    ("attempt", "delay_minutes"),
    [(1, 1), (2, 2), (3, 4), (4, 8)],
)
@pytest.mark.asyncio
async def test_transient_retry_delay_is_deterministic_and_doubles(
    attempt: int,
    delay_minutes: int,
    make_fake_db: Any,
) -> None:
    fake_db = make_fake_db(JobStatus.RUNNING)
    saq_job = Job("run_job", retries=5)
    saq_job.attempts = attempt
    before = datetime.now(UTC)

    await fail_job(
        cast(AsyncDatabase[dict[str, Any]], fake_db),
        "job-test",
        attempt,
        saq_job,
        transient=True,
    )

    retry_at = fake_db.jobs.document["next_retry_at"]
    assert fake_db.jobs.document["status"] == JobStatus.FAILED.value
    assert fake_db.jobs.document["attempts"] == attempt
    assert saq_job.retry_delay == delay_minutes * 60
    assert isinstance(retry_at, datetime)
    assert retry_at >= before + timedelta(minutes=delay_minutes)


@pytest.mark.asyncio
async def test_permanent_failure_disables_retries_and_has_no_due_time(
    make_fake_db: Any,
) -> None:
    fake_db = make_fake_db(JobStatus.RUNNING)
    saq_job = Job("run_job", retries=5)
    saq_job.attempts = 2

    await fail_job(
        cast(AsyncDatabase[dict[str, Any]], fake_db),
        "job-test",
        2,
        saq_job,
        transient=False,
    )

    assert fake_db.jobs.document["status"] == JobStatus.FAILED.value
    assert fake_db.jobs.document["attempts"] == 2
    assert fake_db.jobs.document["next_retry_at"] is None
    assert saq_job.retries == 2
    assert not saq_job.retryable


@pytest.mark.asyncio
async def test_final_transient_failure_goes_directly_to_dead_in_one_update(
    make_fake_db: Any,
) -> None:
    fake_db = make_fake_db(JobStatus.RUNNING)
    saq_job = Job("run_job", retries=5)
    saq_job.attempts = 5

    await fail_job(
        cast(AsyncDatabase[dict[str, Any]], fake_db),
        "job-test",
        5,
        saq_job,
        transient=True,
    )

    assert fake_db.jobs.document["status"] == JobStatus.DEAD.value
    assert fake_db.jobs.document["attempts"] == 5
    assert fake_db.jobs.document["next_retry_at"] is None
    assert fake_db.jobs.update_count == 1
