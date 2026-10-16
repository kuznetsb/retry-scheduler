from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from pymongo.asynchronous.database import AsyncDatabase
from saq.job import Job
from saq.queue.redis import RedisQueue

from app.repositories.models import JobDocument, JobStatus
from app.repositories.queries import find_job, insert_job, update_job
from app.schemas import CreateJobRequest

# Assumption: five total execution attempts is an acceptable hardcoded limit.
MAX_JOB_ATTEMPTS = 5

# Assumption: a one-minute base delay with exponential growth is appropriate.
RETRY_BASE_DELAY_MINUTES = 1

# Modeled job statuses as a finite-state machine (FSM): each status lists
# allowed next statuses, and transition_job rejects transitions not listed here.
JOB_STATUS_TRANSITIONS = {
    JobStatus.PENDING: {JobStatus.RUNNING},
    JobStatus.RUNNING: {
        JobStatus.COMPLETED,
        JobStatus.FAILED,
        JobStatus.DEAD,
    },
    JobStatus.FAILED: {JobStatus.RUNNING},
    JobStatus.COMPLETED: set(),
    JobStatus.DEAD: set(),
}


async def create_job(
    db: AsyncDatabase[dict[str, Any]],
    request: CreateJobRequest,
    queue: RedisQueue,
) -> JobDocument:
    """Persist a new pending job and enqueue it for execution.

    Args:
        db: Database used to persist the job.
        request: URL and payload requested by the API caller.
        queue: SAQ queue used to schedule the job.

    Returns:
        The newly created job document.
    """
    job = JobDocument(
        id=f"job_{uuid4().hex}",
        url=request.url,
        payload=request.payload,
        status=JobStatus.PENDING,
    )
    await insert_job(db, job)
    await queue.enqueue(
        "run_job",
        # SAQ uses key to identify/deduplicate queue entries; job_id is passed
        # to the task so it can locate and update this MongoDB document.
        key=job.id,
        job_id=job.id,
        url=str(job.url),
        payload=job.payload,
        retries=MAX_JOB_ATTEMPTS,
    )
    return job


async def get_job(
    db: AsyncDatabase[dict[str, Any]],
    job_id: str,
) -> JobDocument | None:
    """Retrieve a job by its identifier.

    Args:
        db: Database containing the job.
        job_id: Identifier of the job to retrieve.

    Returns:
        The job document if found, otherwise ``None``.
    """
    return await find_job(db, job_id)


async def transition_job(
    db: AsyncDatabase[dict[str, Any]],
    job_id: str,
    target: JobStatus,
    **updates: Any,
) -> None:
    """Validate and persist a job status transition.

    The current status is checked against the FSM before the conditional
    MongoDB update is issued. If another update changes the status in between,
    the conditional update fails and this function raises ``RuntimeError``.

    Args:
        db: Database containing the job.
        job_id: Identifier of the job to transition.
        target: Status to transition the job to.
        **updates: Additional job fields to set with the new status.

    Raises:
        LookupError: If the job does not exist.
        ValueError: If the transition is not allowed by the FSM.
        RuntimeError: If the job status changes before the update succeeds.
    """
    job = await find_job(db, job_id)
    if job is None:
        raise LookupError(f"Job {job_id} does not exist")
    if target not in JOB_STATUS_TRANSITIONS[job.status]:
        raise ValueError(
            f"Invalid job transition for {job_id}: {job.status.value} -> {target.value}"
        )

    updated = await update_job(
        db,
        job_id,
        job.status,
        {"status": target.value, **updates},
    )
    if not updated:
        raise RuntimeError(f"Job {job_id} status changed during transition")


async def claim_job(
    db: AsyncDatabase[dict[str, Any]],
    job_id: str,
    attempt: int,
) -> bool:
    """Atomically claim a pending or due retry job before executing it.

    Multiple workers may read the same eligible state, but only one can update
    the document while its status still matches that state. Losing workers get
    ``False`` and must skip execution; this conditional update avoids a separate
    database lock.
    """
    job = await find_job(db, job_id)
    if job is None:
        raise LookupError(f"Job {job_id} does not exist")
    if job.status not in (JobStatus.PENDING, JobStatus.FAILED):
        return False
    if job.status == JobStatus.FAILED:
        if job.next_retry_at is None:
            return False
        if job.next_retry_at > datetime.now(UTC):
            return False

    return await update_job(
        db,
        job_id,
        job.status,
        {
            "status": JobStatus.RUNNING.value,
            "attempts": attempt,
            "next_retry_at": None,
        },
    )


async def fail_job(
    db: AsyncDatabase[dict[str, Any]],
    job_id: str,
    attempt: int,
    saq_job: Job,
    transient: bool,
) -> None:
    """Record a failed attempt and configure SAQ to retry transient failures.

    Permanent failures are marked failed and SAQ retries are disabled. Transient
    failures at the attempt limit transition directly to dead. Otherwise, a
    transient failure is marked failed with a ``next_retry_at`` timestamp. Its
    calculated delay is also assigned to ``saq_job.retry_delay`` in seconds, so
    SAQ schedules the retry in Redis when the task re-raises its exception.

    Args:
        db: Database containing the job document.
        job_id: Identifier of the failed job.
        attempt: Current 1-based execution attempt number.
        saq_job: SAQ job whose retry behavior is being configured.
        transient: Whether the failure is eligible for retry.
    """
    if not transient:
        saq_job.retries = attempt
        await transition_job(
            db,
            job_id,
            JobStatus.FAILED,
            attempts=attempt,
            next_retry_at=None,
        )
        return

    if not saq_job.retryable:
        await transition_job(
            db,
            job_id,
            JobStatus.DEAD,
            attempts=attempt,
            next_retry_at=None,
        )
        return

    retry_delay = timedelta(minutes=RETRY_BASE_DELAY_MINUTES * 2 ** (attempt - 1))
    saq_job.retry_delay = retry_delay.total_seconds()

    await transition_job(
        db,
        job_id,
        JobStatus.FAILED,
        attempts=attempt,
        next_retry_at=datetime.now(UTC) + retry_delay,
    )
