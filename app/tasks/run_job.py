import asyncio
import logging
from typing import Any

from aiohttp import (
    ClientConnectionError,
    ClientPayloadError,
    ClientResponseError,
    ClientSession,
    ClientTimeout,
)
from saq.types import Context

from app.handlers.job import claim_job, fail_job, transition_job
from app.repositories.mongo import mongo_manager
from app.repositories.models import JobStatus

logger = logging.getLogger(__name__)
RETRYABLE_CLIENT_ERROR_STATUSES = {408, 425, 429}

# Assumption: job payloads are always delivered using HTTP POST.
JOB_HTTP_METHOD = "POST"

# Assumption: a 30-second timeout is sufficient for the receiving endpoint.
REQUEST_TIMEOUT_SECONDS = 30


def _is_transient_http_status(status: int) -> bool:
    if 400 <= status < 500:
        return status in RETRYABLE_CLIENT_ERROR_STATUSES
    return 500 <= status < 600


def _is_transient_failure(error: Exception) -> bool:
    if isinstance(error, ClientResponseError):
        return _is_transient_http_status(error.status)
    return isinstance(
        error,
        (asyncio.TimeoutError, ClientConnectionError, ClientPayloadError),
    )


async def run_job(
    ctx: Context,
    *,
    job_id: str,
    url: str,
    payload: dict[str, Any],
) -> None:
    """Deliver a job payload and persist the outcome.

    Claims the job in MongoDB before sending its payload using the configured
    HTTP method and timeout. Duplicate or already-claimed deliveries are
    skipped. On success, the job is marked completed. On failure, the failure
    is recorded and SAQ retry behavior is configured according to whether the
    error is transient; the original exception is then re-raised.

    Args:
        ctx: SAQ task context containing the current job and its attempt count.
        job_id: Identifier of the job document in MongoDB.
        url: Destination URL that receives the job payload.
        payload: JSON-serializable job data sent to the destination.

    Raises:
        RuntimeError: If the SAQ context does not contain the current job.
        Exception: If delivery fails, after recording the failure and
            configuring retry behavior.
        LookupError: If the corresponding MongoDB job does not exist.
        ValueError: If the resulting job status transition is not allowed.
    """
    saq_job = ctx.get("job")
    if saq_job is None:
        raise RuntimeError("SAQ task context is missing the current job")

    db = mongo_manager.db
    attempt = saq_job.attempts
    if not await claim_job(db, job_id, attempt):
        logger.info("Skipping duplicate or already-claimed job [id=%s]", job_id)
        return

    timeout = ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
    try:
        async with ClientSession(timeout=timeout) as session:
            async with session.request(JOB_HTTP_METHOD, url, json=payload) as response:
                response.raise_for_status()
    except Exception as error:
        await fail_job(
            db,
            job_id,
            attempt,
            saq_job,
            transient=_is_transient_failure(error),
        )
        logger.exception(f"Job [id={job_id}] payload delivery failed")
        raise

    await transition_job(
        db,
        job_id,
        JobStatus.COMPLETED,
        attempts=attempt,
        next_retry_at=None,
    )
