from typing import Any
from uuid import uuid4

from pymongo.asynchronous.database import AsyncDatabase

from app.repositories.models import JobDocument, JobStatus
from app.repositories.queries import find_job, insert_job
from app.schemas import CreateJobRequest
from saq.queue.redis import RedisQueue


async def create_job(
    db: AsyncDatabase[dict[str, Any]],
    request: CreateJobRequest,
    queue: RedisQueue,
) -> JobDocument:
    job = JobDocument(
        id=f"job_{uuid4().hex}",
        url=request.url,
        payload=request.payload,
        status=JobStatus.PENDING,
    )
    await insert_job(db, job)
    await queue.enqueue("process_job", id=job.id)
    return job


async def get_job(
    db: AsyncDatabase[dict[str, Any]],
    job_id: str,
) -> JobDocument | None:
    return await find_job(db, job_id)
