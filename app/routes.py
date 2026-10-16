from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from pymongo.asynchronous.database import AsyncDatabase
from saq.queue.redis import RedisQueue

from app.dependencies import get_db, get_redis_queue
from app.handlers.job import create_job, get_job
from app.schemas import CreateJobRequest, JobResponse

router = APIRouter()


@router.post(
    "/jobs",
    response_model=JobResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def submit_job(
    body: CreateJobRequest,
    db: Annotated[AsyncDatabase, Depends(get_db)],
    queue: Annotated[RedisQueue, Depends(get_redis_queue)],
) -> JobResponse:
    job = await create_job(db, body, queue)
    return JobResponse(
        id=job.id,
        status=job.status,
        attempts=job.attempts,
        next_retry_at=job.next_retry_at,
    )


@router.get("/jobs/{job_id}", response_model=JobResponse)
async def read_job(
    job_id: str,
    db: Annotated[AsyncDatabase, Depends(get_db)],
) -> JobResponse:
    job = await get_job(db, job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        )

    return JobResponse(
        id=job.id,
        status=job.status,
        attempts=job.attempts,
        next_retry_at=job.next_retry_at,
    )
