from datetime import datetime
from typing import Any

from pydantic import BaseModel, HttpUrl

from app.repositories.models import JobStatus


class CreateJobRequest(BaseModel):
    url: HttpUrl
    payload: dict[str, Any]


class JobResponse(BaseModel):
    id: str
    status: JobStatus
    attempts: int
    next_retry_at: datetime | None
