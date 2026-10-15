from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, HttpUrl


class JobStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"


class JobDocument(BaseModel):
    id: str
    url: HttpUrl
    payload: dict[str, Any]
    status: JobStatus = JobStatus.PENDING
    attempts: int = 0
    next_retry_at: datetime | None = None
