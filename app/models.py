from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, HttpUrl

JobStatus = Literal["pending", "running", "completed", "failed", "dead"]


class JobDocument(BaseModel):
    id: str
    url: HttpUrl
    payload: dict[str, Any]
    status: JobStatus = "pending"
    attempts: int = 0
    next_retry_at: datetime | None = None
