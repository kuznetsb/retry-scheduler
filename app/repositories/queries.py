from typing import Any

from pymongo.asynchronous.database import AsyncDatabase

from app.repositories.models import JobDocument, JobStatus


async def insert_job(db: AsyncDatabase[dict[str, Any]], job: JobDocument) -> None:
    """Insert a job document into MongoDB.

    Args:
        db: Database containing the ``jobs`` collection.
        job: Job document to persist.
    """
    document = job.model_dump(mode="json", exclude={"id"})
    document["_id"] = job.id
    await db["jobs"].insert_one(document)


async def find_job(
    db: AsyncDatabase[dict[str, Any]],
    job_id: str,
) -> JobDocument | None:
    """Find a job by its identifier.

    Args:
        db: Database containing the ``jobs`` collection.
        job_id: Identifier of the job to retrieve.

    Returns:
        The validated job document, or ``None`` if no job matches.
    """
    document = await db["jobs"].find_one({"_id": job_id})
    if document is None:
        return None

    document["id"] = document.pop("_id")
    return JobDocument.model_validate(document)


async def update_job(
    db: AsyncDatabase[dict[str, Any]],
    job_id: str,
    expected_status: JobStatus,
    updates: dict[str, Any],
) -> bool:
    """Apply updates if the job still has the expected status.

    The conditional single-document update is atomic in MongoDB. It allows
    callers to detect stale reads and prevents concurrent claims or transitions
    from the same status from both succeeding.

    Args:
        db: Database containing the ``jobs`` collection.
        job_id: Identifier of the job to update.
        expected_status: Required current status for the update to match.
        updates: Fields and values to set on the job document.

    Returns:
        ``True`` if a document matched and was updated, otherwise ``False``.
    """
    result = await db["jobs"].update_one(
        {"_id": job_id, "status": expected_status.value},
        {"$set": updates},
    )
    return result.matched_count > 0
