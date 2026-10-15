from typing import Any

from pymongo.asynchronous.database import AsyncDatabase

from app.repositories.models import JobDocument


async def insert_job(db: AsyncDatabase[dict[str, Any]], job: JobDocument) -> None:
    document = job.model_dump(mode="json", exclude={"id"})
    document["_id"] = job.id
    await db["jobs"].insert_one(document)


async def find_job(
    db: AsyncDatabase[dict[str, Any]],
    job_id: str,
) -> JobDocument | None:
    document = await db["jobs"].find_one({"_id": job_id})
    if document is None:
        return None

    document["id"] = document.pop("_id")
    return JobDocument.model_validate(document)
