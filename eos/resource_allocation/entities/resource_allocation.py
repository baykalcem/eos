from datetime import datetime, timezone

from pydantic import BaseModel


class ResourceAllocation(BaseModel):
    id: str
    owner: str
    experiment_id: str | None = None
    start_time: datetime | None = None
    created_at: datetime = datetime.now(tz=timezone.utc)

    class Config:
        arbitrary_types_allowed = True
