from typing import Any

from pydantic import BaseModel


class Container(BaseModel):
    id: str
    type: str
    lab: str

    location: str

    metadata: dict[str, Any] = {}

    class Config:
        arbitrary_types_allowed = True
