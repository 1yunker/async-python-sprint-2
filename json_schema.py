from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from job import Status


class TaskSchemaBase(BaseModel):
    name: str
    target: str
    args: list
    kwargs: dict
    start_at: datetime
    max_working_time: Optional[float]
    tries: int
    # dependencies: list
    status: Status


class TaskJsonSchema(TaskSchemaBase):
    dependencies: list[TaskSchemaBase]
