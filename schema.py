import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel
from pydantic.class_validators import Optional

from job import Status


class TaskSchemaModel(BaseModel):
    id: UUID
    name: str
    args: Optional[list] = None
    kwargs: Optional[dict] = None
    start_datetime: Optional[datetime.datetime] = None
    max_working_time: Optional[int] = None
    tries: int
    status: Status
    dependencies: Optional[list[Any]]


class TaskSchema(TaskSchemaModel):
    dependencies: list[TaskSchemaModel]
