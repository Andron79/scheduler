import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel
from pydantic.class_validators import Optional

from job import Status


class TaskSchemaModel(BaseModel):
    id: UUID
    fn_name: str
    args: list
    kwargs: dict
    start_datetime: datetime.datetime
    max_working_time: Optional[int]
    tries: int
    status: Status
    dependencies: Optional[list[Any]]


class TaskSchema(TaskSchemaModel):
    dependencies: list[TaskSchemaModel]
