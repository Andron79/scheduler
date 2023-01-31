from __future__ import annotations

import logging
import pickle
from datetime import timedelta
from enum import Enum
from typing import Optional, List, Callable, Any, Generator
from uuid import uuid4 as uid

from settings import TIME_FORMAT

import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel

logger = logging.getLogger(__name__)


class TaskSchemaInner(BaseModel):
    id: UUID
    fn_name: str
    args: list
    kwargs: dict
    start_datetime: datetime.datetime
    max_working_time: Optional[int]
    tries: int
    status: Status
    dependencies: list


class TaskSchema(TaskSchemaInner):
    dependencies: list[TaskSchemaInner]


class Status(Enum):
    IN_QUEUE = 0
    IN_PROGRESS = 1
    SUCCESS = 2
    ERROR = 3


class Job:
    def __init__(
            self,
            task,
            start_at: Optional[str] = None,
            max_working_time: Optional[float] = None,
            tries: int = 0,
            dependencies: Optional[list[Any]] | None = None,
            args=None,
            kwargs=None,
            status=Status(0)
    ):
        self.status: Status = status
        self.start_at = datetime.datetime.strptime(start_at, TIME_FORMAT) if start_at else None
        self.duration = timedelta(seconds=max_working_time) if max_working_time else None

        if start_at and max_working_time:
            self.end_at = self.start_at + self.duration
        elif max_working_time:
            self.end_at = datetime.datetime.now() + self.duration
        else:
            self.end_at = None

        self.task = task
        self.tries = tries or 0
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}
        self.max_working_time = max_working_time or None
        self.dependencies = dependencies or []
        # self.complete: bool = False
        self.error: bool = False
        self.id: uid = uid()
        self.name: str = task.__name__

    def __str__(self):
        return f'{self.task.__name__}, id={self.id}'

    def run(self):
        try:
            return self.task(*self.args, **self.kwargs)
            # return next(self.task)
        # except StopIteration:
        #     logger.info(f"Задача {self.task} завершена на 1 ексепшене")
        #     return None

        except Exception as e:
            logger.error(f'Ошибка выполнения задания {e}')
            return None

    def set_next_start_datetime(self) -> None:
        """
        Метод вычисляет новое время запуска Job если он не смог по какой-то причины быть
        выполнен в свое время.

        :return:
        """
        self.start_at += self.duration

    def dependencies_is_success(self) -> bool:
        """
        Метод проверяет что все зависимости выполнены.
        :return Bool:
        """
        for dependency in self.dependencies:
            if dependency.status != Status.SUCCESS:
                return False
        return True

    def check_dependencies_task_start_datetime(self):
        """
        Метод проверяет что время запуска зависимостей раньше чем у основного Job.

        :return:
        """
        for dependencies_task in self.dependencies:
            if dependencies_task.start_datetime > self.start_at:
                self.start_at = dependencies_task.start_datetime + datetime.timedelta(
                    minutes=1)

    def __lt__(self, other) -> bool:
        """
        Метод переопределяет сортировку так чтоб в начале списка были Job с статусом in_queue и
        ближайшим временем.

        :param other:
        :return:
        """
        if other.status.value != self.status.value:
            return other.status.value > self.status.value
        return other.start_at > self.start_at
