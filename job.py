from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, Callable, Any, Generator

from settings import TIME_FORMAT

logger = logging.getLogger(__name__)


class Status(Enum):
    IN_QUEUE = 0
    SUCCESS = 1
    ERROR = 2
    EXPIRED = 3
    IN_PROGRESS = 4


class Job:
    task_id = 0

    def __init__(
            self,
            task: Callable,
            start_at: Optional[str] = None,
            max_working_time: Optional[float] = None,
            tries: int = 0,
            dependencies: Optional[list[Job]] | None = None
    ):
        self.start_at = datetime.strptime(start_at,
                                          TIME_FORMAT) if start_at else None
        self.duration = timedelta(seconds=max_working_time) if max_working_time else None
        Job.task_id += 1
        self.tid = Job.task_id

        if start_at and max_working_time:
            self.end_at = self.start_at + self.duration
        elif max_working_time:
            self.end_at = datetime.now() + self.duration
        else:
            self.end_at = None

        self.task: Any = task()
        self.tries: Optional[int] = tries
        self.max_working_time: Optional[float] = max_working_time
        self.dependencies: Optional[list[Job]] = dependencies or []
        self.status: Status = Status.IN_QUEUE
        self.name: str = task.__name__

    def run(self):
        result = self.task
        return result

    def task_dependencies_is_complete(self) -> bool:
        """
        Метод проверяет что все зависимости у задачи выполнены.
        :return:
        """
        for dependency in self.dependencies:
            if dependency.status == Status.SUCCESS:
                continue
            else:
                return False
        return True
