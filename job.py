from __future__ import annotations

import logging
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, List, Callable, Any
from uuid import uuid4 as uid

from settings import TIME_FORMAT

logger = logging.getLogger(__name__)


class Status(Enum):
    IN_QUEUE = 0
    IN_PROGRESS = 1
    SUCCESS = 2
    ERROR = 3


class Job:
    def __init__(
            self,
            task: Callable,
            start_at: Optional[str] = None,
            max_working_time: Optional[float] = None,
            tries: int = 0,
            dependencies: Optional[list[Any]] | None = None
    ):
        self.start_at = datetime.strptime(start_at,
                                          TIME_FORMAT) if start_at else None
        self.duration = timedelta(seconds=max_working_time) if max_working_time else None
        if start_at and max_working_time:
            self.end_at = self.start_at + self.duration
        elif max_working_time:
            self.end_at = datetime.now() + self.duration
        else:
            self.end_at = None

        self.task = task()
        self.tries = tries
        self.max_working_time = max_working_time
        self.dependencies = dependencies or []
        self.success = False
        self.complete = False
        self.error = False
        self.status = Status.IN_QUEUE
        self.id = uid()
        self.name = task.__name__

    def run(self):
        # try:
        # result = next(self.task)
        return next(self.task)
        # except StopIteration:
        #     task.status = Status.SUCCESS
        #     logger.info(f"Задача {task} завершена со статусом {task.success}!")
        # except Exception as e:
        #     # logger.error(f'Ошибка выполнения задания {e}')
        #     return None
