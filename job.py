from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, Callable, Any

from settings import TIME_FORMAT

logger = logging.getLogger(__name__)


class Status(Enum):
    IN_QUEUE = 0
    SUCCESS = 1
    ERROR = 2


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

        self.task: Any = task()
        self.tries: Optional[int] = tries
        self.max_working_time: Optional[float] = max_working_time
        self.dependencies: Optional[list[Any]] = dependencies or []
        self.status: Status = Status.IN_QUEUE
        self.name: str = task.__name__

    def run(self):

        # if self.end_at and self.end_at < datetime.now():
        #     logger.info(f"Задача {self.task} остановлена, время исполнения превысило {self.task.max_working_time} cek.")
        #     return


        return next(self.task)
