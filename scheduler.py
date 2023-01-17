from __future__ import annotations

import time
from collections import deque
import logging
from datetime import datetime
from threading import Timer

from job import Job
from settings import MAX_TASKS_COUNT

logger = logging.getLogger(__name__)


class Scheduler:

    def __init__(self, pool_size: int = MAX_TASKS_COUNT):
        self.pool_size = pool_size
        self._queue = deque()

    def queue_is_full(self):
        return True if len(self._queue) >= self.pool_size else False

    @staticmethod
    def future_task(task):
        return True if task.start_at and task.start_at > datetime.now() else False

    def schedule(self, task: Job):
        if self.future_task(task):
            logger.info(f"Задача {task} ожидает старта в {task.start_at}")
            t = Timer((task.start_at - datetime.now()).total_seconds(),
                      self._queue.append, [task])
            t.start()
            return False

        if task.dependencies:
            for dependency in task.dependencies:
                self.schedule(dependency)

        self._queue.append(task)
        return True

    def new_task(self):
        """
         Допускает новую запущенную задачу в планировщик
        """
        if self._queue:
            return self._queue.popleft()

    def check_and_run_task(self, task: Job | None) -> None:

        if self.queue_is_full():
            logger.error("Очередь заполнена")
            return

        if task is None:
            return

        if task.end_at and task.end_at < datetime.now():
            logger.info(f"Задача {task} просрочена")
            return

        if self.future_task(task):
            logger.info(f"Задача {task} просрочена")
            return

        if task.dependencies:
            for dependency in task.dependencies:
                if dependency in self._queue:
                    logger.info(f"Задача {task} ожидает окончания выполнения зависимости {dependency}")
                    self._queue.append(task)
                    return

        try:
            result = task.run()
        except StopIteration:
            logger.info(f"Выполнение задачи {task} закончено")
            return
        self._queue.append(task)
        return result

    def run(self) -> None:
        logger.info("Задачи запущены")
        while True:
            try:
                task = self.new_task()
                self.check_and_run_task(task)
                time.sleep(0.1)
            except KeyboardInterrupt:
                logger.info('Пользователь остановил исполнение планировщика')
                break
