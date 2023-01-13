from __future__ import annotations
from collections import deque
import logging
import time
from datetime import datetime
from threading import Timer

from job import Job
from settings import MAX_TASKS_COUNT

logger = logging.getLogger(__name__)


class Scheduler:

    def __init__(self, pool_size: int = MAX_TASKS_COUNT):
        self.pool_size = pool_size
        self._queue: list[Job] = []
        self._task_queue = deque()

    def schedule(self, task: Job):
        """Adds a job to the FIFO queue."""
        if len(self._task_queue) >= self.pool_size:
            logger.error("Очередь заполнена")
            return False
        if task.start_at and task.start_at < datetime.now():
            logger.info(f"Задача {task} просрочена")
            return False
        if task.start_at and task.start_at > datetime.now():
            logger.info(f"Задача {task} стартует в {task.start_at}")
            t = Timer((task.start_at - datetime.now()).total_seconds(),
                      self._queue.append, [task])
            t.start()
            return False
        if task.dependencies:
            for dependency in task.dependencies:
                self.schedule(dependency)
        self._task_queue.append(task)

        # return True

    # def new_task(self, task):
    #     """
    #      Допускает новую запущенную задачу в планировщик
    #     """
    #     # """Returns the next task to run."""
    #     # logger.info('self._queue')
    #     self._task_queue.append(task)
    # if self._queue:
    #     # logger.info(self._queue)
    #     return self._queue.pop(0)

    # def _run_task(self, task: Job | None) -> None:
    #     """Runs a task."""
    #     if task is None:
    #         # logger.info("Running %s", task)
    #         return
    #     try:
    #         logger.info("Running %s", task)
    #         result = task.run()
    #     except StopIteration:
    #         logger.info("Task %s finished", task)
    #         return
    #     self._queue.append(task)
    #     logger.info("Running %s", task)
    #     return result

    # @coroutine
    # def run(self) -> None:
    #     """Runs the dispatcher."""
    #     logger.info("Starting tasks")
    #     print(self._queue)
    #     while True:
    #         task = self.get_task()
    #         logger.info(task)
    #         self._run_task(task)
    #         time.sleep(0.1)

    def run(self):
        """
        Работает, пока не останется задач
        """
        logger.info("Задачи запущены")
        while self._task_queue:
            task = self._task_queue.popleft()
            if task is None:
               return
            try:
                # logger.info("Работает до следующей инструкции yield")
                logger.info(f"Выполняется задача {task.__str__()}")
                task.run()
                self._task_queue.append(task)
            except StopIteration:
                logger.info("Выполнение задач закончено")
                pass
