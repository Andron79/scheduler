from __future__ import annotations

import logging
import time
from datetime import datetime
from threading import Timer

from job import Job
from settings import MAX_TASKS_COUNT

logger = logging.getLogger(__name__)


def coroutine(f):
    def wrap(*args, **kwargs):
        gen = f(*args, **kwargs)
        gen.send(None)
        return gen

    return wrap


class Scheduler:

    def __init__(self, pool_size: int = MAX_TASKS_COUNT):
        self.pool_size = pool_size
        self._queue: list[Job] = []

    def schedule(self, task: Job):
        """Adds a job to the FIFO queue."""
        if len(self._queue) >= self.pool_size:
            logger.error("Queue is full")
            return False
        if task.start_at and task.start_at < datetime.now():
            logger.info("Task %s is expired", task)
            return False
        if task.start_at and task.start_at > datetime.now():
            logger.info("Scheduling %s to start at %s", task, task.start_at)
            t = Timer((task.start_at - datetime.now()).total_seconds(),
                      self._queue.append, [task])
            t.start()
            return False
        # if task.dependencies:
        #     for dependency in task.dependencies:
        #         self.schedule(dependency)
        self._queue.append(task)
        print(self._queue)
        return True

    def get_task(self) -> Job | None:
        """Returns the next task to run."""
        logger.info('self._queue')
        if self._queue:
            # logger.info(self._queue)
            return self._queue.pop(0)

    def _run_task(self, task: Job | None) -> None:
        """Runs a task."""
        if task is None:
            # logger.info("Running %s", task)
            return
        try:
            logger.info("Running %s", task)
            result = task.run()
        except StopIteration:
            logger.info("Task %s finished", task)
            return
        self._queue.append(task)
        logger.info("Running %s", task)
        return result

    # @coroutine
    def run(self) -> None:
        """Runs the dispatcher."""
        logger.info("Starting tasks")
        print(self._queue)
        while True:
            task = self.get_task()
            logger.info(task)
            self._run_task(task)
            time.sleep(0.1)

    # @coroutine
    # def run(self, task):
    #     history = []
    #     while True:
    #         task = (yield)
    #         if task == "h":
    #             print(history)
    #             continue
    #         print(task)
    #         history.append(task)
    #
    # def restart(self):
    #     pass
    #
    # def stop(self):
    #     pass
