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
        self.msg: str = ""

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

        # if task.tries:
        #     logger.info(f"Задача {task} запущена с количеством повторений {task.tries} в случае неудачи")

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
            # logger.info(self._queue)
            return self._queue.popleft()

    def check_and_run_task(self, task: Job | None) -> str | None:

        if self.queue_is_full():
            logger.error("Очередь заполнена")
            return

        if task is None:
            return

        if task.end_at and task.end_at < datetime.now():
            logger.info(f"Задача {task} остановлена, время исполнения превысило {task.max_working_time} cek.")
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

        if not task.success and task.tries > 0:
            logger.info(f"Задача {task} {task.success} осталось попыток перезапуска {task.tries}")
            task.tries -= 1
            if task not in self._queue:
                self._queue.append(task)

        try:
            logger.info(f"Запускаем задачу {task}")
            result = task.run()
            task.success = True

        except ValueError as e:
            task.success = False
            logger.info(f"Задача {task} завершилась со статусом {task.success}")

        except StopIteration:
            logger.info(f"Задача {task} завершилась со статусом {task.success}")
            return

        # self._queue.append(task)
        # logger.info(self._queue)
        # logger.info(f"Задача {task} добавлена в очередь задач")
        # return result

    def run(self) -> None:
        logger.info("Задачи запущены")
        while True:
            try:
                task = self.new_task()
                self.check_and_run_task(task)

            except KeyboardInterrupt:
                logger.info('Пользователь остановил исполнение планировщика')
                return
