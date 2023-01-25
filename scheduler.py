from __future__ import annotations

import pickle
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
    def timetable_task(task):
        return True if task.start_at and task.start_at > datetime.now() else False

    def save_task(self, task: Job):
        current_task = dict(task.__dict__)
        del current_task['task']
        # logger.error(current_task)
        with open(f'{task.name}.lock', "ab") as f:
            pickle.dump(current_task, f)
        logger.info(f"Сохраненная задача : {task}")

    def add_task(self, task: Job):
        self.save_task(task)
        if self.timetable_task(task):
            logger.info(f"Задача {task} ожидает старта в {task.start_at}")
            t = Timer((task.start_at - datetime.now()).total_seconds(),
                      self._queue.append, [task])
            t.start()
            return False

        if task.dependencies:
            for dependency in task.dependencies:
                logger.info(f"Задача {task} ожидает окончания выполнения зависимости {dependency}")
                self.add_task(dependency)

        self._queue.append(task)
        return True

    def get_task(self):
        """
         Допускает новую запущенную задачу в планировщик
        """
        if self._queue:
            return self._queue.popleft()

    def process_task(self, task: Job | None) -> str | None:

        if self.queue_is_full():
            logger.error("Очередь заполнена")
            return

        if task is None:
            return

        if task.end_at and task.end_at < datetime.now():
            logger.info(f"Задача {task} остановлена, время исполнения превысило {task.max_working_time} cek.")
            return

        if self.timetable_task(task):
            logger.info(f"Задача {task} просрочена")
            return

        if not task.error and task.tries >= 0:
            logger.info(f"Задача {task} {task.error} осталось попыток перезапуска {task.tries}")
            task.tries -= 1
            if task not in self._queue:
                self._queue.append(task)

        try:
            with open(f'{task.name}.lock', 'rb') as f:
                data = pickle.load(f)
            logger.info(data)
            result = task.run()
            logger.warning(result)
            task.success = True

        except StopIteration:
            self.save_task(task)
            logger.info(f"Задача {task} завершена со статусом {task.success}!")
            return

    def run(self) -> None:
        logger.info("Задачи запущены")
        while True:
            try:
                task = self.get_task()
                self.process_task(task)
                time.sleep(0.1)
            except KeyboardInterrupt:
                logger.info('Пользователь остановил работу планировщика')
                return
