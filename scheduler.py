from __future__ import annotations

import json
import pickle
import time
from collections import deque
import logging
from datetime import datetime
from pprint import pprint
from threading import Timer

from job import Job, Status
from schema import TaskSchema
from settings import MAX_TASKS_COUNT

logger = logging.getLogger(__name__)


class Scheduler:

    def __init__(self, pool_size: int = MAX_TASKS_COUNT):
        self.pool_size = pool_size
        self._queue = deque()

    def queue_is_full(self):
        return True if len(self._queue) >= self.pool_size else False

    @staticmethod
    def timetable_task(task):
        return True if task.start_at and task.start_at > datetime.now() else False

    def add_task(self, task: Job):
        if self.timetable_task(task):
            logger.info(f"Задача {task} ожидает старта в {task.start_at}")
            t = Timer((task.start_at - datetime.now()).total_seconds(),
                      self._queue.append, [task])
            t.start()
            return False

        if task.dependencies:
            for dependency in task.dependencies:
                logger.info(f"Задача {task.name} ожидает окончания выполнения зависимости {dependency.name}")
                self.add_task(dependency)

        if task:
            self._queue.append(task)
            # task.status = Status.IN_QUEUE
            return True

    def get_task(self):
        """
         Допускает новую запущенную задачу в планировщик
        """
        if self._queue:
            return self._queue.popleft()

    def exit(self, task):
        # if task.status != Status.ERROR:
        #     task.status = Status.SUCCESS
        logger.info(f'Задача {task.name} завершена со статусом {task.status.name}')
        return

    def process_task(self, task: Job | None) -> Job | None:

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

        if task.status == Status.ERROR and task.tries >= 0:
            logger.info(f"Задача {task.name} осталось попыток перезапуска {task.tries}")
            task.tries -= 1
            if task not in self._queue:
                self._queue.appendleft(task)
                return

        try:
            result = task.run()
        except StopIteration:
            task.status = Status.SUCCESS
            self.exit(task)
            return

        except Exception as e:
            task.status = Status.ERROR
            logger.error(f'Задание {task.name} завершилось со статусом {task.status.name} - {e}')
            return
        self.add_task(task)

    def run(self) -> None:
        logger.info("Планировщик запущен")
        while True:
            try:
                task = self.get_task()
                self.process_task(task)
                time.sleep(0.1)
            except KeyboardInterrupt:
                logger.info('Пользователь остановил работу планировщика')
                self.stop()
                return

    def stop(self) -> None:  # TODO
        """
        Метод останавливает работу планировщика.
        :return:
        """
        tasks_json = []
        for task in self._queue:
            logger.info(f'{task.name} status {task.status}')
            task_dict = task.__dict__
            task_dict['dependencies'] = [x.__dict__ for x in task_dict['dependencies']]
            tasks_json.append(TaskSchema.parse_obj(task.__dict__).json())
            logger.error(TaskSchema.parse_obj(task.__dict__).json())
        pprint(tasks_json)
        with open('data.json', 'w') as f:
            json.dump(tasks_json, f)
        logger.info('Состояние задач сохранено в файл')
