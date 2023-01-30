from __future__ import annotations

import json
import pickle
import time
from collections import deque
import logging
from datetime import datetime
from functools import wraps
# from functools import wraps
from threading import Timer
from typing import Optional

from job import Job, Status, TaskSchema
from settings import MAX_TASKS_COUNT

logger = logging.getLogger(__name__)


# def coroutine(func):
#     @wraps(func)
#     def inner(*args, **kwargs):
#         fn = func(*args, **kwargs)
#         fn.send(None)
#         return fn
#
#     return inner


# def coroutine(func):
#     def starter(*args, **kwargs):
#         gen = func(*args, **kwargs)
#         # logger.info(gen)
#         next(gen)
#         return gen
#     return starter


class Scheduler:

    def __init__(self, pool_size: int = MAX_TASKS_COUNT):
        self.scheduler_run = None
        self.pool_size = pool_size
        self._queue = deque()
        # self.msg: str = ""

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
                if dependency not in self._queue:
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

    # @coroutine
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
        #
        # if task.status.ERROR and task.tries >= 0:
        #     logger.info(f"Задача {task} {task.error} осталось попыток перезапуска {task.tries}")
        #     task.tries -= 1
        #     if task not in self._queue:
        #         self._queue.append(task)
        #         # self.add_task(task)
        #
        # if task.status.IN_QUEUE:
        #     logger.info(f"Задача {task} добавлена снова в очередь")
        #     # task.tries -= 1
        #     if task not in self._queue:
        #         self.add_task(task)
            # self._queue.append(task)

        try:
            logger.info(task.__dict__)
            result = task.run()
            task.status = Status.IN_QUEUE

        except StopIteration:
            task.status = Status.SUCCESS
            logger.info(f"Задача {task} завершена со статусом {task.status}!")
            # self._queue.append(task)
            return task
        task.status = Status.IN_QUEUE
        # task.status = Status.ERROR
        self.add_task(task)
        # except Exception as err:
        #     logger.error(f'Job id={task.id} is fail with {err}')
        #     if task.tries > 0:
        #         # task.set_next_start_datetime()
        #         task.tries -= 1
        #         task.status = Status.IN_QUEUE
        #     else:
        #         task.status = Status.ERROR

    def get_or_create_job(
            self,
            id,
            fn_name,
            args,
            kwargs,
            start_at,
            max_working_time,
            tries,
            status,
            dependencies
    ) -> Job:
        """
        Метод получает все необходимые для создания Job параметры. Пытается найти в добавленных
        с таким же id. Если находит то, возвращает найденный, если не находит то, создает новый и
        возвращает его. Если при завершении статус Job был in_progress и он не был завершен
        то он переводится в in_queue чтоб стартавать Job заново.
        :param id:
        :param fn_name:
        :param args:
        :param kwargs:
        :param start_at:
        :param max_working_time:
        :param tries:
        :param status:
        :param dependencies:
        :return:
        """
        job = self.get_task_in_scheduler_tasks(id)

        if job:
            return job
        return Job(
            id=task.id,
            task=self.task,
            args=args,
            kwargs=kwargs,
            # start_at=start_at,
            max_working_time=max_working_time,
            tries=tries,
            status=Status.IN_QUEUE if status == Status.IN_PROGRESS else status,
            dependencies=dependencies
        )

    def get_task_in_scheduler_tasks(self, task_id) -> Optional[Job]:
        """
        Пытается найти в добавленных Job с таким же id. Если находит то, возвращает найденный,
        если не находит то, создает новый и
        возвращает его.
        :param task_id:
        :return:
        """
        try:
            job = next(x for x in self._queue if x.id == task_id)
        except StopIteration:
            job = None
        return

    def run(self) -> None:
        # with open('222222.lock', 'rb') as f:
        #     data = pickle.load(f)
        # logger.info(data)
        logger.info("Планировщик запущен")
        while True:
            try:
                task = self.get_task()
                self.process_task(task)
                time.sleep(0.1)
            except KeyboardInterrupt:
                self.stop()
                return

    def stop(self) -> None:  # TODO
        """
        Метод останавливает работу планировщика.
        :return:
        """
        self.scheduler_run = False
        tasks_json = []
        for task in self._queue:
            task_dict = task.__dict__
            task_dict['dependencies'] = [x.__dict__ for x in task_dict['dependencies']]
            tasks_json.append(TaskSchema.parse_obj(task.__dict__).json())
            logger.info(task)

        with open('data.json', 'w') as f:
            json.dump(tasks_json, f)
        logger.info('Пользователь остановил работу планировщика')
