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


def coroutine(f):
    def wrap(*args, **kwargs):
        gen = f(*args, **kwargs)
        gen.send(None)
        return gen

    return wrap


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
        # if self.timetable_task(task):
        #     logger.info(f"Задача {task} ожидает старта в {task.start_at}")
        #     t = Timer((task.start_at - datetime.now()).total_seconds(),
        #               self._queue.append, [task])
        #     t.start()
        #     return False
        #
        # if task.dependencies:
        #     for dependency in task.dependencies:
        #         if dependency not in self._queue:
        #             logger.info(f"Задача {task} ожидает окончания выполнения зависимости {dependency}")
        #             self.add_task(dependency)

        self._queue.append(task)
        # logger.info(len(self._queue))
        return True

    def get_task(self, task):
        """
         Допускает новую запущенную задачу в планировщик
        """

        while self.scheduler_run:
            # self._queue.sort()
            if self._queue[0].status == Status.IN_QUEUE:
                # time_next_task = self._queue[0].start_at
                # now_datetime = datetime.now()
                #
                # logger.info(
                #     f'Next task start at {time_next_task}'
                # )
                # time_to_next_task = time_next_task.timestamp() - now_datetime.timestamp()
                #
                # if time_to_next_task > 0:
                #     time.sleep(time_to_next_task)

                if self._queue[0].dependencies_is_success() is False:
                    self._queue[0].set_next_start_datetime()

                if not self.queue_is_full():
                    self._queue[0].status = Status.IN_PROGRESS
                    task.send(self._queue[0])
                else:
                    logger.info('Pool size more than 10')

        # if self._queue:
        #     logger.info(self._queue)
        #     return self._queue.popleft()

    # @coroutine
    def process_task(self, task: Job | None) -> Job | None:

        if self.queue_is_full():
            logger.error("Очередь заполнена")
            return

        if task is None:
            return

        # if task.end_at and task.end_at < datetime.now():
        #     logger.info(f"Задача {task} остановлена, время исполнения превысило {task.max_working_time} cek.")
        #     return

        # if self.timetable_task(task):
        #     logger.info(f"Задача {task} просрочена")
        #     return

        # if not task.error and task.tries >= 0:
        #     logger.info(f"Задача {task} {task.error} осталось попыток перезапуска {task.tries}")
        #     task.tries -= 1
        #     # if task not in self._queue:
        #     self._queue.append(task)
        #         # self.add_task(task)
        #     return
        #
        # if task.status.IN_QUEUE:
        #     logger.info(f"Задача {task} добавлена снова в очередь")
        #     # task.tries -= 1
        #     if task not in self._queue:
        #         self.add_task(task)
        # self._queue.append(task)
        # result = task.run()
        try:
            logger.info(type(task))
            # logger.info(task.__dict__)
            result = task.run()
            logger.info(type(result))
            # logger.info(type(next(result)))
            next(result)
            logger.info(type(result))
            # logger.info(type(next(result)))
            # next(result)
            # next(result)
            # next(task.run())
            # next(result)
            # task.success = True
            # logger.info(self._queue.append(task))

        except StopIteration:
            task.status = Status.SUCCESS
            logger.info(f"Задача {task} завершена со статусом {task.status}!")
            # self._queue.append(task)
            return
        # self._queue.append(task)
        return result

    @coroutine
    def execute_job(self):
        while task := (yield):
            try:

                logger.error(task)
                logger.info(
                    f'Number of tasks {(self.count_tasks_in_queue(Status.IN_QUEUE))} '
                    f'from {len(self._queue)}, '
                    f'Active tasks {(self.count_tasks_in_queue(Status.IN_PROGRESS))}.'
                )
                result = task.run()
                task.status = Status.SUCCESS
                next(result)
                next(result)
            except Exception as err:
                logger.error(f'Job id={task.id} is fail with {err}')
                if task.tries > 0:
                    task.set_next_start_datetime()
                    task.tries -= 1
                    task.status = Status.IN_QUEUE
                else:
                    task.status = Status.ERROR

    def count_tasks_in_queue(self, status) -> int:
        """
        Метод возвращает количество Job в очереди.
        :return:
        """
        return len([task for task in self._queue if task.status == status])

    def get_task_in_scheduler_tasks(self, job_id) -> Optional[Job]:
        """
        Пытается найти в добавленных Job с таким же id. Если находит то, возвращает найденный,
        если не находит то, создает новый и
        возвращает его.

        :param job_id:
        :return:
        """
        try:
            job = next(job for job in self._queue if job.id == job_id)
        except StopIteration:
            job = None
        return job

    def get_or_create_job(
            self,
            # id,
            task,
            args,
            kwargs,
            start_datetime,
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
        :param task:
        :param args:
        :param kwargs:
        :param start_datetime:
        :param max_working_time:
        :param tries:
        :param status:
        :param dependencies:
        :return:
        """
        job = self.get_task_in_scheduler_tasks(task.id)
        logger.info('------------------------------------')

        if job:
            return job
        return Job(
            # id=id,
            task=task,
            args=args,
            kwargs=kwargs,
            start_at=start_datetime,
            max_working_time=max_working_time,
            tries=tries,
            status=Status.IN_QUEUE if status == Status.IN_PROGRESS else status,
            dependencies=dependencies
        )

    # def run(self) -> None:
    #     logger.info("Планировщик запущен")
    #     while True:
    #         try:
    #             task = self.get_task()
    #             self.process_task(task)
    #             time.sleep(0.1)
    #         except KeyboardInterrupt:
    #             # self.stop()
    #             return

    def run(self) -> None:
        """
        Метод генератор смотрит когда, стартует следующий Job. Ждет этого момента.
        Если у Job есть не выполненные зависимости откладывает ее. Если зависимостей нет или они
        завершены передает Job на выполнение.

        :return: Job
        """
        logger.info("Планировщик запущен")
        self.scheduler_run = True
        # while True:
        try:
            # self.get_or_create_job()
            execute = self.execute_job()
            self.get_task(execute)
        except KeyboardInterrupt:
            # self.stop()
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

# class Scheduler:
#
#     def __init__(self, pool_size: int = MAX_TASKS_COUNT):
#         self.pool_size = pool_size
#         self._queue = deque()
#         self.msg: str = ""
#
#     def queue_is_full(self):
#         return True if len(self._queue) >= self.pool_size else False
#
#     @staticmethod
#     def timetable_task(task):
#         return True if task.start_at and task.start_at > datetime.now() else False
#
#     def save_task(self, task: Job):
#         current_task = dict(task.__dict__)
#         del current_task['task']
#         # logger.error(current_task)
#         with open(f'tasks/{task.name}.lock', "ab") as f:
#             pickle.dump(current_task, f)
#         logger.info(f"Сохраненная задача : {task}")
#
#     def add_task(self, task: Job):
#         # self.save_task(task)
#         if self.timetable_task(task):
#             logger.info(f"Задача {task} ожидает старта в {task.start_at}")
#             t = Timer((task.start_at - datetime.now()).total_seconds(),
#                       self._queue.append, [task])
#             t.start()
#             return False
#
#         if task.dependencies:
#             for dependency in task.dependencies:
#                 logger.info(f"Задача {task} ожидает окончания выполнения зависимости {dependency}")
#                 self.add_task(dependency)
#
#         self._queue.append(task)
#         return True
#
#     def get_task(self):
#         """
#          Допускает новую запущенную задачу в планировщик
#         """
#         if self._queue:
#             return self._queue.popleft()
#
#     def process_task(self, task: Job | None) -> Job | None:
#
#         if self.queue_is_full():
#             logger.error("Очередь заполнена")
#             return
#
#         if task is None:
#             return
#
#         if task.end_at and task.end_at < datetime.now():
#             logger.info(f"Задача {task} остановлена, время исполнения превысило {task.max_working_time} cek.")
#             return
#
#         if self.timetable_task(task):
#             logger.info(f"Задача {task} просрочена")
#             return
#
#         if not task.error and task.tries >= 0:
#             logger.info(f"Задача {task} {task.error} осталось попыток перезапуска {task.tries}")
#             task.tries -= 1
#             if task not in self._queue:
#                 self._queue.append(task)
#
#         try:
#             result = task.run()
#             logger.warning(result)
#             task.success = True
#
#         except StopIteration:
#             self.save_task(task)
#             logger.info(f"Задача {task} завершена со статусом {task.success}!")
#             return task
#
#     def run(self) -> None:
#         logger.info("Задачи запущены")
#         while True:
#             try:
#                 task = self.get_task()
#                 self.process_task(task)
#                 time.sleep(0.1)
#             except KeyboardInterrupt:
#                 logger.info('Пользователь остановил работу планировщика')
#                 return
