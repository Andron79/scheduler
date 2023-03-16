from __future__ import annotations

import pathlib
import pickle
import time
from collections import deque
import logging
from datetime import datetime
from threading import Timer
from typing import Iterable, Type, Union

from job import Job, Status
from settings import MAX_TASKS_COUNT, SAVED_TASKS_FILE
from tasks import worker_tasks

logger = logging.getLogger(__name__)


class Scheduler:

    def __init__(self, pool_size: int = MAX_TASKS_COUNT):
        self.pool_size: int = pool_size
        self._queue: deque = deque()

    def queue_is_full(self) -> bool:
        return True if len(self._queue) >= self.pool_size else False

    @staticmethod
    def timetable_task(task) -> bool:
        return True if task.start_at and task.start_at > datetime.now() else False

    def add_task(self, task: Job) -> Union[bool, None]:
        """
         Добавляет новую задачу в очередь, устанавливает параметры задачи.
        """
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
            task.status = Status.IN_QUEUE
            return True

    def get_task(self) -> Union[Job, Type[KeyboardInterrupt]]:
        """
         Допускает новую запущенную задачу в событийный цикл, а если задач нет, останавливает планировщик.
        """
        if self._queue:
            return self._queue.popleft()
        else:
            raise KeyboardInterrupt

    def process_task(self, task: Union[Job, None]) -> None:
        """
         Подготовка и запуск задачи.
        """

        if self.queue_is_full():
            logger.error("Очередь заполнена")
            return

        if task is None:
            return

        if task.end_at and task.end_at < datetime.now():
            logger.info(f"Задача {task} остановлена, время исполнения превысило {task.max_working_time} cek.")
            return

        # datetime.now() + self.duration
        # end_at = datetime.now() + self.duration

        # task_end_at = datetime.now() + task.duration
        # if task.max_working_time and (datetime.now() + task.duration) < task_end_at:
        #     logger.info(f"Задача {task} остановлена, время исполнения превысило {task.max_working_time} cek.")
        #     return

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
            logger.info(f'Задача {task.name} завершена со статусом {task.status.name}')
            return

        except Exception as e:
            task.status = Status.ERROR
            logger.error(f'Задание {task.name} завершилось со статусом {task.status.name} - {e}')
            return
        self.add_task(task)
        return result

    def run(self) -> None:
        """
         Событийный цикл.
        """
        logger.info("Планировщик запущен")
        # time.sleep(10)
        self.start()
        while True:
            try:
                task = self.get_task()
                self.process_task(task)
                time.sleep(0.1)
            except KeyboardInterrupt:
                logger.info('Работа планировщика завершена')
                self.stop()
                return

    def stop(self) -> None:
        """
        Метод останавливает работу планировщика и записывает невыполненные таски в файл, если они есть.
        :return:
        """
        tasks_to_save = []
        for task in self._queue:
            task_dict = task.__dict__
            task_dict['task'] = str(task.name)
            task_dict['dependencies'] = [x.__dict__ for x in task_dict['dependencies']]
            tasks_to_save.append(task_dict)
        if tasks_to_save:
            with open(SAVED_TASKS_FILE, 'wb') as f:
                pickle.dump(tasks_to_save, f)
                logger.info('Состояние задач сохранено в файл')

    def start(self) -> None:
        """
        Метод читает из файла невыполненные таски и добавляет их в очередь на выполнение.
        :return:
        """
        tasks_file = pathlib.Path(SAVED_TASKS_FILE)
        if tasks_file.exists():
            with open('saved_tasks.lock', 'rb') as f:
                tasks_data = pickle.load(f)
                if tasks_data:
                    for task in tasks_data:
                        job = Job(
                            task=worker_tasks.get(task['name'])
                        )
                        self.add_task(job)

            tasks_file.unlink(missing_ok=False)
            logger.info(f'Состояние задач прочитано из файла {SAVED_TASKS_FILE}, файл удален удален.')
