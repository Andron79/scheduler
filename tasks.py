import logging
import time
import pathlib
from typing import Generator

import requests

logger = logging.getLogger(__name__)


def task_1() -> None:
    logger.warning('task_1 started')
    # time.sleep(2)
    x = 1 / 0
    yield
    # time.sleep(4)
    logger.warning('task_1 complete!')


def task_2() -> None:
    logger.warning('task_2 started')
    # p = pathlib.Path()
    directory = pathlib.Path('data')
    if not directory.exists():
        directory.mkdir()
    logger.warning(f'Директорий существует {directory.exists()}')
    yield
    if directory.exists():
        directory.rmdir()
        logger.warning(f'Директорий существует {directory.exists()}')
    logger.warning('task_2 complete')


def task_3() -> None:
    logger.warning('task_3 started')
    yield
    logger.warning('task_3 complete')


def stage_3() -> Generator:
    aa = (yield)
    try:
        while True:
            data_chunk = (yield)
            print(f"Stage_3: Получено {data_chunk}")
            return data_chunk
            # yield data_chunk
    except GeneratorExit:
        print("Stage_3: Завершение фабрики")
        return aa


def stage_2() -> Generator:
    output = stage_3()
    output.send(None)
    try:
        while True:
            data_chunk = (yield)
            print(f"Stage_2: Обработка {data_chunk}")
            output.send(data_chunk * 2)
    except GeneratorExit:
        pass


def stage_1() -> None:
    output = stage_2()
    output.send(None)
    data = 10
    print(f"Stage_1: Отправлено {data}")
    output.send(data)
    output.close()
    yield


def api_exact_time() -> None:
    response = requests.get("http://worldtimeapi.org/api/timezone/Europe/Moscow")
    logger.warning(response.json()['datetime'])
    yield
    logger.warning('Задание api_exact_time завершено')


worker_tasks = {
    'task_1': task_1,
    'task_2': task_2,
    'task_3': task_3,
    'stage_3': stage_3,
    'stage_2': stage_2,
    'stage_1': stage_1,
    'api_exact_time': api_exact_time,
}
