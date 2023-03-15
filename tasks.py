import logging
import time
import pathlib

import requests

logger = logging.getLogger(__name__)


def task_1():
    logger.warning('task_1 started')
    yield
    logger.warning('task_1 complete!')


def task_2():
    logger.warning('task_2 started')
    p = pathlib.Path()
    directory = pathlib.Path('data')
    if not directory.exists():
        directory.mkdir()
    logger.info(f'Директорий существует {directory.exists()}')
    yield
    if directory.exists():
        directory.rmdir()
        logger.info(f'Директорий существует {directory.exists()}')
    logger.warning('task_2 complete')


def task_3():
    logger.warning('task_3 started')

    p = pathlib.Path(__file__)
    yield

    logger.warning('task_3 complete')


def target():
    try:
        while True:
            data_chunk = (yield)
            print(f"Target: Получено {data_chunk}")
    except GeneratorExit:
        print("Target: Завершение")


def pipe():
    output = target()
    output.send(None)
    try:
        while True:
            data_chunk = (yield)
            print(f"Pipe: Обработка {data_chunk}")
            output.send(data_chunk * 2)
    except GeneratorExit:
        pass


def source():
    output = pipe()
    output.send(None)
    for data in range(5):
        print(f"Source: Отправлено {data}")
        output.send(data)
    output.close()
    yield


def api_exact_time():
    response = requests.get("http://worldtimeapi.org/api/timezone/Europe/Moscow")
    logger.warning(response.json()['datetime'])
    yield
    logger.warning('Задание api_exact_time завершено')


worker_tasks = {
    'task_1': task_1,
    'task_2': task_2,
    'task_3': task_3,
    'target': target,
    'pipe': pipe,
    'source': source,
    'api_exact_time': api_exact_time,
}