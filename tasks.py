import logging
import time
import pathlib
from pprint import pprint

import requests

logger = logging.getLogger(__name__)


def task_1():
    logger.warning('task_1 started')
    yield ValueError('task_1_error')
    logger.warning('task_1 complete!')


def task_2():
    logger.warning('task_2 started')
    p = pathlib.Path()
    directory = pathlib.Path('data')
    print(directory.exists())
    yield
    directory.mkdir(parents=False, exist_ok=False)
    print(directory.exists())
    logger.warning('task_2 complete')


def task_3():
    logger.warning('task_3 started')

    p = pathlib.Path(__file__)
    print(p)
    # time.sleep(10)
    # try:
    # t = 1 / 0
    # except ZeroDivisionError:
    #     # raise ValueError('task_1_error')
    #     yield ValueError('task_3_error')
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


def api_exact_time():
    response = requests.get("http://worldtimeapi.org/api/timezone/Europe/Moscow")
    logger.warning(response.json()['datetime'])
    yield
