import json
import logging
import time
from typing import Generator

logger = logging.getLogger(__name__)


def task_1():
    logger.warning('task_1 started')
    yield ValueError('task_1_error')
    logger.warning('task_1 complete!')


def task_2():
    logger.warning('task_2 started')
    k = 1 / 0
    yield
    logger.warning('task_2 complete')


def task_3():
    logger.warning('task_3 started')
    yield
    # time.sleep(10)
    # try:
    #     x = 1 / 0
    # except ZeroDivisionError as e:
    #     return None
    # yield
    # tasks_json = {'a': 'b'}
    # with open('task.json', 'w') as f:
    #     json.dump(tasks_json, f)
    logger.warning('task_3 complete')
    # yield
