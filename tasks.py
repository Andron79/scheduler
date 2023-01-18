import logging
import time

logger = logging.getLogger(__name__)


def task_1():
    print('task_1 started')
    # raise ValueError('jhihuihui')
    yield
    # raise ValueError('jhihuihui')
    print('task_1 complete!')


def task_2():
    print('task_2 started')
    yield
    raise ValueError
    print('task_2 complete')


def task_3():
    print('task_3 started')
    # time.sleep(5)
    yield
    print('task_3 complete')
