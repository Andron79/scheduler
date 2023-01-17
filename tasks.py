import logging
import time

logger = logging.getLogger(__name__)


def task_1(n: int = 10):
    while n > 0:
        # print('task_1', n)
        yield
        n -= 1
    print('task_1 complete!')


def task_2(n: int = 5):
    x = 0
    while x < n:
        # print('task_2', x)
        yield
        x += 1
    print('task_2 complete')


def task_3(n: int = 10):
    # while n > 0:
    #     # print('task_3', n)
    #     yield
    #     n -= 1
    print('task_3 started')
    time.sleep(5)
    yield
    print('task_3 complete')
