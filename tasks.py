import logging
import time

logger = logging.getLogger(__name__)


def task_1():
    logger.warning('task_1 started')
    yield ValueError('task_1_error')
    logger.warning('task_1 complete!')


def task_2():
    logger.warning('task_2 started')
    # yield
    # logger.warning('task_2 complete')


def task_3():
    logger.warning('task_3 started')
    # time.sleep(10)
    # try:
    # t = 1 / 0
    # except ZeroDivisionError:
    #     # raise ValueError('task_1_error')
    #     yield ValueError('task_3_error')
    yield

    logger.warning('task_3 complete')
