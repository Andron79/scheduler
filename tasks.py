import logging
import os
import shutil

logger = logging.getLogger(__name__)


def print_hellow():
    print('Hellow World1111111111111')
    yield


def print_hellow1():
    logger.info("make_and_delete_dirs finished")
    for i in range(9):
        print(i)
        yield
    logger.info("make_and_delete_dirs finished")
    # return 10


def make_and_delete_dirs():
    logger.info("make_and_delete_dirs start")
    for number in range(2):
        dir_name = f"dir_{number}"
        if not os.path.isdir(dir_name):
            os.makedirs(dir_name)
        shutil.rmtree(dir_name)
        yield
    logger.info("make_and_delete_dirs finished")


def write_lines():
    logger.info("write_lines start")
    with open("lines.txt", "w") as file:
        file.writelines([f"{number}\n" for number in range(100)])
        yield
    logger.info("write_lines finished")


def hello():
    value = yield 'Hello'
    print('value = {}'.format(value))
