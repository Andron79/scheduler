import logging

from job import Job
from scheduler import Scheduler
from tasks import print_hellow, print_hellow1, make_and_delete_dirs, write_lines, hello

logger = logging.getLogger(__name__)


#
# def coroutine(f):
#     def wrap(*args, **kwargs):
#         gen = f(*args, **kwargs)
#         gen.send(None)
#         return gen
#
#     return wrap
#
#
# @coroutine
# def run():
#     history = []
#     while True:
#         task = (yield)
#         if task == "h":
#             print(task)
#             continue
#         # yield task
#         print(task)
#         history.append(task)


def main():
    scheduler = Scheduler()
    # job = Job(make_and_delete_dirs)
    # scheduler.schedule(job)
    # scheduler.run()
    # job = Job(print_hellow1)
    # scheduler.schedule(job)
    # scheduler.run()
    job = Job(hello)
    scheduler.schedule(job)
    # scheduler.run()
    job = Job(print_hellow1)
    scheduler.schedule(job)
    # scheduler.run()


if __name__ == '__main__':
    main()
