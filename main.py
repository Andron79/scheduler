import logging

from tasks import task_2, task_3, source, api_exact_time, task_1
from job import Job
from scheduler import Scheduler

logger = logging.getLogger(__name__)


def main():
    # s = source()
    scheduler = Scheduler()
    # job = Job(task_1, dependencies=[Job(task_3)], tries=6)
    # scheduler.add_task(job)

    # job = Job(task_2)
    # scheduler.add_task(job)
    job = Job(task_1, tries=0)
    scheduler.add_task(job)
    # job = Job(source)
    # scheduler.add_task(job)
    # job = Job(api_exact_time, tries=2)
    # scheduler.add_task(job)
    scheduler.run()


if __name__ == '__main__':
    main()
