import logging

from tasks import task_1, task_2, task_3
from job import Job
from scheduler import Scheduler

logger = logging.getLogger(__name__)


def main():
    scheduler = Scheduler()
    # job = Job(task_2, dependencies=[Job(task_1)], tries=2)
    # scheduler.schedule(job)
    # job = Job(task_3, start_at='2023-01-18 14:32')
    # scheduler.schedule(job)
    job = Job(task_1, tries=3)
    scheduler.schedule(job)
    # job = Job(task_3)
    # scheduler.schedule(job)
    scheduler.run()


if __name__ == '__main__':
    main()
