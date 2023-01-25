import logging

from tasks import task_2, task_3
from job import Job
from scheduler import Scheduler

logger = logging.getLogger(__name__)


def main():
    scheduler = Scheduler()
    # job = Job(task_1, dependencies=[Job(task_3)], tries=6)
    # scheduler.add_task(job)

    job = Job(task_2)
    scheduler.add_task(job)
    job = Job(task_3, max_working_time=11)
    scheduler.add_task(job)
    # job = Job(task_3, tries=2)
    # scheduler.add_task(job)
    scheduler.run()


if __name__ == '__main__':
    main()
