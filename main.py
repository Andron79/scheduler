import logging

from tasks import task_2, task_3, task_1
from job import Job
from scheduler import Scheduler

logger = logging.getLogger(__name__)


def main():
    scheduler = Scheduler()

    job = Job(task_2)
    # scheduler.add_task(job)
    # job = Job(task_3, dependencies=[Job(task_2)])
    scheduler.add_task(job)
    scheduler.run()


if __name__ == '__main__':
    main()
