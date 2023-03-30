import logging

from tasks import task_2, api_exact_time, task_1, job_factory
from job import Job
from scheduler import Scheduler

logger = logging.getLogger(__name__)


def main():
    scheduler = Scheduler()
    job = Job(task_1, tries=1)
    scheduler.add_task(job)

    job = Job(task_2, dependencies=[Job(task_1)], tries=2)
    scheduler.add_task(job)

    job = Job(task_1, max_working_time=2)
    scheduler.add_task(job)

    job = Job(task_1, start_at='2023-03-30 14:17', tries=1)
    scheduler.add_task(job)

    job = Job(task_2, tries=1)
    scheduler.add_task(job)

    job = Job(job_factory)
    scheduler.add_task(job)

    job = Job(api_exact_time, tries=2)
    scheduler.add_task(job)

    scheduler.run()


if __name__ == '__main__':
    main()
