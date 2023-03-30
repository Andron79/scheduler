import logging

from tasks import task_2, task_3, stage_1, api_exact_time, task_1
from job import Job
from scheduler import Scheduler

logger = logging.getLogger(__name__)


def main():
    scheduler = Scheduler()
    # job = Job(task_1, max_working_time=7)
    # scheduler.add_task(job)
    #
    # job = Job(task_1, start_at='2023-03-17 18:18')
    # scheduler.add_task(job)
    #
    # job = Job(task_2, tries=1)
    # scheduler.add_task(job)
    # job = Job(task_1, tries=1)
    # scheduler.add_task(job)
    # job = Job(source)
    # scheduler.add_task(job)
    # job = Job(api_exact_time, tries=2)
    # scheduler.add_task(job)
    #
    # job = Job(task_2, dependencies=[Job(task_1)], tries=2)
    # scheduler.add_task(job)
    # job = Job(task_1, max_working_time=7)
    # scheduler.add_task(job)
    #
    # job = Job(task_1, start_at='2023-03-29 10:13', tries=1)
    # scheduler.add_task(job)
    #
    # job = Job(task_2, tries=1)
    # scheduler.add_task(job)
    # job = Job(task_1, tries=1)
    # scheduler.add_task(job)
    job = Job(stage_1)
    scheduler.add_task(job)
    # job = Job(api_exact_time, tries=2)
    # scheduler.add_task(job)

    # job = Job(task_3, dependencies=[Job(task_1)], tries=1)
    # scheduler.add_task(job)
    scheduler.run()


if __name__ == '__main__':
    main()
