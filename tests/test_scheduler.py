import logging

from job import Job
from tasks import task_1, task_2

logger = logging.getLogger(__name__)


def test_schedule_task_in_queue(scheduler):
    job = Job(task_1)
    scheduler.add_task(job)
    assert job in scheduler._queue, f'Таск {job.name} не в очереди'
    logger.info(f'Таск {job.name} в очереди задач')


def test_schedule_expired_task(scheduler):
    job = Job(task_1, start_at='2022-03-29 10:13')
    scheduler.add_task(job)
    assert job not in scheduler._queue, f'Просроченный таск {job.name} попал в очередь!'
    logger.info(f'Просроченный таск {job.name} не попал в очередь!')


def test_schedule_task_dependencies(scheduler):
    dependencies = [Job(task_2)]
    job = Job(task_1, dependencies=dependencies)
    scheduler.add_task(job)
    assert len(scheduler._queue) == len(dependencies) + 1, f'Не все таски попали в очередь'
    logger.info(f'Таск {job.name} и его зависимости попали в очередь')


def test_schedule_deferred_task(scheduler):
    job = Job(task_1, start_at='2025-03-29 10:13')
    scheduler.add_task(job)
    assert job not in scheduler._queue, f'Отложенный таск {job.name} попал в очередь!'
    logger.info(f'Отложенный таск {job.name} не попал в очередь!')
