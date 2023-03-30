import logging

from job import Job
from settings import MAX_TASKS_COUNT
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
    assert scheduler.expired_task(job), f'Просроченный таск {job.name} попал в очередь!'
    logger.info(f'f"Задача {job.name} просрочена и исполняться не будет"')


def test_schedule_task_dependencies(scheduler):
    dependencies = [Job(task_2)]
    job = Job(task_1, dependencies=dependencies)
    scheduler.add_task(job)
    assert len(scheduler._queue) == len(dependencies) + 1, f'Не все таски попали в очередь'
    logger.info(f'Таск {job.name} и его зависимости попали в очередь')


def test_schedule_deferred_task(scheduler):
    job = Job(task_1, start_at='2025-03-29 10:13')
    scheduler.add_task(job)
    assert scheduler.deferred_task(job), f'Отложенный таск {job.name} попал в очередь!'
    logger.info(f'Отложенный таск {job.name} не попал в очередь!')


def test_schedule_queue_is_full(scheduler):
    for task in range(MAX_TASKS_COUNT + 1):
        job = Job(task_1)
        scheduler.add_task(job)
    assert scheduler.queue_is_full(), f'Очередь не переполнена!'
    logger.info(f'Очередь переполнена!')
