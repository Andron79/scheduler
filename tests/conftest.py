import pytest

from job import Job
from scheduler import Scheduler


def task():
    pass


@pytest.fixture()
def job() -> Job:
    return Job(
        task=task,
        start_at=None,
        max_working_time=None,
        tries=0,
        dependencies=[]
    )


@pytest.fixture()
def scheduler(job):
    yield Scheduler()
