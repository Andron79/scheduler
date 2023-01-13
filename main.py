import logging
from datetime import datetime

from example import countdown, countup, countdown1
from job import Job
from scheduler import Scheduler
from tasks import print_hellow, print_hellow1, make_and_delete_dirs, write_lines, hello

logger = logging.getLogger(__name__)


def main():
    scheduler = Scheduler()
    job = Job(countdown, start_at='2023-01-13 17:47')  #, start_at=datetime(year=2023, month=1, day=13, hour=17, minute=28))
    scheduler.schedule(job)
    # job = Job(countdown1)
    # scheduler.schedule(job)
    # job = Job(countup)
    # scheduler.schedule(job)
    scheduler.run()


if __name__ == '__main__':
    main()
