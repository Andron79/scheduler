import logging
import types
from typing import Generator, Union
from queue import Queue

logger = logging.getLogger(__name__)


def double(x):
    yield x * x


def add(x, y):
    yield from double(x + y)


def main():
    result = yield add(1, 2)
    print(result)
    yield


class Task:
    task_id = 0

    def __init__(self, target: Generator):
        Task.task_id += 1
        self.tid = Task.task_id  # Task ID
        self.target = target  # Target coroutine
        self.sendval = None  # Value to send
        self.stack = []  # Call stack

    # Run a task until it hits the next yield statement
    def run(self):
        while True:
            try:
                result = self.target.send(self.sendval)

                if isinstance(result, types.GeneratorType):
                    self.stack.append(self.target)
                    self.sendval = None
                    self.target = result
                else:
                    if not self.stack:
                        return
                    self.sendval = result
                    self.target = self.stack.pop()

            except StopIteration:
                if not self.stack:
                    raise
                self.sendval = None
                self.target = self.stack.pop()


class Scheduler:
    def __init__(self):
        self.ready = Queue()
        self.task_map = {}

    def add_task(self, coroutine: Generator) -> int:
        new_task = Task(coroutine)
        self.task_map[new_task.tid] = new_task
        self.schedule(new_task)
        return new_task.tid

    def exit(self, task: Task):
        logger.info('Task %d terminated', task.tid)
        del self.task_map[task.tid]

    def schedule(self, task: Task):
        self.ready.put(task)

    def _run_once(self):
        task = self.ready.get()
        try:
            result = task.run()
        except StopIteration:
            self.exit(task)
            return
        self.schedule(task)

    def event_loop(self):
        while self.task_map:
            self._run_once()


if __name__ == '__main__':
    task = Task(main())
    task.run()
