from collections import deque


def countdown(n: int = 10):
    while n > 0:
        print('T-minus', n)
        yield
        n -= 1
    print('Blastoff!')


def countup(n: int = 5):
    x = 0
    while x < n:
        print('IIIIIIIIIIIIIIIIIIIIIIIII', x)
        yield
        x += 1


def countdown1(n: int = 10):
    while n > 0:
        print('T-qqqqqqq', n)
        yield
        n -= 1
    print('OOOOOOOOO!')


class TaskScheduler:

    def __init__(self):
        self._task_queue = deque()

    def new_task(self, task):
        """
         Допускает новую запущенную задачу в планировщик
         """
        self._task_queue.append(task)

    def run(self):
        """
        Работает, пока не останется задач
        """
        while self._task_queue:
            task = self._task_queue.popleft()
            try:
                # Работает до следующей инструкции yield
                next(task)
                self._task_queue.append(task)
            except StopIteration:
                # Генератор более не выполняется
                pass


def main():
    sched = TaskScheduler()
    sched.new_task(countdown(10))
    # sched.new_task(countdown(5))
    sched.new_task(countup(15))
    sched.run()


if __name__ == '__main__':
    main()
