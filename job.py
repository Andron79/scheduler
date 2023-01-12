from datetime import datetime
from typing import Optional, List, Callable


class Job:
    def __init__(
            self,
            task: Callable,
            start_at: Optional[datetime] = None,
            max_working_time: Optional[str] = None,
            tries: int = 0,
            dependencies:  Optional[List[str]] = None
    ):
        self.task = task()
        self.tries = tries
        self.start_at = start_at
        self.max_working_time = max_working_time
        self.dependencies = dependencies

    def run(self):
        result = next(self.task)
        print(result)
        return result

    def pause(self):
        pass

    def stop(self):
        pass
