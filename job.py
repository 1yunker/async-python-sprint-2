from functools import wraps
from queue import Queue
from tracemalloc import start
from typing import Any, Callable, Generator


class Job:
    """
    Задача для планировщика.
    """

    def __init__(
            self,
            target: Callable,
            args: tuple = None,
            kwargs: dict = None,
            start_at="",
            max_working_time=-1,
            tries=0,
            dependencies=[]
    ):
        self.__args = args or ()
        self.__kwargs = kwargs or {}
        self.__coroutine = target(*self.__args, **self.__kwargs)
        self.start_at = start_at
        self.max_working_time = max_working_time
        self.tries = tries
        self.dependencies = dependencies

    def run(self) -> None:
        """
        Запустить задачу.
        """
        self.__coroutine.send(None)

    def pause(self):
        """
        Поставить задачу на паузу.
        """
        pass

    def stop(self):
        """
        Остановить задачу.
        """
        pass
