from datetime import datetime
from enum import Enum
from typing import Callable
from functools import wraps

from scheduler import logging
logger = logging.getLogger()


def coroutine(f):
    @wraps(f)
    def wrap(*args, **kwargs):
        gen = f(*args, **kwargs)
        gen.send(None)
        return gen
    return wrap


class Status(Enum):
    """
    Статус задачи.
    --------------
    READY - задача готова к исполнению
    EXEC - задача выполняется
    WAIT - задача ожидает выполнение другой задачи
    DONE - задача успешно исполнена
    ERROR - при выполнении задачи возникла ошибка
    """
    READY = 0
    EXEC = 1
    WAIT = 2
    DONE = 3
    ERROR = 4


class Job:
    """
    Задача для планировщика.
    """

    def __init__(
            self,
            name,
            target: Callable,
            args=None,
            kwargs=None,
            start_at=datetime.now(),
            max_working_time=-1,
            tries: int = 0,
            dependencies=[],
            status=Status.READY
    ):
        self.name = name
        self._args = args or ()
        self._kwargs = kwargs or {}
        self._target = target
        self.start_at = start_at
        self.max_working_time = max_working_time
        self.tries = tries
        self.dependencies = dependencies
        self.status = status

    @coroutine
    def run(self) -> None:
        """
        Запустить задачу.
        """
        # start_time = time
        self.status = Status.EXEC
        while task := (yield):
            try:
                return self._target(*self._args, **self._kwargs)
            except Exception as err:
                logger.error(
                    f'Задача {self._target} завершилась с ошибкой: {err}'
                )

                while task.tries > 0:
                    task.tries -= 1
                    logger.warning(f'Задача {task.name} была перезапущена.')
                    try:
                        self._target(*self._args, **self._kwargs)
                        logger.info(f'Задача {task.name} успешно завершена.')
                    except Exception as err:
                        logger.error(
                            f'Задача {task.name} завершилась с ошибкой: {err}'
                        )
                return None

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
