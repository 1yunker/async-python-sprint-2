import time
from datetime import datetime
from enum import Enum
from functools import wraps
from typing import Callable

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
            max_working_time=None,
            tries: int = 0,
            dependencies=[],
            status=Status.READY
    ):
        self.name = name
        self.args = args or ()
        self.kwargs = kwargs or {}
        self.target = target
        self.start_at = start_at
        self.max_working_time = max_working_time
        self.tries = tries
        self.dependencies = dependencies
        self.status = status

    def __repr__(self):
        return f'{self.target}'

    @coroutine
    def run(self) -> None:
        """
        Запустить задачу.
        """
        start_time = time.time()
        self.status = Status.EXEC
        try:
            yield self.target(*self.args, **self.kwargs)
            time_delta = time.time() - start_time
            logger.info(
                f'Здача {self.target} выполнена. '
                f'Время выполнения: {time_delta}'
            )
        except Exception as err:
            logger.error(
                f'Задача {self.target} завершилась с ошибкой: {err}'
            )
            while self.tries > 0:
                self.tries -= 1
                logger.warning(f'Задача {self.name} была перезапущена.')
                try:
                    self.run()
                    logger.info(f'Задача {self.name} успешно завершена.')
                except Exception as err:
                    logger.error(
                        f'Задача {self.name} завершилась с ошибкой: {err}'
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
