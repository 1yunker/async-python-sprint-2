from functools import wraps


def coroutine(f):
    @wraps(f)
    def wrap(*args, **kwargs):
        gen = f(*args, **kwargs)
        gen.send(None)
        return gen
    return wrap


class Scheduler:
    """
    Планировщик задач.
    Принимает и запускает задачи в соответствии с расписанием.
    """

    def __init__(self, pool_size=10):
        pass

    def schedule(self, task):
        """
        Добавить задачу в список.
        """
        pass

    def run(self):
        """
        Запустить исполнение задач из списка.
        """
        pass

    def restart(self):
        """
        Восстановить работу планировщика после штатного завершения.
        """
        pass

    def stop(self):
        """
        Остановить работу планировщика.
        """
        pass

    def backup(self):
        """
        Записать задачи из планировщика в JSON-файл.
        """
        pass

    def restore(self):
        """
        Восстановить задачи планировщика из JSON-файла.
        """
        pass
