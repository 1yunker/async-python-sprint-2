import json
import logging
from queue import Queue

from job import Job
from json_schema import TaskJsonSchema

# from time import sleep


format = '%(asctime)s [%(levelname)s]: %(message)s'
logging.basicConfig(
    filename='sheduler.log',
    encoding='utf-8',
    format=format,
    level=logging.INFO
)
logger = logging.getLogger()


class Scheduler:
    """
    Планировщик задач.
    ------------------
    Принимает и запускает задачи в соответствии с ограничениями:
    pool_size - предельное количество одновремено выполняемых задач
    dependencies - задача не выполняется, пока не выполнились зависимые задачи
    """

    BACKUP_FILE = 'shedule.json'

    def __init__(self, pool_size: int = 10) -> None:
        self._pool_size: int = pool_size
        self.tasks: Queue[Job] = Queue()
        self.is_run: bool = False

    def add_to_schedule(self, task: Job) -> None:
        """
        Добавить задачу в список на исполнение.
        """

        if task.dependencies:
            logger.info('>>> Добавление в планировщик запвисимых задач:')
            for depend_task in task.dependencies:
                if depend_task not in list(self.tasks.queue):
                    self.add_to_schedule(depend_task)
            logger.info('>>> Все зависимые задачи успешно добавлены.')

        self.tasks.put(task)
        logger.info(f'Задача {task.name} успешно добавлена в планировщик.')

    def run(self) -> None:
        """
        Запустить исполнение задач из списка.
        """
        logger.info('Планировщик начал работу.')
        self.is_run = True

        while not self.tasks.empty():

            # Проверям количество одновремено выполняемых задач
            # if

            job = self.tasks.get()
            try:
                job.run()
            except StopIteration:
                continue

        logger.info('Планировщик завершил работу.')

    def restart(self) -> None:
        """
        Восстановить работу планировщика после штатного завершения.
        """
        if self.is_run:
            print('Планировщик уже запущен!')
        else:
            self.restore()
            self.run()

    def stop(self) -> None:
        """
        Остановить работу планировщика.
        """
        self.backup(self.BACKUP_FILE)
        self.is_run = False

    def backup(self, backup_file):
        """
        Записать задачи из планировщика в JSON-файл.
        """
        tasks_json = []
        for task in list(self.tasks.queue):
            task_dict = task.__dict__
            task_dict['dependencies'] = [
                x.__dict__ for x in task_dict['dependencies']
            ]
            tasks_json.append(
                TaskJsonSchema.model_validate(task.__dict__).model_dump_json()
            )
        with open(backup_file, 'w') as f:
            json.dump(tasks_json, f)

    def restore(self):
        """
        Восстановить задачи планировщика из JSON-файла.
        """
        pass
