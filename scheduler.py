from datetime import datetime
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError

import tasks as tsk
from job import Job, Status

format = '%(asctime)s [%(levelname)s]: %(message)s'
logging.basicConfig(
    filename='sheduler.log',
    encoding='utf-8',
    format=format,
    level=logging.INFO
)

logger = logging.getLogger()


def singleton(class_):
    instances = {}

    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]
    return getinstance


@singleton
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
        self.tasks: list[Job] = []
        self.is_run: bool = False

    def add_to_schedule(self, task: Job) -> None:
        """
        Добавить задачу в список на исполнение.
        """
        if task.dependencies:
            for depend_task in task.dependencies:
                if depend_task not in self.tasks:
                    logger.info(
                        '>>> Добавление в планировщик зависимых задач:'
                    )
                    self.add_to_schedule(depend_task)
            logger.info('>>> Все зависимые задачи успешно добавлены.')

        if task not in self.tasks:
            self.tasks.append(task)
            logger.info(f'Задача {task.name} успешно добавлена в планировщик.')

    def run(self, stop_after: float = 0.0) -> None:
        """
        Запустить исполнение задач из списка.
        Args:
            stop_after: время, после которого новые задачи не добавляются
                и планировщик штатно завершает свою работу.

        """
        logger.info('Планировщик начал работу.')
        self.is_run = True
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=self._pool_size) as pool:

            while len(self.tasks) > 0:
                # Проверка на срабатывание остановки планировщика
                time_delta = time.time() - start_time
                if stop_after > 0 and time_delta > stop_after:
                    self.stop()
                    break

                # Берем задачу из очереди
                job = self.tasks.pop(0)
                future = pool.submit(job.run)
                try:
                    future.result(timeout=job.max_working_time)
                except TimeoutError:
                    logger.error(
                        f'Задача {job.name} первана по таймауту.'
                    )
                    continue
                except Exception as err:
                    logger.error(
                        f'Задача {job.name} завершилась с ошибкой: {err}'
                    )
                    continue

        time_delta = time.time() - start_time
        logger.info(
            f'Планировщик завершил работу. Время выполнения: {time_delta}'
        )

    def restart(self) -> None:
        """
        Восстановить работу планировщика после штатного завершения.
        """
        if self.is_run:
            print('Планировщик уже запущен!')
        else:
            self.restore(self.BACKUP_FILE)
            self.run()

    def stop(self) -> None:
        """
        Остановить работу планировщика.
        """
        self.backup(self.BACKUP_FILE)
        self.is_run = False

    def add_task_dependencies(self, task: Job) -> list:
        tasks_json = []
        if task.dependencies:
            for task in task.dependencies:
                task_dict = task.__dict__
                task_dict['target'] = task.target.__name__
                task_dict['start_at'] = str(task.start_at)
                task_dict['dependencies'] = None
                task_dict['status'] = Status.READY
                tasks_json.append(task_dict)
        return tasks_json

    def backup(self, backup_file):
        """
        Записать задачи из планировщика в JSON-файл.
        """
        tasks_json = []
        for task in self.tasks:
            # Добавляем все зависимые задачи перед основной
            if task.dependencies:
                for depend_task in task.dependencies:
                    depend_task_dict = dict(depend_task.__dict__)
                    depend_task_dict['target'] = depend_task.target.__name__
                    depend_task_dict['start_at'] = str(depend_task.start_at)
                    depend_task_dict['dependencies'] = None
                    depend_task_dict['status'] = str(task.status)

                    if depend_task_dict not in tasks_json:
                        tasks_json.append(depend_task_dict)
            # tasks_json.extend(self.add_task_dependencies(task))

            task_dict = dict(task.__dict__)
            task_dict['target'] = task.target.__name__
            task_dict['start_at'] = str(task.start_at)
            task_dict['dependencies'] = None
            task_dict['status'] = str(task.status)
            tasks_json.append(task_dict)

        with open(backup_file, 'w') as f:
            json.dump(tasks_json, f, indent=4)
        logger.info('Работа планировщика остановлена. '
                    'Данные по невыполненным задачам успешно сохранены.')

    def restore(self, backup_file):
        """
        Восстановить задачи планировщика из JSON-файла.
        """
        with open(backup_file, 'r') as f:
            tasks = json.load(f)

        for task in tasks:
            job = Job(
                name=task.get('name'),
                target=getattr(tsk, task['target']),
                args=task.get('args'),
                kwargs=task.get('kwargs'),
                start_at=datetime.strptime(
                    task.get('start_at'), '%Y-%m-%d %H:%M:%S.%f'
                ),
                max_working_time=task.get('max_working_time'),
                tries=task.get('tries'),
            )
            self.add_to_schedule(task=job)
