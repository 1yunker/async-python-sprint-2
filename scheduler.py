import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from datetime import datetime

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
            task.status = Status.READY
            self.tasks.append(task)
            logger.info(f'Задача {task.name} успешно добавлена в планировщик.')

    def is_ready_to_start(self, task: Job) -> bool:
        """
        Проверка задачи на успешное выполнение всех зависимых задач.
        """
        if task.dependencies:
            for depend_task in task.dependencies:
                if depend_task.status != Status.DONE:
                    return False
        return True

    def run(self, stop_after: float = 0.0) -> None:
        """
        Запустить исполнение задач из списка.
        Args:
            stop_after: время, после которого новые задачи не добавляются
                и планировщик штатно завершает свою работу.

        """
        logger.info('Планировщик начал работу.')
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=self._pool_size) as pool:

            while bool(self.tasks):
                # Проверка на срабатывание остановки планировщика
                time_delta = time.time() - start_time
                if stop_after > 0 and time_delta > stop_after:
                    self.stop()
                    break

                # Берем первую задачу из очереди
                job = self.tasks.pop(0)
                # Если зависимые задачи не выполнены - возвращаем
                # задачу в конец очереди
                if (not self.is_ready_to_start(job)
                   and job.status not in (Status.CANCELED, Status.ERROR)):
                    job.status = Status.WAIT
                    self.tasks.append(job)
                    logger.info(
                        f'Задача {job.name} отложена.'
                    )

                future = pool.submit(job.run)
                try:
                    future.result(timeout=job.max_working_time)
                    job.status = Status.DONE
                except TimeoutError:
                    job.status = Status.CANCELED
                    logger.error(
                        f'Задача {job.name} первана по таймауту.'
                    )
                    continue
                except Exception as err:
                    job.status = Status.ERROR
                    logger.error(
                        f'Задача {job.name} завершилась с ошибкой: {err}'
                    )
                    continue

        time_delta = time.time() - start_time
        logger.info(
            f'Планировщик завершил работу. Время выполнения: {time_delta}'
        )
        logger.info('================================')

    def restart(self) -> None:
        """
        Восстановить работу планировщика после штатного завершения.
        """
        self.restore(self.BACKUP_FILE)
        self.run()

    def stop(self) -> None:
        """
        Остановить работу планировщика.
        """
        logger.info('Работа планировщика остановлена.')
        self.backup(self.BACKUP_FILE)

    def get_task_as_dict(self, task: Job) -> dict:
        task_dict = dict(task.__dict__)
        task_dict['target'] = task.target.__name__
        task_dict['start_at'] = str(task.start_at)
        task_dict['dependencies'] = None
        task_dict['status'] = str(task.status)
        return task_dict

    def backup(self, backup_file: str) -> None:
        """
        Записать задачи из планировщика в JSON-файл.
        """
        tasks_json = []
        for task in self.tasks:
            # Добавляем все зависимые задачи перед основной
            if task.dependencies:
                for depend_task in task.dependencies:
                    depend_task_dict = self.get_task_as_dict(depend_task)
                    # Проверям присутствует ли уже зависимая задача в очереди
                    if depend_task_dict not in tasks_json:
                        tasks_json.append(depend_task_dict)

            task_dict = self.get_task_as_dict(task)
            tasks_json.append(task_dict)

        with open(backup_file, 'w') as f:
            json.dump(tasks_json, f, indent=4)
        logger.info(
            f'Задачи планировщика успешно сохранены в {self.BACKUP_FILE}.'
        )

    def restore(self, backup_file: str) -> None:
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
                status=Status.READY
            )
            self.add_to_schedule(task=job)
        logger.info(
            f'Задачи планировщика успешно восстановлены из {self.BACKUP_FILE}.'
        )
