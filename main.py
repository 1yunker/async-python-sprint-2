from time import sleep

from scheduler import Scheduler
from tasks import get_tasks

if __name__ == '__main__':

    # Инициализируем планировщик
    scheduler = Scheduler(pool_size=5)

    # Грузим задачи в планировщик
    tasks = get_tasks()
    for task in tasks:
        scheduler.add_to_schedule(task=task)

    # Запускаем планировщик
    scheduler.run()
    # sleep(1)
    # scheduler.stop()

    # Перезапускаем планировщик
    # scheduler.restart()
    # sleep(1)
    # scheduler.stop()
