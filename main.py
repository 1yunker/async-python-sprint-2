from scheduler import Scheduler
from tasks import get_tasks

if __name__ == '__main__':

    # Инициализируем планировщик c ограничением в 5 потоков
    scheduler = Scheduler(pool_size=5)

    # Грузим задачи и запускаем планировщик
    tasks = get_tasks()
    for task in tasks:
        scheduler.add_to_schedule(task=task)
    scheduler.run()
    # scheduler.run(stop_after=2.0)

    # Перезапускаем планировщик
    # scheduler.restart()
