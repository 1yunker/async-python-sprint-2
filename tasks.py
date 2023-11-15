import json
import os
import shutil

from external.client import YandexWeatherAPI
from job import Job
from scheduler import logging
from utils import get_url_by_city_name

logger = logging.getLogger()

TEMP_DIR = 'results'


def task_01_mkdir(temp_dir):
    os.makedirs(temp_dir, exist_ok=True)
    logger.info(f'Директория {temp_dir} успешно создана.')


def task_02_rmdir(temp_dir):
    shutil.rmtree(temp_dir)
    logger.info(f'Директория {temp_dir} успешно удалена.')


def task_03_get_weather_and_save(city_name, temp_dir=TEMP_DIR):
    logger.info(f'Запуск получения данных по городу {city_name}')
    try:
        url_with_data = get_url_by_city_name(city_name)
        resp = YandexWeatherAPI.get_forecasting(url_with_data)
        if resp:
            file_with_path = f'{temp_dir}/{city_name}_response.json'
            with open(file_with_path, 'w') as file:
                json.dump(resp, file, indent=4)
                logger.info(
                    f'Информация о городе {city_name} успешно записана.'
                )
    except Exception as error_msg:
        logger.error(
            f'При получении данных о городе {city_name} возникла ошибка: '
            f'{error_msg}.'
        )


def get_tasks():
    tasks = []
    # Эмулируем сбой вызова задачи (не передали обязательные параметры)
    task_00 = Job(
        name='CREATE_TEMP_DIR',
        target=task_01_mkdir,  # args=(TEMP_DIR,),
        max_working_time=None,
        tries=3,
    )
    task_01 = Job(
        name='CREATE_TEMP_DIR',
        target=task_01_mkdir, args=(TEMP_DIR,),
        tries=3,
    )
    task_02 = Job(
        name='WEATHER_MOSCOW_TO_JSON',
        target=task_03_get_weather_and_save,
        args=('MOSCOW', TEMP_DIR,),
        max_working_time=1,
        dependencies=[task_01],
    )
    task_03 = Job(
        name='WEATHER_GIZA_TO_JSON',
        target=task_03_get_weather_and_save,
        args=('GIZA', TEMP_DIR),
        max_working_time=0.25,
        dependencies=[task_01],
        tries=3,
    )
    task_05 = Job(
        name='WEATHER_LONDON_TO_JSON',
        target=task_03_get_weather_and_save,
        args=('LONDON', TEMP_DIR),
        max_working_time=1,
        dependencies=[task_01],
        tries=3,
    )
    task_04 = Job(
        name='DELETE_TEMP_DIR',
        target=task_02_rmdir, args=(TEMP_DIR,),
        dependencies=[task_01, task_02, task_03, task_05],
        tries=3,
    )

    tasks.append(task_00)
    tasks.append(task_03)
    tasks.append(task_04)
    tasks.append(task_01)
    tasks.append(task_02)
    tasks.append(task_05)

    # tasks.append(task_00)
    # tasks.append(task_01)
    # from utils import CITIES
    # for city_name in CITIES.keys():
    #     tasks.append(
    #         Job(
    #             name=f'WEATHER_{city_name}_TO_JSON',
    #             target=task_03_get_weather_and_save,
    #             args=(city_name, TEMP_DIR),
    #             max_working_time=0.25,
    #             tries=3,
    #             dependencies=[task_01],
    #         )
    #     )

    return tasks
