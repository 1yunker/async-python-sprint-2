import json
import os
import shutil

from external.client import YandexWeatherAPI
from job import Job
from scheduler import logging
from utils import CITIES, get_url_by_city_name

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


def task_003_get_url_with_data(city_name):
    try:
        url_with_data = get_url_by_city_name(city_name)
        logger.info(f'URL с данными о городе {city_name} успешно получен.')
        return url_with_data
    except Exception as error_msg:
        logger.error(
            f'При получении URL c данныvb о городе {city_name} возникла '
            f'ошибка: {error_msg}.'
        )
        return None


def task_002_get_forecasting(url_with_data):
    try:
        resp = YandexWeatherAPI.get_forecasting(url_with_data)
        logger.info(f'Данные о погоде по {url_with_data} успешно получены.')
        return resp
    except Exception as error_msg:
        logger.error(
            f'При получении данных о погоде по {url_with_data} возникла '
            f'ошибка: {error_msg}.'
        )
        return None


def task_001_save_to_josn(resp, temp_dir, city_name):
    try:
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
    task_00 = Job(
        name='CREATE_TEMP_DIR',
        target=task_01_mkdir,  # args=(TEMP_DIR,),
        max_working_time=None,
        tries=3,
    )
    task_01 = Job(
        name='CREATE_TEMP_DIR',
        target=task_01_mkdir, args=(TEMP_DIR,),
    )
    task_02 = Job(
        name='WEATHER_MOSCOW_TO_JSON',
        target=task_03_get_weather_and_save,
        args=('MOSCOW', TEMP_DIR,),
        dependencies=[task_01],
    )
    task_03 = Job(
        name='WEATHER_GIZA_TO_JSON',
        target=task_03_get_weather_and_save,
        args=('GIZA', TEMP_DIR),
        dependencies=[task_01],
    )
    task_04 = Job(
        name='DELETE_TEMP_DIR',
        target=task_02_rmdir, args=(TEMP_DIR,),
        dependencies=[task_01, task_02, task_03]
    )



    # tasks.append(task_00)
    # tasks.append(task_01)
    # for city_name in CITIES.keys():
    #     tasks.append(
    #         Job(
    #             name=f'WEATHER_{city_name}_TO_JSON',
    #             target=task_03_get_weather_and_save,
    #             args=(city_name, TEMP_DIR),
    #             max_working_time=0.25,
    #             tries=3,
    #         )
    #     )

    tasks.append(task_00)
    tasks.append(task_03)
    tasks.append(task_04)
    tasks.append(task_01)
    tasks.append(task_02)
    return tasks
