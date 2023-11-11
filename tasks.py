import os
import shutil

from job import Job

TEMP_DIR = 'results'


def task_01_mkdir(temp_dir):
    yield os.makedirs(temp_dir, exist_ok=True)


def task_02_rmdir(temp_dir):
    yield shutil.rmtree(temp_dir)


def get_tasks():
    tasks = []
    task_01 = Job(
        name='CREATE_TEMP_DIR',
        target=task_01_mkdir, args=(TEMP_DIR,)
    )
    task_02 = Job(
        name='DELETE_TEMP_DIR',
        target=task_02_rmdir, args=(TEMP_DIR,),
        dependencies=[task_01]
    )
    tasks.append(task_02)
    return tasks
