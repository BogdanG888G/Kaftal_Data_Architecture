from airflow.decorators import task, dag
#from airflow.providers.

import logging
import pandas as pd

from datetime import *


default_args = {
    'owner': 'airflow',
    'retries': 3
}

@dag(
    dag_id = 'etl_transformation',
    description = 'Перевод файлов хранилище S3',
    catchup = False,
    start_date = datetime(2026, 2, 1),
    schedule = '@weekly',
    tags = ['s3']
)

def pipeline():

    @task
    def start():

        logging.info('Запуск программы')

    start()

pipeline()