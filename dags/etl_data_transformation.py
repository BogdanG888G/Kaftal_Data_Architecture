from airflow.decorators import task, dag
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.standard.sensors.filesystem import FileSensor

import logging
import pandas as pd

from datetime import *
import os

default_args = {
    'owner': 'airflow',
    'retries': 3
}

@dag(
    dag_id = 'etl_transformation',
    description = 'Перевод файлов в хранилище S3',
    catchup = False,
    start_date = datetime(2026, 2, 1),
    schedule = '@weekly',
    tags = ['s3']
)


def pipeline():


    wait_for_data = FileSensor(
        task_id = 'wait_for_data', 
        fs_conn_id = 'folder_connect',
        filepath = '/opt/airflow/data/*.csv',
        mode = 'reschedule',
        poke_interval = 20,
        timeout = 60
        )


    @task
    def start():

        data_dir = '/opt/airflow/data'

        s3 = S3Hook(aws_conn_id = 's3_connect')
        
        folder = os.listdir(data_dir)

        for i in folder:
            
            parts = i.split('_')
            
            ans = parts[2][:-4] + '/' + parts[1] + '/' + parts[0]


            s3.load_file(
                filename = data_dir + '/' + i,
                key = ans,
                bucket_name = 'data',
                replace = True)
            
            logging.info(f'Загрузили файл {i}')

    wait_for_data >> start()

pipeline()

