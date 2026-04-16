from airflow.sdk import task, dag
from airflow.models import Variable
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule

import datetime
import requests
import os
import shutil
import logging

TG_TOKEN = Variable.get('TELEGRAM_TOKEN')
TG_CHAT_ID = Variable.get('TELEGRAM_CHAT_ID')


def telegram_alert(context):

    dag_id = context['ti'].dag_id
    task = context['ti'].task_id
    ds = context['ds']
    error = context['exception']
    
    message = f'Ошибка в даге <b>{dag_id}</b>, в задаче <b>{task}</b>, ошибка <b>{error}</b>, дата <b>{ds}</b>'

    requests.post(url = f'https://api.telegram.org/bot{TG_TOKEN}/Sendmessage', 
                  data = {'chat_id': TG_CHAT_ID, 'text': message, 'parse_mode': 'HTML'})
    


@dag(
    dag_id = 'etl_diksi',
    start_date = datetime.datetime(year=2026, month=2, day=1),
    schedule = '@daily',
    catchup = False,
    description = 'Автоматизация для Дикси',
    tags = ['Spark', 'S3', 'diksi']
)


def pipeline():
    
    wait_for_files = FileSensor(
        task_id = 'wait_for_files',
        fs_conn_id = 'folder_connect',
        filepath = '/opt/airflow/data/diksi*',
        mode =  'poke',
        timeout = 60,
        on_failure_callback = telegram_alert
    )


    @task(on_failure_callback = telegram_alert)

    def load_to_s3_raw():
                
        input_folder = '/opt/airflow/data/'
        output_folder = '/opt/airflow/archive/'

        s3 = S3Hook(aws_conn_id = 's3_connect')

        for file in os.listdir(input_folder):

            if file.startswith('diksi'):

                file_name = input_folder + file

                s3.load_file(filename = file_name, key = file, bucket_name = 'raw')

                logging.info(f'Загрузили файл {file} в сырой слой')

                shutil.move(src=file_name, dst=output_folder + file)

                logging.info(f'Архивировали файл {file}')
        

    wait_for_files >> load_to_s3_raw()

pipeline()