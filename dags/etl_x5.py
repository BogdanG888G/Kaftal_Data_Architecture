from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

import logging
import requests
import datetime
import os
import shutil

TG_TOKEN = Variable.get('TELEGRAM_TOKEN')
TG_CHAT = Variable.get('TELEGRAM_CHAT_ID')

def telegram_alert(context):
    
    dag_id = context['ti'].dag_id
    task = context['ti'].task_id
    ds = context['ds']
    error = context['exception']

    meessage = f'Ошибка <b>{error}</b> в даге <b>{dag_id}</b>, в задаче <b>{task}</b>, дата запуска <b>{ds}</b>'

    requests.post(url = f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage',
                  data={'chat_id': TG_CHAT, 'text': meessage, 'parse_mode': 'HTML'})
    

def telegram_success():

    requests.post(url = f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage',
                  data={'chat_id': TG_CHAT, 'text': 'Паплайн по X5 успешно завершился ✅', 'parse_mode': 'HTML'})

@dag(
    dag_id = 'x5_etl',
    description = 'Автоматизация обработки данных для X5',
    catchup = False,
    start_date = datetime.datetime(year=2026, month = 4, day=1),
    schedule = '@daily',
    tags = ['X5', 'S3', 'Spark'],
    on_success_callback = telegram_success
)

def pipeline():

    wait_for_data = FileSensor(
        task_id = 'wait_for_data',
        fs_conn_id = 'folder_connect',
        filepath = '/opt/airflow/data/x5*',
        mode = 'reschedule',
        timeout = 60,
        on_failure_callback = telegram_alert,
    )


    @task(on_failure_callback = telegram_alert, on_success_callback = telegram_success, trigger_rule = TriggerRule.ALL_SUCCESS)
    def load_to_s3():
        
        data_dir = '/opt/airflow/data'
        archive_dir = '/opt/airflow/archive'

        s3 = S3Hook(aws_conn_id = 's3_connect')

        folder = os.listdir(data_dir)

        for file in folder:
            
            if 'x5' in file:

                s3.load_file(
                    filename = data_dir + '/' + file,
                    key = file,
                    bucket_name = 'raw'
                )

                logging.info(f'Успешно загружен файл {file}')

                shutil.move(src=data_dir + '/' + file, dst = archive_dir + '/' + file)

                logging.info(f'Архивировали {file} в archive')

        else:
            logging.info('Нет релевантных файлов')


    spark_processing = SparkSubmitOperator(
        task_id = 'spark_processing',
        conn_id = 'spark_connection',
        trigger_rule = TriggerRule.ALL_DONE,
        application = 'jobs/x5_spark_processing.py',
        on_failure_callback = telegram_alert,
        deploy_mode='client',
        conf={
        'spark.jars.packages': (
            'org.apache.hadoop:hadoop-aws:3.3.4,'
            'com.amazonaws:aws-java-sdk-bundle:1.12.262,'
            'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,'
            'org.apache.iceberg:iceberg-aws-bundle:1.5.0'
        ),
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.iceberg.type': 'rest',
        'spark.sql.catalog.iceberg.uri': 'http://iceberg-rest:8181',
        'spark.sql.catalog.iceberg.io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
        'spark.sql.catalog.iceberg.s3.endpoint': 'http://minio:9000',
        'spark.sql.catalog.iceberg.s3.access-key-id': 'minioadmin',
        'spark.sql.catalog.iceberg.s3.secret-access-key': 'minioadmin',
        'spark.sql.catalog.iceberg.s3.path-style-access': 'true',
        'spark.sql.catalog.iceberg.client.region': 'us-east-1',
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'minioadmin',
        'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    } 
    )

    wait_for_data >> load_to_s3() >> spark_processing

    spark_processing


pipeline()