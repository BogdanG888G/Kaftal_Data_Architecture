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

LOCAL_DATA_DIR = '/opt/airflow/data'
LOCAL_ARCHIVE_DIR = '/opt/airflow/archive'
RAW_BUCKET = 'raw'
FILE_PREFIX = 'samokat'


def send_telegram(text: str):
    token = Variable.get('TELEGRAM_TOKEN')
    chat_id = Variable.get('TELEGRAM_CHAT_ID')
    requests.post(
        url=f'https://api.telegram.org/bot{token}/sendMessage',
        data={'chat_id': chat_id, 'text': text, 'parse_mode': 'HTML'},
        timeout=15
    )


def telegram_alert(context):
    dag_id = context['ti'].dag_id
    task_id = context['ti'].task_id
    ds = context['ds']
    error = context.get('exception')
    message = (
        f'Ошибка <b>{error}</b>\n'
        f'Даг: <b>{dag_id}</b>\n'
        f'Задача: <b>{task_id}</b>\n'
        f'Дата запуска: <b>{ds}</b>'
    )
    send_telegram(message)


def telegram_success(context):
    send_telegram('Пайплайн по Самокату успешно завершился ✅')


@dag(
    dag_id='samokat_etl',
    description='Автоматизация обработки данных Самоката',
    catchup=False,
    start_date=datetime.datetime(year=2026, month=4, day=1),
    schedule='@daily',
    tags=['Samokat', 'S3', 'Spark', 'Konditerka'],
    on_success_callback=telegram_success
)
def pipeline():

    wait_for_data = FileSensor(
        task_id='wait_for_data',
        fs_conn_id='folder_connect',
        filepath=f'{LOCAL_DATA_DIR}/{FILE_PREFIX}*.csv',
        mode='reschedule',
        poke_interval=60,
        timeout=60 * 60 * 12,
        on_failure_callback=telegram_alert,
    )

    @task(on_failure_callback=telegram_alert, trigger_rule=TriggerRule.ALL_SUCCESS)
    def load_to_raw_s3():
        os.makedirs(LOCAL_ARCHIVE_DIR, exist_ok=True)
        s3 = S3Hook(aws_conn_id='s3_connect')
        files = sorted(os.listdir(LOCAL_DATA_DIR))
        loaded_any = False

        for file_name in files:
            if not file_name.startswith(FILE_PREFIX) or not file_name.lower().endswith('.csv'):
                continue

            loaded_any = True
            local_path = os.path.join(LOCAL_DATA_DIR, file_name)
            archive_path = os.path.join(LOCAL_ARCHIVE_DIR, file_name)

            try:
                if s3.check_for_key(key=file_name, bucket_name=RAW_BUCKET):
                    logging.warning(f'⊘ Файл {file_name} уже есть в bucket {RAW_BUCKET}, пропускаем')
                else:
                    s3.load_file(
                        filename=local_path,
                        key=file_name,
                        bucket_name=RAW_BUCKET,
                        replace=False
                    )
                    logging.info(f'✓ Загружен файл {file_name} в bucket {RAW_BUCKET}')

                shutil.move(local_path, archive_path)
                logging.info(f'📦 Архивировали {file_name}')
            except Exception as e:
                logging.error(f'❌ Ошибка при обработке файла {file_name}: {e}')
                raise

        if not loaded_any:
            raise FileNotFoundError(f'Файлы {FILE_PREFIX}*.csv не найдены в {LOCAL_DATA_DIR}')

    spark_processing = SparkSubmitOperator(
        task_id='spark_processing',
        conn_id='spark_connection',
        trigger_rule=TriggerRule.ALL_DONE,
        application='jobs/samokat_spark_processing.py',
        on_failure_callback=telegram_alert,
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
            'spark.sql.catalog.iceberg.warehouse': 's3a://warehouse/',
            'spark.sql.catalog.iceberg.s3.endpoint': 'http://minio:9000',
            'spark.sql.catalog.iceberg.s3.access-key-id': 'minioadmin',
            'spark.sql.catalog.iceberg.s3.secret-access-key': 'minioadmin',
            'spark.sql.catalog.iceberg.s3.path-style-access': 'true',
            'spark.sql.catalog.iceberg.s3.ssl-enabled': 'false',
            'spark.sql.catalog.iceberg.client.region': 'us-east-1',
            'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
            'spark.hadoop.fs.s3a.access.key': 'minioadmin',
            'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        }
    )

    wait_for_data >> load_to_raw_s3() >> spark_processing


pipeline()