from airflow.sdk import task, dag
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


import logging
import pandas as pd
import requests
from datetime import *
import os
import traceback


# === TELEGRAM CONFIG ===
TELEGRAM_TOKEN = "8421267289:AAGv29lCile1WtII7qV_zPCUyi-hz-8832I"
TELEGRAM_CHAT_ID = "433923554"


def send_telegram_alert(context):
    """Отправляет алерт с полной информацией об ошибке"""
    
    try:
        ti = context.get('task_instance')
        dag_id = context.get('dag').dag_id
        task_id = ti.task_id
        execution_date = context.get('dag_run').start_date
        exception = context.get('exception')
        
        # ===== ГЛАВНОЕ ИСПРАВЛЕНИЕ: получаем traceback правильно =====
        
        # Способ 1: Из самого exception объекта
        if exception is not None:
            error_message = str(exception)
            # Извлекаем полный traceback из exception
            tb_lines = traceback.format_exception(
                type(exception), 
                exception, 
                exception.__traceback__
            )
            error_trace = ''.join(tb_lines)
        else:
            # Способ 2: Если exception is None (часто у сенсоров по timeout)
            error_message = "Задача завершилась с ошибкой (exception=None, возможно timeout)"
            error_trace = "Traceback недоступен — проверьте логи в Airflow UI"
        
        # Способ 3: Читаем логи таска напрямую (самый надёжный)
        try:
            task_log = ti.log.handlers  # доступ к логам
            # Или получаем последние строки лога
            from airflow.utils.log.log_reader import TaskLogReader
            log_reader = TaskLogReader()
            log_content = log_reader.read_log_stream(
                ti=ti,
                try_number=ti.try_number
            )
            # Берём последние 500 символов лога
            full_log = ''.join([chunk for chunk in log_content])
            last_log_lines = full_log[-500:] if full_log else "Логи пустые"
        except Exception as log_err:
            last_log_lines = f"Не удалось прочитать логи: {log_err}"
        
        # Ссылка на логи (исправленный формат URL)
        log_url = (
            f"http://localhost:8080/dags/{dag_id}/grid"
            f"?task_id={task_id}"
        )
        
        # Формируем сообщение
        message = f"""
⚠️ <b>ОШИБКА В DAG!</b>

<b>DAG:</b> <code>{dag_id}</code>
<b>Задача:</b> <code>{task_id}</code>
<b>Попытка:</b> <code>{ti.try_number} из {ti.max_tries + 1}</code>
<b>Время:</b> <code>{execution_date}</code>
<b>Состояние:</b> <code>{ti.state}</code>

<b>Ошибка:</b>
<code>{error_message[:300]}</code>

<b>Traceback:</b>
<code>{error_trace[:800]}</code>

<b>Последние логи:</b>
<code>{last_log_lines[:500]}</code>

<b>Логи:</b>
<a href="{log_url}">🔗 Открыть в Airflow</a>
        """.strip()
        
        # Telegram ограничение — 4096 символов
        if len(message) > 4096:
            message = message[:4090] + "...</code>"
        
        # Отправляем
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        response = requests.post(
            url,
            data={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "HTML"
            },
            timeout=10
        )
        
        if response.status_code == 200:
            logging.info("✅ Алерт отправлен в Telegram")
        else:
            logging.error(f"❌ Telegram ошибка: {response.status_code} - {response.text}")

    except Exception as e:
        # Логируем ошибку самого callback с полным traceback
        logging.error(f"❌ Ошибка отправки алерта: {e}")
        logging.error(traceback.format_exc())

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
    'on_failure_callback': send_telegram_alert
}

SPARK_JARS = ','.join([
    '/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar',
    '/opt/spark/jars/hadoop-aws-3.3.4.jar',
    '/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar',
    '/opt/spark/jars/wildfly-openssl-1.0.7.Final.jar',
])

@dag(
    dag_id = 'etl_transformation',
    description = 'Перевод файлов в хранилище S3',
    catchup = False,
    start_date = datetime(2026, 2, 1),
    schedule = '@weekly',
    tags = ['s3', 'spark'],
    on_failure_callback=send_telegram_alert
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

            if i.endswith('.csv'):
                
                parts = i.split('_')
                
                ans = parts[2][:-4] + '/' + parts[1] + '/' + parts[0]


                s3.load_file(
                    filename = data_dir + '/' + i,
                    key = ans,
                    bucket_name = 'data',
                    replace = True)
                
                logging.info(f'Загрузили файл {i}')
        

    @task.branch
    def transform_to_silver():

        s3 = S3Hook(aws_conn_id = 's3_connect')
        
        files = s3.get_conn().list_objects_v2(Bucket = 'data')

        for file in files['Contents']:

            file_size = file['Size']
            file_name = file['Key']

            logging.info(f'Считали {file_name}, размер {file_size}')

            if file_size > 1000_000:
                return 'spark_preprocess'
            
            else:
                return 'pandas_preprocess'



    @task
    def pandas_preprocess(**context):
        return 'Обработка через pandas'


    spark_preprocess = SparkSubmitOperator(
    task_id='spark_preprocess',
    application='/opt/airflow/jobs/etl_preprocess.py',
    conn_id='spark_connection',
    name='etl_transformation_spark',
    
    jars=SPARK_JARS,
    
    # Добавляем packages для boto3 и других зависимостей
    packages='org.apache.iceberg:iceberg-aws-bundle:1.5.0',
    
    conf={
        # Iceberg с REST catalog (как в jupyter)
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
        
        # S3/MinIO для чтения данных
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'minioadmin',
        'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
        'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
        
        # Память
        'spark.driver.memory': '1g',
        'spark.executor.memory': '1g',
        'spark.sql.adaptive.enabled': 'true',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',

        # Timeout для больших данных
        'spark.network.timeout': '6000s',
        'spark.executor.heartbeatInterval': '600s',
    },
    
    verbose=True,
)

    @task.bash(trigger_rule = 'one_success')
    def dbt_run():
        return 'cd /opt/airflow/dbt && dbt run --target docker'


    wait_for_data >> start() >> transform_to_silver() >> [pandas_preprocess(), spark_preprocess] >> dbt_run()

pipeline()

