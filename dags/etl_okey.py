from airflow.sdk import task, dag
from airflow.models import Variable
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule
import datetime, requests, os, shutil, logging

TG_TOKEN = Variable.get('TELEGRAM_TOKEN')
TG_CHAT_ID = Variable.get('TELEGRAM_CHAT_ID')

def telegram_alert(context):
    m = f'❌ Ошибка в даге <b>{context["ti"].dag_id}</b>\nЗадача: <b>{context["ti"].task_id}</b>\nДата: <b>{context["ds"]}</b>\nОшибка: <b>{context["exception"]}</b>'
    requests.post(url=f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage', data={'chat_id': TG_CHAT_ID, 'text': m, 'parse_mode': 'HTML'})

def telegram_success():
    requests.post(url=f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage', data={'chat_id': TG_CHAT_ID, 'text': '✅ Паплайн по Окей успешно завершился', 'parse_mode': 'HTML'})

default_args = {'retries': 2, 'retry_delay': datetime.timedelta(minutes=1)}

@dag(dag_id='etl_okey', start_date=datetime.datetime(year=2026, month=2, day=1), schedule='@daily', catchup=False,
     description='Автоматизация обработки данных для Окей', tags=['Spark', 'S3', 'okey'],
     on_success_callback=telegram_success, default_args=default_args)
def pipeline():
    
    wait_for_files = FileSensor(task_id='wait_for_files', 
    fs_conn_id='folder_connect',
    filepath='/opt/airflow/data/okey*', 
    mode='reschedule', 
    poke_interval=30,
    timeout=60,  
    on_failure_callback=telegram_alert)

    @task(on_failure_callback=telegram_alert, trigger_rule=TriggerRule.ALL_SUCCESS, retries=2, retry_delay=datetime.timedelta(minutes=1))
    def load_to_s3_raw():
        s3 = S3Hook(aws_conn_id='s3_connect')
        for file in os.listdir('/opt/airflow/data/'):
            if file.startswith('okey'):
                s3.load_file(filename=f'/opt/airflow/data/{file}', key=file, bucket_name='raw', replace=True)
                shutil.move(f'/opt/airflow/data/{file}', f'/opt/airflow/archive/{file}')
                logging.info(f'✓ {file}')

    spark_processing = SparkSubmitOperator(task_id='spark_processing', conn_id='spark_connection',
        trigger_rule=TriggerRule.ALL_DONE, application='jobs/okey_spark_processing.py',
        on_failure_callback=telegram_alert, retries=1, retry_delay=datetime.timedelta(minutes=2),
        deploy_mode='client', conf={
            'spark.jars.packages': 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.iceberg:iceberg-aws-bundle:1.5.0',
            'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog',
            'spark.sql.catalog.iceberg.type': 'rest', 'spark.sql.catalog.iceberg.uri': 'http://iceberg-rest:8181',
            'spark.sql.catalog.iceberg.io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
            'spark.sql.catalog.iceberg.s3.endpoint': 'http://minio:9000',
            'spark.sql.catalog.iceberg.s3.access-key-id': 'minioadmin',
            'spark.sql.catalog.iceberg.s3.secret-access-key': 'minioadmin',
            'spark.sql.catalog.iceberg.s3.path-style-access': 'true',
            'spark.sql.catalog.iceberg.client.region': 'us-east-1',
            'spark.sql.catalog.iceberg.warehouse': 's3://warehouse',
            'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
            'spark.hadoop.fs.s3a.access.key': 'minioadmin', 'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.pyspark.python': 'python3', 'spark.pyspark.driver.python': 'python3',
            'spark.sql.adaptive.enabled': 'true', 'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        })

    wait_for_files >> load_to_s3_raw() >> spark_processing

pipeline()