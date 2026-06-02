from airflow.sdk import dag
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import datetime
import requests


def get_tg_credentials():
    """Получаем credentials только в момент вызова"""
    token = Variable.get('TELEGRAM_TOKEN')
    chat_id = Variable.get('TELEGRAM_CHAT_ID')
    return token, chat_id


def telegram_alert(context):
    """Вызывается при ошибке задачи"""
    token, chat_id = get_tg_credentials()
    
    task_id = context.get('task_instance').task_id
    dag_id = context.get('dag').dag_id
    exception = context.get('exception')
    
    message = (
        f'❌ <b>Ошибка в справочнике</b>\n'
        f'DAG: <code>{dag_id}</code>\n'
        f'Task: <code>{task_id}</code>\n'
        f'Ошибка: <b>{exception}</b>'
    )
    
    try:
        requests.post(
            url=f'https://api.telegram.org/bot{token}/sendMessage',
            data={
                'chat_id': chat_id,
                'text': message,
                'parse_mode': 'HTML'
            },
            timeout=10
        )
    except Exception as e:
        print(f'Не удалось отправить сообщение в Telegram: {e}')


def telegram_success(context):  # ← context обязателен!
    """Вызывается при успехе DAG"""
    token, chat_id = get_tg_credentials()
    
    dag_id = context.get('dag').dag_id
    
    message = f'✅ Справочник товаров обновлён\nDAG: <code>{dag_id}</code>'
    
    try:
        requests.post(
            url=f'https://api.telegram.org/bot{token}/sendMessage',
            data={
                'chat_id': chat_id,
                'text': message,
                'parse_mode': 'HTML'
            },
            timeout=10
        )
    except Exception as e:
        print(f'Не удалось отправить сообщение в Telegram: {e}')


@dag(
    dag_id='update_product_mapping',
    start_date=datetime.datetime(year=2026, month=4, day=1),
    schedule='0 4 * * *',
    catchup=False,
    description='Обновление справочника товаров из Iceberg',
    tags=['mapping', 'clickhouse'],
    on_success_callback=telegram_success
)

def pipeline():    
    update_mapping = SparkSubmitOperator(
        task_id='update_mapping',
        conn_id='spark_connection',
        application='jobs/update_product_mapping.py',
        on_failure_callback=telegram_alert,
        retries=1,
        retry_delay=datetime.timedelta(minutes=2),
        deploy_mode='client',
        conf={
            'spark.jars.packages': (
                'org.apache.hadoop:hadoop-aws:3.3.4,'
                'com.amazonaws:aws-java-sdk-bundle:1.12.262,'
                'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,'
                'org.apache.iceberg:iceberg-aws-bundle:1.5.0,'
                'com.clickhouse:clickhouse-jdbc:0.6.2,'
                'org.apache.httpcomponents.client5:httpclient5:5.3.1'
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
            'spark.sql.catalog.iceberg.warehouse': 's3://warehouse',
            'spark.sql.catalog.iceberg.client.region': 'us-east-1',           
            'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
            'spark.hadoop.fs.s3a.access.key': 'minioadmin',
            'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.region': 'us-east-1',                   
            "spark.pyspark.python": "python3",
            "spark.pyspark.driver.python": "python3",
            "spark.executorEnv.PYSPARK_PYTHON": "python3",
            "spark.executorEnv.PYSPARK_DRIVER_PYTHON": "python3",
        }
    )
    
    update_mapping

pipeline()