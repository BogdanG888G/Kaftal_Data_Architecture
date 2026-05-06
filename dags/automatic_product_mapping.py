from airflow.sdk import dag
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import datetime
import requests

TG_TOKEN = Variable.get('TELEGRAM_TOKEN')
TG_CHAT_ID = Variable.get('TELEGRAM_CHAT_ID')

def telegram_alert(context):
    m = f'❌ Ошибка в справочнике\n<b>{context["exception"]}</b>'
    requests.post(url=f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage', data={'chat_id': TG_CHAT_ID, 'text': m, 'parse_mode': 'HTML'})

def telegram_success():
    requests.post(url=f'https://api.telegram.org/bot{TG_TOKEN}/sendMessage', data={'chat_id': TG_CHAT_ID, 'text': '✅ Справочник товаров обновлён', 'parse_mode': 'HTML'})

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
            'spark.pyspark.python': 'python3.10',
            'spark.pyspark.driver.python': 'python3.10',
        }
    )
    
    update_mapping

pipeline()