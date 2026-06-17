from airflow.sdk import dag
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import datetime
import requests
import html


def send_telegram_message(text: str):
    token = Variable.get("TELEGRAM_TOKEN")
    chat_id = Variable.get("TELEGRAM_CHAT_ID")

    requests.post(
        url=f"https://api.telegram.org/bot{token}/sendMessage",
        data={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
        },
        timeout=10,
    ).raise_for_status()


def telegram_alert(context):
    exception = html.escape(str(context.get("exception", "Unknown error")))
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id

    send_telegram_message(
        f"❌ <b>Ошибка в витрине Кондитерки</b>\n"
        f"DAG: <code>{dag_id}</code>\n"
        f"Task: <code>{task_id}</code>\n"
        f"Ошибка: <b>{exception}</b>"
    )


def telegram_success(context):
    dag_id = context["dag"].dag_id
    send_telegram_message(
        f"✅ <b>Витрина konditerka sales_mart обновлена</b>\n"
        f"DAG: <code>{dag_id}</code>"
    )


@dag(
    dag_id="konditerka_build_sales_mart",
    start_date=datetime.datetime(year=2026, month=4, day=1),
    schedule="0 5 * * *",   # каждый день в 05:00
    catchup=False,
    description="Сборка единой витрины Кондитерки (X5 + Магнит + Ашан) → ClickHouse",
    tags=["mart", "clickhouse", "konditerka"],
    on_success_callback=telegram_success,
)
def pipeline():

    build_mart = SparkSubmitOperator(
        task_id="build_mart",
        conn_id="spark_connection",
        application="jobs/build_sales_mart_konditerka.py",
        on_failure_callback=telegram_alert,
        py_files="/opt/airflow/deps/clickhouse_connect",
        retries=1,
        retry_delay=datetime.timedelta(minutes=2),
        deploy_mode="client",
        conf={
            "spark.jars.packages": (
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
                "org.apache.iceberg:iceberg-aws-bundle:1.5.0,"
                "com.clickhouse:clickhouse-jdbc:0.6.2,"
                "org.apache.httpcomponents.client5:httpclient5:5.3.1"
            ),
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.iceberg.type": "rest",
            "spark.sql.catalog.iceberg.uri": "http://iceberg-rest:8181",
            "spark.sql.catalog.iceberg.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "spark.sql.catalog.iceberg.s3.endpoint": "http://minio:9000",
            "spark.sql.catalog.iceberg.s3.access-key-id": "minioadmin",
            "spark.sql.catalog.iceberg.s3.secret-access-key": "minioadmin",
            "spark.sql.catalog.iceberg.s3.path-style-access": "true",
            "spark.sql.catalog.iceberg.warehouse": "s3://warehousekonditerka",
            "spark.sql.catalog.iceberg.client.region": "us-east-1",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.region": "us-east-1",
            "spark.pyspark.python": "python3",
            "spark.pyspark.driver.python": "python3",
            "spark.executorEnv.PYSPARK_PYTHON": "python3",
            "spark.executorEnv.PYSPARK_DRIVER_PYTHON": "python3",
        },
    )

    build_mart


pipeline()