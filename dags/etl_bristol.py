from airflow.sdk import task, dag
from airflow.models import Variable
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule

import datetime
import requests
import os
import shutil
import logging
import html


def send_telegram_message(text: str):
    token = Variable.get("TELEGRAM_TOKEN")
    chat_id = Variable.get("TELEGRAM_CHAT_ID")

    response = requests.post(
        url=f"https://api.telegram.org/bot{token}/sendMessage",
        data={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
        },
        timeout=10,
    )

    response.raise_for_status()


def telegram_alert(context):
    dag_id = context["ti"].dag_id
    task_id = context["ti"].task_id
    ds = context.get("ds", "unknown")
    error = html.escape(str(context.get("exception", "Unknown error")))

    message = (
        f"❌ <b>Ошибка в даге</b> <code>{dag_id}</code>\n"
        f"Задача: <code>{task_id}</code>\n"
        f"Дата: <b>{ds}</b>\n"
        f"Ошибка: <b>{error}</b>"
    )

    send_telegram_message(message)


def telegram_success(context):
    dag_id = context["dag"].dag_id
    ds = context.get("ds", "unknown")

    message = (
        f"✅ <b>Пайплайн по Бристоль успешно завершился</b>\n"
        f"DAG: <code>{dag_id}</code>\n"
        f"Дата: <b>{ds}</b>"
    )

    send_telegram_message(message)


@dag(
    dag_id="etl_bristol",
    start_date=datetime.datetime(year=2026, month=2, day=1),
    schedule="@daily",
    catchup=False,
    description="Автоматизация обработки данных для Бристоль",
    tags=["Spark", "S3", "bristol"],
    on_success_callback=telegram_success,
)
def pipeline():

    wait_for_files = FileSensor(
        task_id="wait_for_files",
        fs_conn_id="folder_connect",
        filepath="/opt/airflow/data/bristol*",
        mode="reschedule",
        poke_interval=30,
        timeout=60,
        on_failure_callback=telegram_alert,
    )

    @task(
        on_failure_callback=telegram_alert,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        retry_delay=datetime.timedelta(minutes=1),
    )
    def load_to_s3_raw():
        input_folder = "/opt/airflow/data/"
        output_folder = "/opt/airflow/archive/"
        s3 = S3Hook(aws_conn_id="s3_connect")
        files_processed = 0

        for file in os.listdir(input_folder):
            if file.startswith("bristol"):
                file_path = input_folder + file
                s3.load_file(
                    filename=file_path,
                    key=file,
                    bucket_name="raw",
                    replace=True,
                )
                logging.info(f"✓ Загрузили {file} в сырой слой")
                shutil.move(src=file_path, dst=output_folder + file)
                logging.info(f"✓ Архивировали {file}")
                files_processed += 1

        if files_processed == 0:
            logging.warning("⚠ Нет файлов Бристоль")
        else:
            logging.info(f"✅ Обработано: {files_processed}")

    spark_processing = SparkSubmitOperator(
        task_id="spark_processing",
        conn_id="spark_connection",
        trigger_rule=TriggerRule.ALL_DONE,
        application="jobs/bristol_spark_processing.py",
        on_failure_callback=telegram_alert,
        retries=1,
        retry_delay=datetime.timedelta(minutes=2),
        deploy_mode="client",
        conf={
            "spark.jars.packages": (
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
                "org.apache.iceberg:iceberg-aws-bundle:1.5.0"
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
            "spark.sql.catalog.iceberg.client.region": "us-east-1",
            "spark.sql.catalog.iceberg.warehouse": "s3://warehouse",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.pyspark.python": "python3",
            "spark.pyspark.driver.python": "python3",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
        },
    )

    wait_for_files >> load_to_s3_raw() >> spark_processing


pipeline()