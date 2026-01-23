import logging
import json
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow.exceptions import AirflowFailException
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.slack.notifications.slack import SlackNotifier

from airflow import DAG
from airflow.sdk import task

def jdbc_url():
    return f"jdbc:postgresql://{os.environ["POSTGRES_HOST"]}:{os.environ["POSTGRES_PORT"]}/{os.environ["WAREHOUSE_DB"]}"

def table_name():
    return f"{os.environ["WAREHOUSE_SCHEMA_RAW"]}.{os.environ["DATASET_NAME"]}_clean"

RAW_FILE_TEMPLATE = f"{os.environ["BASE_DATA_DIR"]}/{os.environ["RAW_SUBDIR"]}/{{{{ ds }}}}/{os.environ["DATASET_NAME"]}.csv"
REPORT_FILE_TEMPLATE = f"{os.environ["BASE_DATA_DIR"]}/{os.environ["REPORTS_SUBDIR"]}/{{{{ ds }}}}/{os.environ["DATASET_NAME"]}"

def raw_data_dir(ds: str) -> str:
    return f"{os.environ["BASE_DATA_DIR"]}/raw/{ds}"

def raw_file(ds: str, dataset_name: str) -> str:
    return f"{raw_data_dir(ds)}/{dataset_name}.csv"

def reports_dir(ds: str) -> str:
    return f"{os.environ["BASE_DATA_DIR"]}/reports/{ds}"

def report_file(ds: str, dataset_name: str) -> str:
    return f"{reports_dir(ds)}/{dataset_name}"

def spark_conn_id():
    return os.environ["SPARK_CONN_ID"]

def slack_conn_id():
    return os.environ["SLACK_CONN_ID"]

def slack_channel():
    return os.environ["SLACK_CHANNEL"]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 1, 1),
    "schedule_interval": "@daily"
}

with DAG(
    dag_id="pipeline",
    catchup=False,
    default_args=default_args
):
    @task
    def check_source_available():
        dataset_source = os.environ["DATASET_SOURCE"]
        logging.info(f"Checking dataset source availability: {dataset_source}")
        resp = requests.get(dataset_source)
        if resp.status_code != 200:
            raise AirflowFailException("Dataset source is unavailable")
        logging.info("Dataset source is available")

    @task
    def download_data(**kwargs):
        dataset_source = os.environ["DATASET_SOURCE"]
        dataset_name = os.environ["DATASET_NAME"]
        ds = kwargs["ds"]
        os.makedirs(raw_data_dir(ds), exist_ok=True)

        logging.info(f"Downloading raw dataset from {dataset_source}")
        resp = requests.get(dataset_source)
        if resp.status_code != 200:
            raise AirflowFailException("Dataset source is unavailable")

        with open(raw_file(ds, dataset_name), "wb+") as f:
            f.write(resp.content)

        logging.info(f"Saved raw dataset to {raw_file(ds, dataset_name)}")

    spark_validate = SparkSubmitOperator(
        task_id="spark_validate",
        application="/opt/airflow/spark_jobs/validate.py",
        conn_id=spark_conn_id(),
        application_args=[
            RAW_FILE_TEMPLATE
        ],
        spark_binary="spark-submit",
        verbose=True
    )

    spark_transform = SparkSubmitOperator(
        task_id="spark_transform",
        application="/opt/airflow/spark_jobs/transform.py",
        conn_id=spark_conn_id(),
        application_args=[
            RAW_FILE_TEMPLATE,
            jdbc_url(),
            table_name()
        ],
        jars=f"{os.environ["SPARK_JARS_DIR"]}/{os.environ["POSTGRES_JDBC_JAR"]}",
        spark_binary="spark-submit",
        verbose=True
    )

    spark_generate_report = SparkSubmitOperator(
        task_id="spark_generate_report",
        application="/opt/airflow/spark_jobs/generate_report.py",
        conn_id=spark_conn_id(),
        application_args=[
            jdbc_url(),
            table_name(),
            REPORT_FILE_TEMPLATE
        ],
        spark_binary="spark-submit",
        jars=f"{os.environ["SPARK_JARS_DIR"]}/{os.environ["POSTGRES_JDBC_JAR"]}",
        verbose=True
    )

    @task
    def push_report_to_xcom(**kwargs):
        dataset_name = os.environ["DATASET_NAME"]
        ds = kwargs["ds"]

        df = pd.read_parquet(report_file(ds, dataset_name))

        report = df.iloc[0].to_dict()

        kwargs["ti"].xcom_push(key="report", value=json.dumps(report))
        logging.info(f"Report pushed via XCom")

    def slack_report_text():
        return "{{ti.xcom_pull(task_ids='push_report_to_xcom', key='report')}}"

    @task(
        on_success_callback=SlackNotifier(
            slack_conn_id=slack_conn_id(),
            text=slack_report_text(),
            channel=slack_channel()
        )
    )
    def notify(**kwargs):
        report = json.loads(kwargs["ti"].xcom_pull(task_ids="push_report_to_xcom", key="report"))
        print(f"Report:\n {report}")

    check_availability_task = check_source_available()
    download_data_task = download_data()
    spark_validate_task = spark_validate
    spark_transform_task = spark_transform
    spark_generate_report_task = spark_generate_report
    push_report_to_xcom_task = push_report_to_xcom()
    notify_task = notify()
    check_availability_task >> download_data_task >> spark_validate_task >> spark_transform_task >> spark_generate_report_task >> push_report_to_xcom_task >> notify_task
