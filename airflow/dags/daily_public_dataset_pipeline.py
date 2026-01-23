import logging
import json
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow.exceptions import AirflowFailException
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow import DAG
from airflow.sdk import task

RAW_FILE_TEMPLATE = f"{os.environ["BASE_DATA_DIR"]}/{os.environ["RAW_SUBDIR"]}/{{{{ ds }}}}/{os.environ["DATASET_NAME"]}.csv"
PROCESSED_FILE_TEMPLATE = f"{os.environ["BASE_DATA_DIR"]}/{os.environ["PROCESSED_SUBDIR"]}/{{{{ ds }}}}/{os.environ["DATASET_NAME"]}"
REPORT_FILE_TEMPLATE = f"{os.environ["BASE_DATA_DIR"]}/{os.environ["REPORTS_SUBDIR"]}/{{{{ ds }}}}/{os.environ["DATASET_NAME"]}"

def raw_data_dir(ds: str) -> str:
    return f"{os.environ["BASE_DATA_DIR"]}/raw/{ds}"

def raw_file(ds: str, dataset_name: str) -> str:
    return f"{raw_data_dir(ds)}/{dataset_name}.csv"

def processed_dir(ds: str) -> str:
    return f"{os.environ["BASE_DATA_DIR"]}/processed/{ds}"

def processed_file(ds: str, dataset_name: str) -> str:
    return f"{processed_dir(ds)}/{dataset_name}"

def reports_dir(ds: str) -> str:
    return f"{os.environ["BASE_DATA_DIR"]}/reports/{ds}"

def report_file(ds: str, dataset_name: str) -> str:
    return f"{reports_dir(ds)}/{dataset_name}"

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
        conn_id="spark_default",
        application_args=[
            RAW_FILE_TEMPLATE
        ],
        spark_binary="spark-submit",
        verbose=True
    )

    spark_transform = SparkSubmitOperator(
        task_id="spark_transform",
        application="/opt/airflow/spark_jobs/transform.py",
        conn_id="spark_default",
        application_args=[
            RAW_FILE_TEMPLATE,
            PROCESSED_FILE_TEMPLATE
        ],
        jars=f"{os.environ["SPARK_JARS_DIR"]}/{os.environ["POSTGRES_JDBC_JAR"]}",
        spark_binary="spark-submit",
        verbose=True
    )

    spark_generate_report = SparkSubmitOperator(
        task_id="spark_generate_report",
        application="/opt/airflow/spark_jobs/generate_report.py",
        conn_id="spark_default",
        application_args=[
            PROCESSED_FILE_TEMPLATE,
            REPORT_FILE_TEMPLATE
        ],
        spark_binary="spark-submit",
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

    @task
    def notify(**kwargs):
        report = json.loads(kwargs["ti"].xcom_pull(task_ids="push_report_to_xcom", key="report"))
        print(f"Report:\n {report}")

    check_availability_task = check_source_available()
    download_data_task = download_data()
    spark_validate_task = spark_validate
    spark_transform_task = spark_transform
    # spark_generate_report_task = spark_generate_report
    # push_report_to_xcom_task = push_report_to_xcom()
    # notify_task = notify()
    check_availability_task >> download_data_task >> spark_validate_task >> spark_transform_task # >> spark_generate_report_task >> push_report_to_xcom_task >> notify_task
