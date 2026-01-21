import logging
import json
import os.path
from datetime import datetime, timedelta

import requests
from airflow.exceptions import AirflowFailException
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from airflow import DAG
from airflow.sdk import task

DATASET_SOURCE = "https://storage.googleapis.com/kagglesdsdata/datasets/9189971/14389740/e_commerce_shopper_behaviour_and_lifestyle.csv?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20260119%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20260119T143206Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=75916a7299b4c71c91af2f8dc234abde7ef5c84578c2fb22e2216b9bd67c486060bcded451bb36479f49044662cb6c9632d3f07348c461fef18bd061f6cd5fdc8a2d3666dff66fb3b68607c0a49c048833bf9b5e9108f2f082341e84031731319be459928ebeb1564e858a02913448c6f915ab999f70b890f47b6c1ba426d19dd67369affbd1a4800faaa0e56f14b5de0ddc5b7e13d12d26b1e022219477efeb477d41b884d7b6f2b1eff946d1a48d704b6020160abd82f3d80fcb5a16adf18221ca1f9858afa70f151a7b9809291bef41a9b2b96c3a664ec983f38c58e5c1c5ffe6e65777dd848ed5928918fc42cc44ded47d2804b70abfa488c6148d259093"
DATASET_NAME = "e-commerce"

BASE_DATA_DIR = "/opt/airflow/data/"
RAW_FILE_TEMPLATE = f"{BASE_DATA_DIR}/raw/{{{{ ds }}}}/{DATASET_NAME}.csv"
PROCESSED_FILE_TEMPLATE = f"{BASE_DATA_DIR}/processed/{{{{ ds }}}}/{DATASET_NAME}"

def raw_data_dir(ds: str) -> str:
    return f"{BASE_DATA_DIR}/raw/{ds}"

def raw_file(ds: str, dataset_name: str) -> str:
    return f"{raw_data_dir(ds)}/{dataset_name}.csv"

def processed_dir(ds: str) -> str:
    return f"{BASE_DATA_DIR}/processed/{ds}"

def processed_file(ds: str, dataset_name: str) -> str:
    return f"{processed_dir(ds)}/{dataset_name}.csv"

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
        logging.info(f"Checking dataset source availability: {DATASET_SOURCE}")
        resp = requests.get(DATASET_SOURCE)
        if resp.status_code != 200:
            raise AirflowFailException("Dataset source is unavailable")
        logging.info("Dataset source is available")

    @task
    def download_data(**kwargs):
        ds = kwargs["ds"]
        os.makedirs(raw_data_dir(ds), exist_ok=True)

        logging.info(f"Downloading raw dataset from {DATASET_SOURCE}")
        resp = requests.get(DATASET_SOURCE)
        if resp.status_code != 200:
            raise AirflowFailException("Dataset source is unavailable")

        with open(raw_file(ds, DATASET_NAME), "wb+") as f:
            f.write(resp.content)

        logging.info(f"Saved raw dataset to {raw_file(ds, DATASET_NAME).format(ds)}")

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
        spark_binary="spark-submit",
        verbose=True
    )

    @task
    def generate_report(**kwargs):
        ds = kwargs["ds"]
        reports_dir = f"/opt/airflow/reports/{ds}"
        report_file_path = os.path.join(reports_dir, "report.json")

        os.makedirs(reports_dir, exist_ok=True)

        logging.info("Generating report")

        df = pd.read_parquet(processed_dir_path)

        total_rows_processed = df.shape[0]

        today = datetime.today()
        yesterday = today - timedelta(days=1)
        yesterday_dt = datetime(yesterday.year, yesterday.month, yesterday.day)
        customers_yesterday = len(df[df["last_purchase_date"] == yesterday_dt])

        premium_users = df["premium_subscription"].sum()

        report = {
            "total_rows_processed": int(total_rows_processed),
            "customers_yesterday": int(customers_yesterday),
            "premium_users": int(premium_users)
        }

        logging.info(f"Generated report:\n {report}")

        kwargs["ti"].xcom_push(key="report", value=json.dumps(report))
        logging.info(f"Report pushed via XCom")

        with open(report_file_path, "w") as f:
            json.dump(report, f)
        logging.info(f"Saved report to {report_file_path}")

    @task
    def notify(**kwargs):
        report = json.loads(kwargs["ti"].xcom_pull(task_ids="generate_report", key="report"))
        print(f"Report:\n {report}")

    check_availability_task = check_source_available()
    download_data_task = download_data()
    spark_validate_task = spark_validate
    spark_transform_task = spark_transform
    # generate_report_task = generate_report()
    # notify_task = notify()
    check_availability_task >> download_data_task >> spark_validate_task >> spark_transform_task # >> generate_report_task >> notify_task
