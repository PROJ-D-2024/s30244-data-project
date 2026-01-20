import logging
import json
import os.path
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow.exceptions import AirflowFailException

from airflow import DAG
from airflow.sdk import task

RAW_DATA_DIR = "/opt/airflow/data/raw"
PROCESSED_DATA_DIR = "/opt/airflow/data/processed"
RAW_FILE_PATH = os.path.join(RAW_DATA_DIR, "e-commerce.csv")
PROCESSED_FILE_PATH = os.path.join(PROCESSED_DATA_DIR, "e-commerce.csv")
REPORTS_DIR = "/opt/airflow/reports/e-commerce"
REPORT_FILE_PATH = os.path.join(REPORTS_DIR, "report.json")

DATASET_SOURCE = "https://storage.googleapis.com/kagglesdsdata/datasets/9189971/14389740/e_commerce_shopper_behaviour_and_lifestyle.csv?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20260119%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20260119T143206Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=75916a7299b4c71c91af2f8dc234abde7ef5c84578c2fb22e2216b9bd67c486060bcded451bb36479f49044662cb6c9632d3f07348c461fef18bd061f6cd5fdc8a2d3666dff66fb3b68607c0a49c048833bf9b5e9108f2f082341e84031731319be459928ebeb1564e858a02913448c6f915ab999f70b890f47b6c1ba426d19dd67369affbd1a4800faaa0e56f14b5de0ddc5b7e13d12d26b1e022219477efeb477d41b884d7b6f2b1eff946d1a48d704b6020160abd82f3d80fcb5a16adf18221ca1f9858afa70f151a7b9809291bef41a9b2b96c3a664ec983f38c58e5c1c5ffe6e65777dd848ed5928918fc42cc44ded47d2804b70abfa488c6148d259093"

REQUIRED_COLUMNS = [
    "user_id",
    "age",
    "gender",
    "country",
    "monthly_spend",
    "weekly_purchases",
    "average_order_value",
    "purchase_conversion_rate",
    "last_purchase_date"
]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 1, 1),
    "schedule_interval": "@hourly"
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
    def download_data():
        os.makedirs(RAW_DATA_DIR, exist_ok=True)

        logging.info(f"Downloading raw dataset from {DATASET_SOURCE}")
        df = pd.read_csv(DATASET_SOURCE)

        df.to_csv(RAW_FILE_PATH)
        logging.info(f"Saved raw dataset to {RAW_FILE_PATH}")

        return RAW_FILE_PATH

    @task
    def validate_data(file_path: str):
        df = pd.read_csv(file_path)

        logging.info(f"Validating data: {file_path}")

        if df.shape[0] < 50000:
            raise AirflowFailException("Dataset is too small")

        missing_columns = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        if missing_columns:
            raise AirflowFailException(f"Missing columns: {missing_columns}")

        nulls = df[REQUIRED_COLUMNS].isnull().mean()
        high_nulls = nulls[nulls > 0.3]

        if not high_nulls.empty:
            raise AirflowFailException(f"Too many nulls in columns: {list(high_nulls.index)}")

        logging.info(f"Successfully validated data: {file_path}")

        return df

    @task
    def transform_data(df: pd.DataFrame):
        os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

        logging.info("Transforming data")

        rows = df.shape[0]
        df = df[df["monthly_spend"] >= 0]
        df = df[df["weekly_purchases"] >= 0]
        df = df[df["account_age_months"] >= 0]
        new_rows = df.shape[0]
        logging.info(f"Removed {rows-new_rows} invalid rows")

        df["last_purchase_date"] = pd.to_datetime(df["last_purchase_date"])
        logging.info("Converted last_purchase_date to datetime")

        bool_columns = ["has_children", "loyalty_program_member", "weekend_shopper", "premium_subscription"]
        for column in bool_columns:
            df[column] = df[column].astype(bool)
            logging.info(f"Converted {column} to bool")

        df.to_csv(PROCESSED_FILE_PATH)
        logging.info(f"Saved processed dataset to {PROCESSED_FILE_PATH}")

        return df

    @task
    def generate_report(df: pd.DataFrame, **kwargs):
        os.makedirs(REPORTS_DIR, exist_ok=True)

        logging.info("Generating report")

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

        with open(REPORT_FILE_PATH, "w") as f:
            json.dump(report, f)
        logging.info(f"Saved report to {REPORT_FILE_PATH}")

    @task
    def notify(**kwargs):
        report = json.loads(kwargs["ti"].xcom_pull(task_ids="generate_report", key="report"))
        print(f"Report:\n {report}")

    check_source_available()
    raw_file_path = download_data()
    valid_data = validate_data(raw_file_path)
    transformed_data = transform_data(valid_data)
    generate_report_task = generate_report(transformed_data)
    notify_task = notify()
    generate_report_task >> notify_task
