# s30244-data-project
## Description
This project implements an end-to-end data engineering pipeline using:
- Apache Airflow - orchestration 
- Apache Spark - data processing
- PostgreSQL - data warehouse
- dbt - analytics engineering level
### Dataset
- Source: https://www.kaggle.com/datasets/dhrubangtalukdar/e-commerce-shopper-behavior-amazonshopify-based/data
- Size: 1.000.000 entries
- Description: the dataset contains demographic and behavioral information about e-commerce customers
### Pipeline
#### 1. Data ingestion
The dataset is downloaded and saved as a CSV file.
#### 2. Data validation
The raw dataset is validated using Spark.  
Validation includes:
- Minimum number of rows
- Presence of required columns
- Percentage of null values per column
#### 3. Data cleaning and transformation
Validated data is processed with Spark to ensure consistency and types.
Transformation includes:
- Removal of invalid records
- Casting columns to proper data types
#### 4. Daily report generation
Based on the processed data, Spark generates a daily report.
The report includes:
- Country with the highest number of customers who placed orders yesterday
- Total number of customers who placed orders yesterday
- Total number of premium users
#### 5. Notification (Slack)
The generated report is sent to a Slack channel.

## Setup 
### 1. Clone the repository
```
git clone git@github.com:PROJ-D-2024/s30244-data-project.git
```
### 2. Create .env file in the project root
```
AIRFLOW_UID=50000

DATASET_SOURCE=https://storage.googleapis.com/kagglesdsdata/datasets/9189971/14389740/e_commerce_shopper_behaviour_and_lifestyle.csv?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20260123%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20260123T085407Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=1f2bbe0180661a86e47c378c5443a3fdb6d27753448765b9ad0a12cf487e430429681ad0a807ec7042e209d20cc1fad9b4c7850900e92aa10e6aafc8a868e72a18a2d175f4729dff36f8a1c80f484169298e7d2ef90e78056faae6f31cb58b09a1bd34cfd308e0ba723fc2cf1764aaa3dd08fd706b24cd08bf739a65e76ac94f47b6b71b295fa1d7cd7c27fe255d7d269dacaf5dac001319c9b80823fced71b9c3d9074a82c0d950b60ec1254f10bd67f3aeb0a5164f2888096602da7fdbc72f14fdbc47fb7d1dcd455e6532b400c568a18cf9ca0ebd398e0c1e21e0d0592575571eed1fbf270a76267eb05e13f22747337f63bd81120c1ac0d6b773b9078945

BASE_DATA_DIR=/opt/airflow/data/
RAW_SUBDIR=raw
PROCESSED_SUBDIR=processed
REPORTS_SUBDIR=reports
DATASET_NAME=e_commerce

WAREHOUSE_DB=warehouse
WAREHOUSE_SCHEMA_RAW=raw
WAREHOUSE_SCHEMA_ANALYTICS=analytics

POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow

SPARK_CONN_ID=spark_default
SLACK_CONN_ID=slack_default
SLACK_CHANNEL=pro
```

### 3. Start services
```
docker compose up -d
```

### 4. Create connections
Go to http://localhost:8080/connections (Airflow UI).

Create 2 connections:  
**Spark**
- Connection ID: spark_default
- Connection Type: Spark
- Host: local[*]

**Slack**
- Connection ID: slack_default
- Connection Type: Slack API
- Slack API Token: *your API token*

### 5. Run pipeline DAG (*Manually*)
Go to http://localhost:8080/dags/pipeline.

Click the "Trigger" button (*Single Run*).

**Execution result:**
![dag run](screenshots/dag_run.png)

**Saved data:**
![raw data](screenshots/raw_data.png)

**Slack notification:**
![slack notification](screenshots/slack_notification.png)

### 6. Run transformations and tests(dbt)
```
docker compose run --rm dbt run --project-dir analytics
```
**Result:**
![dbt data](screenshots/dbt_data.png)

```
docker compose run --rm dbt test --project-dir analytics
```

**Result:**
![dbt test](screenshots/dbt_test.png)
