import sys

from pyspark.sql import SparkSession

class DataValidationError(Exception):
    pass

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

input_path = sys.argv[1]

spark = SparkSession.builder.appName("Validation").getOrCreate()

df = spark.read.csv(input_path, header=True, inferSchema=True)

print(f"Validating data: {input_path}")

rows = df.count()
if rows < 50000:
    raise DataValidationError("Dataset is too small")

missing_columns = [c for c in REQUIRED_COLUMNS if c not in df.columns]
if missing_columns:
    raise DataValidationError(f"Missing columns: {missing_columns}")

for column in REQUIRED_COLUMNS:
    nulls = df.filter(df[column].isNull()).count() / rows
    if nulls > 0.3:
        raise DataValidationError(f"Too many nulls in column: {column}")

print(f"Successfully validated data: {input_path}")

spark.stop()