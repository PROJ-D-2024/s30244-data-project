import os

from pyspark.sql import SparkSession

import sys

from pyspark.sql.functions import to_date, col

input_path = sys.argv[1]
jdbc_url = sys.argv[2]
table_name = sys.argv[3]

spark = SparkSession.builder.appName("Transformation").getOrCreate()

df = spark.read.csv(input_path, header=True, inferSchema=True)

rows = df.count()

df_clean = df.filter(
    (df.monthly_spend >= 0) &
    (df.weekly_purchases >= 0) &
    (df.account_age_months >= 0)
)

new_rows = df_clean.count()

print(f"Removed {rows-new_rows} invalid rows")

df_clean = df_clean.withColumn("last_purchase_date", to_date("last_purchase_date", "yyyy-MM-dd"))
print("Converted last_purchase_date to datetime")

bool_columns = ["has_children", "loyalty_program_member", "weekend_shopper", "premium_subscription"]
for column in bool_columns:
    df_clean = df_clean.withColumn(column, col(column).cast("boolean"))
    print(f"Converted {column} to bool")

df_clean.write.format("jdbc")\
    .option("url", jdbc_url)\
    .option("dbtable", table_name)\
    .option("user", os.environ["POSTGRES_USER"])\
    .option("password", os.environ["POSTGRES_PASSWORD"])\
    .option("driver", "org.postgresql.Driver")\
    .mode("overwrite")\
    .save()

spark.stop()
