from pyspark.sql import SparkSession

import sys

from pyspark.sql.functions import count, date_sub, col, current_date

input_path = sys.argv[1]
output_path = sys.argv[2]

spark = SparkSession.builder.appName("ReportGeneration").getOrCreate()

df = spark.read.parquet(input_path)

yesterday_df = df.filter(
    col("last_purchase_date") == date_sub(current_date(), 1)
)

yesterday_count = yesterday_df.count()

premium_users = (
    yesterday_df
    .filter(col("premium_subscription") == True)
    .count()
)

top_country_row = (
    yesterday_df
    .groupBy("country")
    .agg(
        count("*").alias("orders_yesterday")
    )
    .orderBy(col("orders_yesterday").desc())
    .limit(1)
    .collect()
)

top_country = top_country_row[0]["country"] if top_country_row else None

report_df = spark.createDataFrame(
    [
        (
            top_country,
            yesterday_count,
            premium_users
         )
    ], ["top_country_yesterday", "customers_yesterday", "premium_users"]
)

report_df.write.mode("overwrite").parquet(output_path)

print(f"Saved report to {output_path}")

spark.stop()