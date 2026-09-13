# spark/jobs/reset_bronze.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("reset-bronze").getOrCreate()
for table in ["events", "mentions", "gkg"]:
    spark.sql(f"DROP TABLE IF EXISTS local.bronze.{table}")
    print(f"Dropped local.bronze.{table}")
spark.stop()