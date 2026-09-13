# spark/jobs/summarize_bronze.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("summarize-bronze").getOrCreate()

for table in ["events", "mentions", "gkg"]:
    df = spark.table(f"local.bronze.{table}")
    print(f"{table}: {df.count()} total rows")
    spark.sql(f"SELECT * FROM local.bronze.{table}.snapshots").select(
        "committed_at", "operation", "summary"
    ).show(truncate=False)

spark.stop()