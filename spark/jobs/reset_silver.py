"""Drop the two Silver tables (PURGE) so they can be recreated with new DDL.
Silver is a full recompute from Bronze, so nothing is lost. Bronze and Gold are untouched.
Recreate with bootstrap_silver.py, then re-run silver_transform.py and gold_aggregate.py.
"""
from pyspark.sql import SparkSession

TABLES = ["events_india", "mentions_enriched"]


def main():
    spark = SparkSession.builder.appName("reset-silver").getOrCreate()
    for table in TABLES:
        spark.sql(f"DROP TABLE IF EXISTS local.silver.{table} PURGE")
        print(f"Dropped local.silver.{table}")
    spark.stop()


if __name__ == "__main__":
    main()