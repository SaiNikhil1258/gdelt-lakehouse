"""Read-only: investigate the 3 GKG rows whose gkg_ts is NULL (the NULL day in summarize_days).

Also counts NULL timestamps in the other two Bronze tables for comparison.
"""
from pyspark.sql import SparkSession, functions as F


def main():
    spark = SparkSession.builder.appName("diagnose-null-gkg-ts").getOrCreate()

    print("=== NULL TS COUNTS ===")
    for table, col in [("events", "date_added_ts"), ("mentions", "mention_ts"), ("gkg", "gkg_ts")]:
        n = spark.table(f"local.bronze.{table}").filter(F.col(col).isNull()).count()
        print(f"{table}.{col} NULL rows: {n}")

    print("=== NULL GKG TS ROWS ===")
    spark.sql("""
        SELECT GKGRECORDID,
               substring(GKGRECORDID, 1, 14)           AS recordid_slot,
               DATE,
               DATE IS NULL                            AS date_is_null,
               length(DATE)                            AS date_len,
               SourceCollectionIdentifier,
               SourceCommonName,
               DocumentIdentifier,
               regexp_extract(_file, '([^/]+)$', 1)    AS data_file
        FROM local.bronze.gkg
        WHERE gkg_ts IS NULL
    """).show(truncate=60)

    spark.stop()


if __name__ == "__main__":
    main()