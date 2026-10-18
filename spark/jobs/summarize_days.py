"""Read-only: per-day row counts and distinct 15-minute slots for each Bronze table.

A fully ingested day should show 96 slots. Fewer means missing slots (a GDELT-side
gap or a skipped ingest); the row counts show whether a day is oddly small or large.
"""
from pyspark.sql import SparkSession

TABLES = [("events", "date_added_ts"), ("mentions", "mention_ts"), ("gkg", "gkg_ts")]


def main():
    spark = SparkSession.builder.appName("summarize-days").getOrCreate()

    for name, ts_col in TABLES:
        print(f"=== DAYS {name} ===")
        spark.sql(f"""
            SELECT date({ts_col}) AS day,
                   COUNT(*)                  AS row_count,
                   COUNT(DISTINCT {ts_col})  AS slots
            FROM local.bronze.{name}
            GROUP BY 1
            ORDER BY 1
        """).show(60, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()