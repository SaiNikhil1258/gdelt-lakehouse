"""Read-only: file counts / sizes / partitions per lakehouse table, plus a first look at
Silver event dates and the Gold bilateral table. Feeds the compaction decision.
"""
from pyspark.sql import SparkSession

TABLES = [
    "bronze.events", "bronze.mentions", "bronze.gkg",
    "silver.events_india", "silver.mentions_enriched",
    "gold.india_bilateral_sentiment_daily", "gold.india_state_conflict_daily",
    "gold.india_event_mention_counts",
]


def main():
    spark = SparkSession.builder.appName("table-health").getOrCreate()

    print("=== TABLE HEALTH ===")
    for t in TABLES:
        f = spark.sql(
            f"SELECT COUNT(*) AS n, SUM(file_size_in_bytes) AS bytes, SUM(record_count) AS recs "
            f"FROM local.{t}.files"
        ).first()
        parts = spark.sql(f"SELECT COUNT(*) AS n FROM local.{t}.partitions").first()["n"]
        snaps = spark.sql(f"SELECT COUNT(*) AS n FROM local.{t}.snapshots").first()["n"]
        n_files = f["n"] or 0
        size_mb = (f["bytes"] or 0) / 1e6
        avg_mb = size_mb / n_files if n_files else 0.0
        print(
            f"{t:40s} files={n_files:6d} partitions={parts:5d} "
            f"size_mb={size_mb:9.1f} avg_file_mb={avg_mb:7.2f} rows={f['recs'] or 0:9d} snapshots={snaps:4d}"
        )

    print("=== SILVER EVENT DATES ===")
    spark.sql("""
        SELECT COUNT(*)                                                 AS row_count,
               MIN(event_date)                                          AS min_date,
               MAX(event_date)                                          AS max_date,
               COUNT(DISTINCT event_date)                               AS distinct_dates,
               SUM(CASE WHEN event_date >= DATE '2026-09-01' THEN 1 ELSE 0 END) AS rows_in_sept
        FROM local.silver.events_india
    """).show(truncate=False)

    print("=== TOP PARTNERS (events over the whole table) ===")
    spark.sql("""
        SELECT partner_country, SUM(event_count) AS events, COUNT(*) AS day_rows
        FROM local.gold.india_bilateral_sentiment_daily
        GROUP BY partner_country
        ORDER BY events DESC
        LIMIT 8
    """).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()