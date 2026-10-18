"""Read-only: print one fingerprint line per lakehouse table:  FP <table> <row_count> <content_hash>

Bronze tables are append-only, so only the row count is fingerprinted (hash shown as '-').
Silver and Gold are recomputed, so a content hash is added: an order-independent sum of a
per-row xxhash64. Two details keep the comparison fair:
  * doubles are rounded to 6 decimals first, so harmless floating-point summation-order
    noise in averages is not reported as a difference
  * gold.india_event_mention_counts.last_updated_ts is excluded; it changes on every MERGE by design
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType

BRONZE = ["bronze.events", "bronze.mentions", "bronze.gkg"]
RECOMPUTED = {
    "silver.events_india": [],
    "silver.mentions_enriched": [],
    "gold.india_bilateral_sentiment_daily": [],
    "gold.india_state_conflict_daily": [],
    "gold.india_event_mention_counts": ["last_updated_ts"],
}


def content_fingerprint(df, exclude):
    cols = []
    for field in df.schema.fields:
        if field.name in exclude:
            continue
        c = F.col(field.name)
        if isinstance(field.dataType, DoubleType):
            c = F.round(c, 6)
        cols.append(c)
    row = df.agg(F.count("*").alias("n"), F.sum(F.xxhash64(*cols)).alias("h")).first()
    return row["n"], (row["h"] if row["h"] is not None else 0)


def main():
    spark = (
        SparkSession.builder.appName("table-fingerprint")
        .config("spark.executor.memory", "4g")
        .getOrCreate()
    )

    for table in BRONZE:
        n = spark.table(f"local.{table}").count()
        print(f"FP {table} {n} -", flush=True)

    for table, exclude in RECOMPUTED.items():
        n, h = content_fingerprint(spark.table(f"local.{table}"), exclude)
        print(f"FP {table} {n} {h}", flush=True)

    spark.stop()


if __name__ == "__main__":
    main()