# spark/jobs/bronze_ingest.py
import argparse
from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType
from schemas import EVENTS_COLUMNS, MENTIONS_COLUMNS, GKG_COLUMNS
from utils import parse_gdelt_ts

# A source mention row is only identified by event + article + time + sentence +
# the three char offsets + Confidence. (GLOBALEVENTID, MentionIdentifier, mention_ts)
# alone collides: the same article can mention an event in several sentences, GDELT
# also emits rows that differ only in Actor1/Actor2 char offsets, and (observed on
# 2026-09-01) rows that differ only in Confidence (10 vs 20).
MENTIONS_KEY = [
    "GLOBALEVENTID", "MentionIdentifier", "mention_ts",
    "SentenceID", "Actor1CharOffset", "Actor2CharOffset", "ActionCharOffset",
    "Confidence",
]


def dedup_and_append(spark, batch_df, target_table: str, key_cols: list,
                     drop_exact_dupes: bool = False, cache_new_rows: bool = False):
    """Insert-only idempotency via an explicit left-anti join, since Iceberg's
    MERGE INTO insert-only optimization was observed to append duplicates
    unconditionally rather than checking existing keys.

    - The anti-join is null-safe (<=>), so a row with a NULL in a key column is
      still recognised on re-ingest instead of being appended every time.
    - drop_exact_dupes removes rows that are identical in EVERY column within the
      batch (the anti-join only dedups against rows already in the table).
    - cache_new_rows persists the anti-join result so the expensive scan of the
      existing keys runs once instead of twice (count, then write). Used by the
      bulk backfill; the live per-slot pipeline leaves it off.
    - total_count is the raw source row count, before any dedup.
    - Rows that share a key but differ in content are NOT dropped (the verify step
      flags them); only exact copies and rows already in the table are removed.
    """
    total_count = batch_df.count()

    if drop_exact_dupes:
        batch_df = batch_df.dropDuplicates()

    existing_keys = spark.table(target_table).select(*key_cols).distinct()
    match = reduce(
        lambda a, b: a & b,
        [F.col(f"b.{c}").eqNullSafe(F.col(f"e.{c}")) for c in key_cols],
    )
    new_rows = batch_df.alias("b").join(existing_keys.alias("e"), match, "left_anti")

    if cache_new_rows:
        new_rows = new_rows.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        new_count = new_rows.count()
        if new_count > 0:
            new_rows.writeTo(target_table).append()
    finally:
        if cache_new_rows:
            new_rows.unpersist()
    return new_count, total_count


def ingest_events_batch(spark, path: str, cache: bool = False):
    print(f"Reading events from: {path}")
    target_table = "local.bronze.events"
    target_schema = spark.table(target_table).schema

    # Map exact CSV column order to Iceberg data types
    csv_schema = StructType([target_schema[col] for col in EVENTS_COLUMNS if col in target_schema.names])

    raw_df = spark.read \
        .option("sep", "\t") \
        .option("nullValue", "") \
        .schema(csv_schema) \
        .csv(path)

    batch_df = raw_df.withColumn("date_added_ts", parse_gdelt_ts("DATEADDED"))
    return dedup_and_append(spark, batch_df, target_table, ["GLOBALEVENTID"],
                            cache_new_rows=cache)


def ingest_mentions_batch(spark, path: str, cache: bool = False):
    print(f"Reading mentions from: {path}")
    target_table = "local.bronze.mentions"
    target_schema = spark.table(target_table).schema

    csv_schema = StructType([target_schema[col] for col in MENTIONS_COLUMNS if col in target_schema.names])

    raw_df = spark.read \
        .option("sep", "\t") \
        .option("nullValue", "") \
        .schema(csv_schema) \
        .csv(path)

    batch_df = raw_df.withColumn("mention_ts", parse_gdelt_ts("MentionTimeDate"))
    return dedup_and_append(
        spark, batch_df, target_table, MENTIONS_KEY,
        drop_exact_dupes=True, cache_new_rows=cache,
    )


def ingest_gkg_batch(spark, path: str, cache: bool = False):
    print(f"Reading gkg from: {path}")
    target_table = "local.bronze.gkg"
    target_schema = spark.table(target_table).schema

    csv_schema = StructType([target_schema[col] for col in GKG_COLUMNS if col in target_schema.names])

    raw_df = spark.read \
        .option("sep", "\t") \
        .option("nullValue", "") \
        .schema(csv_schema) \
        .csv(path)

    batch_df = raw_df.withColumn("gkg_ts", parse_gdelt_ts("DATE"))
    return dedup_and_append(spark, batch_df, target_table, ["GKGRECORDID"],
                            cache_new_rows=cache)


STREAM_FUNCS = {
    "events": ingest_events_batch,
    "mentions": ingest_mentions_batch,
    "gkg": ingest_gkg_batch,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stream", required=True, choices=STREAM_FUNCS.keys())
    parser.add_argument("--path", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName(f"bronze-ingest-{args.stream}").getOrCreate()
    new_count, total_count = STREAM_FUNCS[args.stream](spark, args.path)
    print(f"{args.stream}: {new_count} new rows written, {total_count} rows in source batch "
          f"({total_count - new_count} already existed or were exact in-batch copies)")
    spark.stop()


if __name__ == "__main__":
    main()