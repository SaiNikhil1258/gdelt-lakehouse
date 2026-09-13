# spark/jobs/bronze_ingest.py
import argparse
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType
from schemas import EVENTS_COLUMNS, MENTIONS_COLUMNS, GKG_COLUMNS
from utils import parse_gdelt_ts


def dedup_and_append(spark, batch_df, target_table: str, key_cols: list):
    """Insert-only idempotency via an explicit left-anti join, since Iceberg's
    MERGE INTO insert-only optimization was observed to append duplicates
    unconditionally rather than checking existing keys."""
    existing_keys = spark.table(target_table).select(*key_cols).distinct()
    new_rows = batch_df.join(existing_keys, on=key_cols, how="left_anti")
    new_count = new_rows.count()
    total_count = batch_df.count()
    if new_count > 0:
        new_rows.writeTo(target_table).append()
    return new_count, total_count


def ingest_events_batch(spark, path: str):
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
    return dedup_and_append(spark, batch_df, target_table, ["GLOBALEVENTID"])


def ingest_mentions_batch(spark, path: str):
    print(f"Reading mentions from: {path}")
    target_table = "local.bronze.mentions"
    target_schema = spark.table(target_table).schema
    
    csv_schema = StructType([target_schema[col] for col in MENTIONS_COLUMNS if col in target_schema.names])

    raw_df = spark.read \
        .option("sep", "\t") \
        .option("nullValue", "") \
        .schema(csv_schema) \
        .csv(path)

    # NOTE: fmt below is a placeholder until diagnose_mentions.py output confirms the real format
    batch_df = raw_df.withColumn("mention_ts", parse_gdelt_ts("MentionTimeDate"))
    return dedup_and_append(
        spark, batch_df, target_table,
        ["GLOBALEVENTID", "MentionIdentifier", "mention_ts"]
    )


def ingest_gkg_batch(spark, path: str):
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
    return dedup_and_append(spark, batch_df, target_table, ["GKGRECORDID"])


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
          f"({total_count - new_count} already existed)")
    spark.stop()

if __name__ == "__main__":
    main()