"""Deliberately damage one table so the acceptance checks can be tested, and undo it exactly.

Every scenario first prints 'SNAP <table> <snapshot_id>' for each table it will touch (the
table's CURRENT snapshot, taken from the 'main' ref), then makes the change. --restore rolls
each table back to its snapshot with Iceberg's rollback_to_snapshot procedure.

    --scenario dup_event | missing_slot | gold_drift | gkg_stall
    --restore  bronze.events=123,silver.mentions_enriched=456

Never run this while the live pipeline is writing to the same tables.
"""
import argparse

from pyspark.sql import SparkSession

SLOT_TO_DELETE = "2026-09-10 12:00:00"


def snap(spark, table):
    row = spark.sql(f"SELECT snapshot_id FROM local.{table}.refs WHERE name = 'main'").first()
    if row is None:
        raise RuntimeError(f"no 'main' snapshot found for {table}")
    print(f"SNAP {table} {row['snapshot_id']}", flush=True)


def dup_event(spark):
    snap(spark, "bronze.events")
    spark.table("local.bronze.events").limit(1).writeTo("local.bronze.events").append()
    print("APPENDED 1 duplicate row to bronze.events", flush=True)


def missing_slot(spark):
    snap(spark, "bronze.mentions")
    n = spark.table("local.bronze.mentions").filter(f"mention_ts = TIMESTAMP '{SLOT_TO_DELETE}'").count()
    spark.sql(f"DELETE FROM local.bronze.mentions WHERE mention_ts = TIMESTAMP '{SLOT_TO_DELETE}'")
    print(f"DELETED {n} rows (slot {SLOT_TO_DELETE}) from bronze.mentions", flush=True)


def gold_drift(spark):
    snap(spark, "gold.india_bilateral_sentiment_daily")
    spark.sql("""
        INSERT INTO local.gold.india_bilateral_sentiment_daily
        VALUES (DATE '2026-09-15', 'ZZZ', CAST(5 AS BIGINT), CAST(0.0 AS DOUBLE), CAST(0.0 AS DOUBLE))
    """)
    print("APPENDED 1 bogus row to gold.india_bilateral_sentiment_daily", flush=True)


def gkg_stall(spark):
    snap(spark, "silver.mentions_enriched")
    spark.sql("""
        UPDATE local.silver.mentions_enriched
        SET V2Themes = NULL, V2Tone = NULL
        WHERE V2Themes IS NOT NULL
    """)
    print("BLANKED the GKG columns of silver.mentions_enriched", flush=True)


SCENARIOS = {
    "dup_event": dup_event,
    "missing_slot": missing_slot,
    "gold_drift": gold_drift,
    "gkg_stall": gkg_stall,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", choices=list(SCENARIOS))
    parser.add_argument("--restore", help="comma-separated table=snapshot_id pairs")
    args = parser.parse_args()
    if bool(args.scenario) == bool(args.restore):
        parser.error("give exactly one of --scenario or --restore")

    spark = SparkSession.builder.appName("inject-fault").getOrCreate()
    try:
        if args.scenario:
            SCENARIOS[args.scenario](spark)
            print(f"INJECTED {args.scenario}", flush=True)
        else:
            for pair in args.restore.split(","):
                table, snapshot_id = pair.split("=")
                spark.sql(f"CALL local.system.rollback_to_snapshot('{table}', {int(snapshot_id)})")
                print(f"RESTORED {table} to snapshot {snapshot_id}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()