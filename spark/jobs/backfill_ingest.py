"""Bulk Bronze backfill: ONE read, ONE dedup, ONE append per day per stream.

Why this replaces the per-slot loop: dedup_and_append scans the existing keys of the
whole target table on every write. Per slot that is 288 scans a day, each bigger than
the last. Per day it is 3.

Behaviour:
  - A day is read with a glob (e.g. s3a://gdelt-landing/events/20260908*.csv). Slots
    GDELT never published simply have no file, so gaps need no special handling.
  - Completeness check: for each day/stream, the number of distinct 15-minute slots in
    Bronze must equal the number of slot files in the landing bucket. A day that already
    matches is skipped (so re-running a whole month is cheap); --no-skip forces a re-run,
    which is safe because the anti-join makes ingestion idempotent.
  - Real errors are NOT swallowed: any exception stops the run with a non-zero exit code.
    Re-running resumes, because finished days are skipped.
  - A day whose slot count still mismatches after ingest is reported as MISMATCH and the
    script exits 1 at the end.

Assumption: the same key never appears in two different slots of one day. (Per-slot
ingestion of Sept 1 found none.) If it did, both rows would be appended and
verify_bronze.py would flag the table.
"""
import argparse
import sys
import time
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

from bronze_ingest import (
    ingest_events_batch,
    ingest_mentions_batch,
    ingest_gkg_batch,
)

LANDING = "s3a://gdelt-landing"

STREAMS = {
    "events": {"fn": ingest_events_batch, "table": "local.bronze.events", "ts_col": "date_added_ts"},
    "mentions": {"fn": ingest_mentions_batch, "table": "local.bronze.mentions", "ts_col": "mention_ts"},
    "gkg": {"fn": ingest_gkg_batch, "table": "local.bronze.gkg", "ts_col": "gkg_ts"},
}
VALID_STREAMS = list(STREAMS)


def date_range(start_date: str, end_date: str):
    """Every date from start_date to end_date inclusive, as YYYYMMDD strings."""
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    if start > end:
        raise ValueError("--start-date cannot be after --end-date")
    current = start
    while current <= end:
        yield current.strftime("%Y%m%d")
        current += timedelta(days=1)


def landing_slots(spark, stream: str, day: str) -> int:
    """Number of slot files for this day present in the landing bucket."""
    path = spark._jvm.org.apache.hadoop.fs.Path(f"{LANDING}/{stream}/{day}*.csv")
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    matches = fs.globStatus(path)
    return 0 if matches is None else len(matches)


def bronze_slots(spark, stream: str, day: str) -> int:
    """Number of distinct 15-minute slot timestamps in Bronze for this day."""
    cfg = STREAMS[stream]
    ts = cfg["ts_col"]
    start = datetime.strptime(day, "%Y%m%d")
    end = start + timedelta(days=1)
    row = spark.sql(f"""
        SELECT COUNT(DISTINCT {ts}) AS n
        FROM {cfg['table']}
        WHERE {ts} >= TIMESTAMP '{start:%Y-%m-%d %H:%M:%S}'
          AND {ts} <  TIMESTAMP '{end:%Y-%m-%d %H:%M:%S}'
    """).first()
    return row["n"]


def main():
    parser = argparse.ArgumentParser(
        description="Bulk-backfill GDELT landing files into the Bronze Iceberg tables."
    )
    parser.add_argument("--date", help="Process a single date, YYYYMMDD.")
    parser.add_argument("--start-date", help="First date, YYYYMMDD.")
    parser.add_argument("--end-date", help="Last date, YYYYMMDD, inclusive.")
    parser.add_argument(
        "--streams", nargs="+", default=VALID_STREAMS, choices=VALID_STREAMS,
        help="Streams to ingest. Default: events mentions gkg",
    )
    parser.add_argument(
        "--no-skip", action="store_true",
        help="Re-run days that already look complete (still idempotent).",
    )
    args = parser.parse_args()

    if args.date and (args.start_date or args.end_date):
        parser.error("Use either --date OR --start-date/--end-date, not both.")
    if args.date:
        try:
            datetime.strptime(args.date, "%Y%m%d")
        except ValueError:
            parser.error(f"Invalid --date '{args.date}'. Expected YYYYMMDD.")
        dates = [args.date]
    else:
        if not args.start_date or not args.end_date:
            parser.error("Provide either --date or both --start-date and --end-date.")
        try:
            dates = list(date_range(args.start_date, args.end_date))
        except ValueError as e:
            parser.error(str(e))

    print("=" * 72)
    print("GDELT BULK BACKFILL (one append per day per stream)")
    print("=" * 72)
    print(f"Days:    {len(dates)} ({dates[0]} -> {dates[-1]})")
    print(f"Streams: {', '.join(args.streams)}")
    print(f"Skip complete days: {not args.no_skip}")
    print("=" * 72)

    spark = (
        SparkSession.builder.appName("gdelt-backfill-ingest")
        .config("spark.executor.memory", "4g")   # default is 1g; the worker has 6g
        .getOrCreate()
    )

    totals = {
        s: {"new": 0, "read": 0, "ingested": 0, "skipped": 0, "nofiles": 0, "mismatch": 0}
        for s in args.streams
    }
    started = time.perf_counter()

    try:
        for day_index, day in enumerate(dates, start=1):
            print()
            print(f"--- DAY {day_index}/{len(dates)}: {day} ---")
            for stream in args.streams:
                cfg = STREAMS[stream]
                files = landing_slots(spark, stream, day)
                if files == 0:
                    print(f"{day} {stream}: NO LANDING FILES (nothing to ingest)")
                    totals[stream]["nofiles"] += 1
                    continue

                have = bronze_slots(spark, stream, day)
                if have == files and not args.no_skip:
                    print(f"{day} {stream}: SKIP (already complete, {have}/{files} slots)")
                    totals[stream]["skipped"] += 1
                    continue

                t0 = time.perf_counter()
                new_rows, read_rows = cfg["fn"](
                    spark, f"{LANDING}/{stream}/{day}*.csv", cache=True
                )
                have_after = bronze_slots(spark, stream, day)
                status = "OK" if have_after == files else "MISMATCH"
                elapsed = time.perf_counter() - t0

                totals[stream]["new"] += new_rows
                totals[stream]["read"] += read_rows
                totals[stream]["ingested"] += 1
                if status == "MISMATCH":
                    totals[stream]["mismatch"] += 1

                print(
                    f"{day} {stream}: +{new_rows} new rows ({read_rows} read), "
                    f"slots {have_after}/{files} {status} ({elapsed:.0f}s)"
                )
    finally:
        spark.stop()

    print()
    print("=" * 72)
    print(f"BULK BACKFILL SUMMARY ({time.perf_counter() - started:.0f}s total)")
    print("=" * 72)
    for stream, t in totals.items():
        print(
            f"{stream}: {t['ingested']} days ingested, {t['skipped']} skipped, "
            f"{t['nofiles']} without files, {t['new']} new rows "
            f"({t['read']} read), {t['mismatch']} slot MISMATCH"
        )
    print("=" * 72)

    if any(t["mismatch"] for t in totals.values()):
        print("At least one day finished with a slot-count MISMATCH. See lines above.")
        sys.exit(1)


if __name__ == "__main__":
    main()