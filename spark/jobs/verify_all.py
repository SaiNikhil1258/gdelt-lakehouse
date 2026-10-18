"""Acceptance checks for the lakehouse over a date range.

Prints one PASS/FAIL line per check and exits 1 if anything fails, so the same script can
gate a backfill, a CI run or a monitoring job. Read-only.

Sections:
  COMPLETENESS    every day has the expected 15-minute slots; Bronze slots == landing files
  INTEGRITY       unique keys, no null keys/timestamps, malformed GKG rows within tolerance,
                  value ranges
  RECONCILIATION  Bronze -> Silver -> Gold tie out exactly
  QUALITY         GKG match rate above a threshold

Not included yet: comparing raw landing-file line counts with Bronze row counts (needs a
read of every CSV; will be an opt-in --deep check once the checks below are proven).
"""
import argparse
import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, functions as F

from bronze_ingest import MENTIONS_KEY

LANDING = "s3a://gdelt-landing"

BRONZE = {
    "events": {"table": "local.bronze.events", "ts": "date_added_ts", "key": ["GLOBALEVENTID"]},
    "mentions": {"table": "local.bronze.mentions", "ts": "mention_ts", "key": MENTIONS_KEY},
    "gkg": {"table": "local.bronze.gkg", "ts": "gkg_ts", "key": ["GKGRECORDID"]},
}


class Report:
    def __init__(self):
        self.results = []

    def check(self, name: str, ok: bool, detail: str = ""):
        self.results.append((name, ok))
        print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  -- {detail}" if detail else ""), flush=True)


def india_condition():
    return (
        (F.col("ActionGeo_CountryCode") == "IN")
        | (F.col("Actor1CountryCode") == "IND")
        | (F.col("Actor2CountryCode") == "IND")
    )


def bronze_slots_by_day(spark, table: str, ts_col: str, start_ts: str, end_ts: str) -> dict:
    rows = spark.sql(f"""
        SELECT date({ts_col}) AS day, COUNT(DISTINCT {ts_col}) AS slots
        FROM {table}
        WHERE {ts_col} >= TIMESTAMP '{start_ts}' AND {ts_col} < TIMESTAMP '{end_ts}'
        GROUP BY 1
    """).collect()
    return {r["day"].strftime("%Y%m%d"): r["slots"] for r in rows}


def landing_files_by_day(spark, stream: str) -> dict:
    path = spark._jvm.org.apache.hadoop.fs.Path(f"{LANDING}/{stream}/")
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    counts = {}
    for status in fs.listStatus(path):
        name = status.getPath().getName()          # e.g. 20260901000000.csv
        if len(name) >= 8 and name[:8].isdigit():
            counts[name[:8]] = counts.get(name[:8], 0) + 1
    return counts


def main():
    parser = argparse.ArgumentParser(description="Lakehouse acceptance checks.")
    parser.add_argument("--start-date", default="20260901", help="YYYYMMDD, inclusive")
    parser.add_argument("--end-date", default="20260930", help="YYYYMMDD, inclusive")
    parser.add_argument("--slots-per-day", type=int, default=96)
    parser.add_argument("--max-missing-slots", type=int, default=0,
                        help="Total missing slots tolerated per stream (GDELT-side gaps).")
    parser.add_argument("--max-malformed-gkg", type=int, default=10,
                        help="Tolerated GKG rows with a malformed record id / NULL timestamp.")
    parser.add_argument("--min-match-rate", type=float, default=0.95,
                        help="Minimum share of Silver mentions that found a GKG record.")
    args = parser.parse_args()

    start = datetime.strptime(args.start_date, "%Y%m%d")
    end = datetime.strptime(args.end_date, "%Y%m%d")
    if start > end:
        parser.error("--start-date cannot be after --end-date")
    days = []
    d = start
    while d <= end:
        days.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    start_ts = start.strftime("%Y-%m-%d %H:%M:%S")
    end_ts = (end + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")

    spark = (
        SparkSession.builder.appName("verify-all")
        .config("spark.executor.memory", "4g")
        .getOrCreate()
    )
    report = Report()

    print(f"=== COMPLETENESS ({args.start_date} -> {args.end_date}, {len(days)} days) ===")
    for stream, cfg in BRONZE.items():
        by_day = bronze_slots_by_day(spark, cfg["table"], cfg["ts"], start_ts, end_ts)
        short = {x: by_day.get(x, 0) for x in days if by_day.get(x, 0) < args.slots_per_day}
        extra = {x: by_day[x] for x in days if by_day.get(x, 0) > args.slots_per_day}
        missing = sum(args.slots_per_day - n for n in short.values())
        ok = missing <= args.max_missing_slots and not extra
        detail = f"{len(days) - len(short)}/{len(days)} full days, {missing} slots missing"
        if short:
            detail += f", short days (first 5): {list(short.items())[:5]}"
        if extra:
            detail += f", days with too many slots: {list(extra.items())[:5]}"
        report.check(f"{stream}: every day has {args.slots_per_day} slots", ok, detail)

        landing = landing_files_by_day(spark, stream)
        diff = {x: (landing.get(x, 0), by_day.get(x, 0)) for x in days
                if landing.get(x, 0) != by_day.get(x, 0)}
        detail = "all days match" if not diff else \
            f"{len(diff)} days differ (day: (landing, bronze)), first 5: {list(diff.items())[:5]}"
        report.check(f"{stream}: landing files == Bronze slots per day", not diff, detail)

    print("=== INTEGRITY ===")
    for stream, cfg in BRONZE.items():
        df = spark.table(cfg["table"])
        total = df.count()
        distinct = df.select(*cfg["key"]).distinct().count()
        report.check(f"{stream}: key is unique", total == distinct,
                     f"total={total} distinct={distinct}")
        nulls = df.filter(" OR ".join(f"{c} IS NULL" for c in cfg["key"])).count()
        report.check(f"{stream}: no NULL key columns", nulls == 0, f"null_key_rows={nulls}")

    for stream in ("events", "mentions"):
        cfg = BRONZE[stream]
        n = spark.table(cfg["table"]).filter(F.col(cfg["ts"]).isNull()).count()
        report.check(f"{stream}: no NULL {cfg['ts']}", n == 0, f"null_rows={n}")

    gkg = spark.table(BRONZE["gkg"]["table"])
    null_ts = gkg.filter(F.col("gkg_ts").isNull()).count()
    report.check("gkg: NULL gkg_ts within tolerance", null_ts <= args.max_malformed_gkg,
                 f"null_ts_rows={null_ts}, tolerance={args.max_malformed_gkg}")
    malformed = gkg.filter(~F.col("GKGRECORDID").rlike("^[0-9]{14}-")).count()
    report.check("gkg: malformed GKGRECORDID within tolerance", malformed <= args.max_malformed_gkg,
                 f"malformed_ids={malformed}, tolerance={args.max_malformed_gkg} "
                 f"(assumes ids look like 14 digits + '-')")

    out_of_range = spark.table(BRONZE["events"]["table"]).filter(
        (F.col("GoldsteinScale") < -10) | (F.col("GoldsteinScale") > 10)
        | (~F.col("QuadClass").isin(1, 2, 3, 4))
        | (F.abs(F.col("AvgTone")) > 100)
        | (F.col("ActionGeo_Lat") < -90) | (F.col("ActionGeo_Lat") > 90)
        | (F.col("ActionGeo_Long") < -180) | (F.col("ActionGeo_Long") > 180)
    ).count()
    report.check("events: GoldsteinScale/QuadClass/AvgTone/lat/long in range", out_of_range == 0,
                 f"out_of_range_rows={out_of_range}")

    print("=== RECONCILIATION ===")
    bronze_india_ids = (
        spark.table("local.bronze.events")
        .filter(india_condition())
        .filter(F.col("SOURCEURL").isNotNull() & F.col("GLOBALEVENTID").isNotNull())
        .select("GLOBALEVENTID").distinct()
    )
    silver_events = spark.table("local.silver.events_india")
    silver_mentions = spark.table("local.silver.mentions_enriched")

    expected = bronze_india_ids.count()
    actual = silver_events.count()
    report.check("Bronze -> Silver: events_india rows == India events in Bronze", expected == actual,
                 f"bronze_india_events={expected} silver={actual}")

    expected_pairs = (
        spark.table("local.bronze.mentions")
        .join(F.broadcast(bronze_india_ids), "GLOBALEVENTID", "inner")
        .select("GLOBALEVENTID", "MentionIdentifier").distinct().count()
    )
    actual = silver_mentions.count()
    report.check("Bronze -> Silver: mentions_enriched rows == distinct (event, article) pairs",
                 expected_pairs == actual, f"bronze_pairs={expected_pairs} silver={actual}")

    orphans = silver_mentions.join(silver_events.select("GLOBALEVENTID"), "GLOBALEVENTID", "left_anti").count()
    report.check("Silver: every mention belongs to a Silver event", orphans == 0,
                 f"orphan_mentions={orphans}")

    partner = F.when(F.col("Actor1CountryCode") == "IND", F.col("Actor2CountryCode")) \
               .otherwise(F.col("Actor1CountryCode"))
    expected = silver_events.filter(partner.isNotNull() & (partner != "IND") & (partner != "")).count()
    actual = spark.table("local.gold.india_bilateral_sentiment_daily") \
        .agg(F.sum("event_count")).first()[0] or 0
    report.check("Silver -> Gold: bilateral event_count sums to the Silver rows it covers",
                 expected == actual, f"silver_rows={expected} gold_sum={actual}")

    expected = silver_events.filter(F.col("ActionGeo_ADM1Code").isNotNull()).count()
    actual = spark.table("local.gold.india_state_conflict_daily") \
        .agg(F.sum("event_count")).first()[0] or 0
    report.check("Silver -> Gold: state-conflict event_count sums to the Silver rows it covers",
                 expected == actual, f"silver_rows={expected} gold_sum={actual}")

    gold_mentions = spark.table("local.gold.india_event_mention_counts")
    expected = silver_mentions.count()
    actual = gold_mentions.agg(F.sum("total_mentions")).first()[0] or 0
    report.check("Silver -> Gold: total_mentions sums to Silver mention rows", expected == actual,
                 f"silver_rows={expected} gold_sum={actual}")

    expected = silver_mentions.select("GLOBALEVENTID").distinct().count()
    actual = gold_mentions.count()
    report.check("Silver -> Gold: one mention-count row per event", expected == actual,
                 f"silver_events_with_mentions={expected} gold_rows={actual}")

    print("=== QUALITY ===")
    total = silver_mentions.count()
    matched = silver_mentions.filter(F.col("V2Themes").isNotNull()).count()
    rate = matched / total if total else 0.0
    report.check("Silver: GKG match rate above threshold", rate >= args.min_match_rate,
                 f"match_rate={rate:.1%} threshold={args.min_match_rate:.0%} ({matched}/{total})")

    failed = [name for name, ok in report.results if not ok]
    print()
    print("=" * 60)
    print(f"SUMMARY: {len(report.results) - len(failed)} passed, {len(failed)} failed")
    for name in failed:
        print(f"  FAILED: {name}")
    print("=" * 60)

    spark.stop()
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()