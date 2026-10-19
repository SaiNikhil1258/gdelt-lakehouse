"""CI only: check that the pipeline, run on the synthetic sample from
scripts/make_ci_sample.py, produced exactly the expected tables.

verify_all.py proves the layers agree with each other; this proves they agree with the
rules we chose. Every number below follows from the rows in make_ci_sample.py.
Exits 1 if anything differs.
"""
import sys

from pyspark.sql import SparkSession

T = "local"
CHECKS = [
    # name, SQL returning one value, expected
    ("Bronze: events rows",   f"SELECT count(*) FROM {T}.bronze.events", 9),
    ("Bronze: mentions rows", f"SELECT count(*) FROM {T}.bronze.mentions", 11),
    ("Bronze: gkg rows",      f"SELECT count(*) FROM {T}.bronze.gkg", 9),

    # events 1-4 and 6-8; event 5 has no India link, event 9 has no source URL
    ("Silver: India events", f"SELECT count(*) FROM {T}.silver.events_india", 7),
    # one row per (event, article); event 1 has two articles, event 8's duplicate collapses
    ("Silver: mention rows", f"SELECT count(*) FROM {T}.silver.mentions_enriched", 8),
    ("Silver: latest GKG record wins",
     f"SELECT max(V2Themes) FROM {T}.silver.mentions_enriched "
     "WHERE MentionIdentifier = 'http://news.example/a1'", "T1_NEW"),
    ("Silver: tracking parameter still matches GKG",
     f"SELECT max(V2Themes) FROM {T}.silver.mentions_enriched "
     "WHERE MentionIdentifier = 'http://news.example/a1?utm_source=tw'", "T1_NEW"),
    ("Silver: ?id=3 keeps its own GKG record",
     f"SELECT max(V2Themes) FROM {T}.silver.mentions_enriched "
     "WHERE MentionIdentifier = 'http://news.example/story.php?id=3'", "T3"),
    ("Silver: ?id=4 keeps its own GKG record",
     f"SELECT max(V2Themes) FROM {T}.silver.mentions_enriched "
     "WHERE MentionIdentifier = 'http://news.example/story.php?id=4'", "T4"),

    # India must be exactly one actor: events 1, 2, 8 (USA) and 6 (PAK)
    ("Gold bilateral: USA events",
     f"SELECT sum(event_count) FROM {T}.gold.india_bilateral_sentiment_daily "
     "WHERE partner_country = 'USA'", 3),
    ("Gold bilateral: PAK events",
     f"SELECT sum(event_count) FROM {T}.gold.india_bilateral_sentiment_daily "
     "WHERE partner_country = 'PAK'", 1),
    ("Gold bilateral: partner countries",
     f"SELECT count(DISTINCT partner_country) FROM {T}.gold.india_bilateral_sentiment_daily", 2),

    # events that happened in India with a known state: 1, 3, 4, 8
    ("Gold state conflict: events",
     f"SELECT sum(event_count) FROM {T}.gold.india_state_conflict_daily", 4),
    ("Gold state conflict: rows",
     f"SELECT count(*) FROM {T}.gold.india_state_conflict_daily", 4),
    ("Gold state conflict: only Indian states",
     f"SELECT count(*) FROM {T}.gold.india_state_conflict_daily WHERE adm1_code NOT LIKE 'IN%'", 0),

    ("Gold mention counts: rows",
     f"SELECT count(*) FROM {T}.gold.india_event_mention_counts", 7),
    ("Gold mention counts: event 1 mentions",
     f"SELECT total_mentions FROM {T}.gold.india_event_mention_counts WHERE GLOBALEVENTID = 1", 2),
    ("Gold mention counts: event 1 sources",
     f"SELECT total_sources FROM {T}.gold.india_event_mention_counts WHERE GLOBALEVENTID = 1", 2),
]


def main():
    spark = SparkSession.builder.appName("ci-sample-assertions").getOrCreate()
    failed = []
    for name, sql, expected in CHECKS:
        row = spark.sql(sql).first()
        actual = row[0] if row is not None else None
        ok = actual == expected
        print(f"{'PASS' if ok else 'FAIL'}  {name}  -- expected={expected!r} actual={actual!r}", flush=True)
        if not ok:
            failed.append(name)
    print(f"SUMMARY: {len(CHECKS) - len(failed)} passed, {len(failed)} failed")
    spark.stop()
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()