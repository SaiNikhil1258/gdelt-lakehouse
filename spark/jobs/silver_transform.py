from pyspark.sql import SparkSession, functions as F
from utils import normalize_url, conflict_severity


def transform_events_india(spark):
    bronze_events = spark.table("local.bronze.events")

    silver_events = (
        bronze_events
        .withColumn("event_date", F.to_date("SQLDATE", "yyyyMMdd"))
        .filter(
            (F.col("ActionGeo_CountryCode") == "IN") |
            (F.col("Actor1CountryCode") == "IND") |
            (F.col("Actor2CountryCode") == "IND")
        )
        .filter(F.col("SOURCEURL").isNotNull() & F.col("GLOBALEVENTID").isNotNull())
        .withColumn("conflict_severity", conflict_severity(F.col("QuadClass")))
        .select(
            "GLOBALEVENTID", "event_date", "Actor1CountryCode", "Actor1Name",
            "Actor2CountryCode", "Actor2Name", "EventRootCode", "QuadClass",
            "conflict_severity", "GoldsteinScale", "AvgTone",
            "ActionGeo_CountryCode", "ActionGeo_ADM1Code", "ActionGeo_Lat", "ActionGeo_Long",
            "SOURCEURL"
        )
        .dropDuplicates(["GLOBALEVENTID"])  # defensive; Bronze should already guarantee this
    )

    row_count = silver_events.count()
    silver_events.writeTo("local.silver.events_india").overwritePartitions()
    return row_count


def transform_mentions_enriched(spark):
    """One row per (GLOBALEVENTID, MentionIdentifier), i.e. per article that mentions an
    India-relevant event.

    Design, each step backed by what we measured on September's Bronze data:
    1. Scope to India events FIRST (small, broadcast), then collapse to the article grain.
       Bronze has 2 rows for ~2% of (event, article) pairs. MentionSourceName,
       MentionDocTone and MentionDocLen were identical in all of them, so they are safe to
       collapse (min). Confidence differed in some pairs, so the highest value is kept.
       SentenceID / char offsets / InRawText are per-sentence detail that Silver does not carry.
    2. Pull only the GKG rows these articles can match, then keep ONE GKG row per
       normalized URL (the latest gkg_ts, ties broken by GKGRECORDID). About 1.6% of GKG rows
       share a normalized URL with another row; without this a LEFT JOIN would multiply mentions.
       This tie-break is a deterministic choice, not a verified 'best' row.
    3. LEFT JOIN, so mentions without a GKG record (e.g. during a GKG stall) are kept.
    """
    bronze_mentions = spark.table("local.bronze.mentions")
    bronze_gkg = spark.table("local.bronze.gkg")
    silver_events = spark.table("local.silver.events_india").select("GLOBALEVENTID", "event_date")

    pairs = (
        bronze_mentions.alias("m")
        .join(
            F.broadcast(silver_events.alias("e")),
            F.col("m.GLOBALEVENTID") == F.col("e.GLOBALEVENTID"),
            "inner",   # scope to India-relevant events only
        )
        .groupBy(
            F.col("m.GLOBALEVENTID").alias("GLOBALEVENTID"),
            F.col("m.MentionIdentifier").alias("MentionIdentifier"),
        )
        .agg(
            F.min("e.event_date").alias("event_date"),
            F.min("m.MentionSourceName").alias("MentionSourceName"),
            F.min("m.MentionDocTone").alias("MentionDocTone"),
            F.max("m.Confidence").alias("Confidence"),
        )
        .withColumn("norm_url", normalize_url(F.col("MentionIdentifier")))
    )

    gkg_one_per_url = (
        bronze_gkg
        .filter(F.col("DocumentIdentifier").isNotNull())   # also drops malformed fragment rows
        .withColumn("norm_url", normalize_url(F.col("DocumentIdentifier")))
        .join(pairs.select("norm_url").distinct(), "norm_url", "left_semi")
        .groupBy("norm_url")
        .agg(F.max(F.struct("gkg_ts", "GKGRECORDID", "V2Themes", "V2Tone")).alias("g"))
        .select(
            "norm_url",
            F.col("g.V2Themes").alias("V2Themes"),
            F.col("g.V2Tone").alias("V2Tone"),
        )
    )

    silver_mentions = (
        pairs.join(gkg_one_per_url, "norm_url", "left")
        .select(
            "GLOBALEVENTID", "event_date", "MentionSourceName", "MentionIdentifier",
            "MentionDocTone", "Confidence", "V2Themes", "V2Tone",
        )
    )

    row_count = silver_mentions.count()
    silver_mentions.writeTo("local.silver.mentions_enriched").overwritePartitions()
    return row_count


def main():
    spark = (
        SparkSession.builder.appName("silver-transform")
        .config("spark.executor.memory", "4g")   # default is 1g; the worker has 6g
        .getOrCreate()
    )
    events_count = transform_events_india(spark)
    print(f"silver.events_india: {events_count} rows written")
    mentions_count = transform_mentions_enriched(spark)
    print(f"silver.mentions_enriched: {mentions_count} rows written")
    spark.stop()


if __name__ == "__main__":
    main()