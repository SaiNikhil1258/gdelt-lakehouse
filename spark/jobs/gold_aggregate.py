from pyspark.sql import SparkSession, functions as F


def build_bilateral_sentiment(spark):
    events = spark.table("local.silver.events_india")

    gold = (
        events
        .withColumn(
            "partner_country",
            F.when(F.col("Actor1CountryCode") == "IND", F.col("Actor2CountryCode"))
             .otherwise(F.col("Actor1CountryCode"))
        )
        .filter(
            F.col("partner_country").isNotNull() &
            (F.col("partner_country") != "IND") &
            (F.col("partner_country") != "")
        )
        .groupBy("event_date", "partner_country")
        .agg(
            F.count("*").alias("event_count"),
            F.avg("GoldsteinScale").alias("avg_goldstein"),
            F.avg("AvgTone").alias("avg_tone"),
        )
    )

    row_count = gold.count()
    gold.writeTo("local.gold.india_bilateral_sentiment_daily").overwritePartitions()
    return row_count


def build_state_conflict(spark):
    events = spark.table("local.silver.events_india")

    gold = (
        events
        .filter(F.col("ActionGeo_ADM1Code").isNotNull())
        .groupBy("event_date", "ActionGeo_ADM1Code", "conflict_severity")
        .agg(
            F.count("*").alias("event_count"),
            F.avg("GoldsteinScale").alias("avg_goldstein"),
        )
        .withColumnRenamed("ActionGeo_ADM1Code", "adm1_code")
    )

    row_count = gold.count()
    gold.writeTo("local.gold.india_state_conflict_daily").overwritePartitions()
    return row_count


def merge_mention_counts(spark):
    """Upsert against the FULL current total for each event -- not an additive
    delta. See discussion above: Silver is a full recompute, not incremental,
    so this MERGE must replace each event's totals rather than add to them,
    or re-running the pipeline would double-count on every run."""
    mentions = spark.table("local.silver.mentions_enriched")

    agg = (
        mentions
        .groupBy("GLOBALEVENTID")
        .agg(
            F.count("*").alias("total_mentions"),
            F.countDistinct("MentionSourceName").alias("total_sources"),
            F.avg("MentionDocTone").alias("running_avg_tone"),
        )
    )
    agg.createOrReplaceTempView("mention_agg_source")

    spark.sql("""
        MERGE INTO local.gold.india_event_mention_counts t
        USING mention_agg_source s
        ON t.GLOBALEVENTID = s.GLOBALEVENTID
        WHEN MATCHED THEN UPDATE SET
            t.total_mentions    = s.total_mentions,
            t.total_sources     = s.total_sources,
            t.running_avg_tone  = s.running_avg_tone,
            t.last_updated_ts   = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
            GLOBALEVENTID, total_mentions, total_sources, running_avg_tone, last_updated_ts
        ) VALUES (
            s.GLOBALEVENTID, s.total_mentions, s.total_sources, s.running_avg_tone, current_timestamp()
        )
    """)

    return agg.count()


def main():
    spark = SparkSession.builder.appName("gold-aggregate").getOrCreate()

    b = build_bilateral_sentiment(spark)
    print(f"gold.india_bilateral_sentiment_daily: {b} rows written")

    s = build_state_conflict(spark)
    print(f"gold.india_state_conflict_daily: {s} rows written")

    m = merge_mention_counts(spark)
    print(f"gold.india_event_mention_counts: merged against {m} distinct events")

    spark.stop()

if __name__ == "__main__":
    main()