from pyspark.sql import SparkSession, functions as F
from utils import normalize_url


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
        .withColumn(
            "conflict_severity",
            F.when(F.col("QuadClass") == 4, "Material Conflict")
             .when(F.col("QuadClass") == 3, "Verbal Conflict")
             .when(F.col("QuadClass") == 2, "Material Cooperation")
             .otherwise("Verbal Cooperation")
        )
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
    bronze_mentions = spark.table("local.bronze.mentions")
    bronze_gkg = spark.table("local.bronze.gkg")
    silver_events = spark.table("local.silver.events_india").select("GLOBALEVENTID", "event_date")

    silver_mentions = (
        bronze_mentions.alias("m")
        .join(
            bronze_gkg.alias("g"),
            normalize_url(F.col("m.MentionIdentifier")) == normalize_url(F.col("g.DocumentIdentifier")),
            "left"
        )
        .join(
            silver_events.alias("e"),
            F.col("m.GLOBALEVENTID") == F.col("e.GLOBALEVENTID"),
            "inner"   # scope to India-relevant events only
        )
        .select(
            F.col("m.GLOBALEVENTID"), F.col("e.event_date"),
            F.col("m.MentionSourceName"), F.col("m.MentionIdentifier"),
            F.col("m.MentionDocTone"), F.col("m.Confidence"),
            F.col("g.V2Themes"), F.col("g.V2Tone")
        )
    )

    row_count = silver_mentions.count()
    silver_mentions.writeTo("local.silver.mentions_enriched").overwritePartitions()
    return row_count


def main():
    spark = SparkSession.builder.appName("silver-transform").getOrCreate()
    events_count = transform_events_india(spark)
    print(f"silver.events_india: {events_count} rows written")
    mentions_count = transform_mentions_enriched(spark)
    print(f"silver.mentions_enriched: {mentions_count} rows written")
    spark.stop()

if __name__ == "__main__":
    main()