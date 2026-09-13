from pyspark.sql import SparkSession, functions as F

def main():
    spark = SparkSession.builder.appName("verify-silver").getOrCreate()

    events = spark.table("local.silver.events_india")
    total = events.count()
    distinct_ids = events.select("GLOBALEVENTID").distinct().count()

    valid_india = (
        (F.col("ActionGeo_CountryCode") == "IN") |
        (F.col("Actor1CountryCode") == "IND") |
        (F.col("Actor2CountryCode") == "IND")
    )
    non_india = events.filter(~valid_india).count()

    status = "OK" if (total == distinct_ids and non_india == 0) else "PROBLEM"
    print(f"events_india: total={total} distinct_ids={distinct_ids} non_india_rows={non_india} -> {status}")

    mentions = spark.table("local.silver.mentions_enriched")
    m_total = mentions.count()
    matched = mentions.filter(F.col("V2Themes").isNotNull()).count()
    match_rate = (matched / m_total) if m_total else 0
    print(f"mentions_enriched: total={m_total} gkg_matched={matched} match_rate={match_rate:.1%}")

    spark.stop()

if __name__ == "__main__":
    main()