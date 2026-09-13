from pyspark.sql import SparkSession, functions as F

def main():
    spark = SparkSession.builder.appName("verify-gold").getOrCreate()

    bilateral = spark.table("local.gold.india_bilateral_sentiment_daily")
    print(f"bilateral_sentiment_daily: total={bilateral.count()}")

    state = spark.table("local.gold.india_state_conflict_daily")
    print(f"state_conflict_daily: total={state.count()}")

    mention_counts = spark.table("local.gold.india_event_mention_counts")
    m_total = mention_counts.count()
    m_distinct = mention_counts.select("GLOBALEVENTID").distinct().count()
    status = "OK" if m_total == m_distinct else "PROBLEM"
    print(f"event_mention_counts: total={m_total} distinct_ids={m_distinct} -> {status}")

    # Cross-check: every India-relevant event with at least one mention
    # should have exactly one row in the Gold counter table.
    silver_event_ids = spark.table("local.silver.mentions_enriched").select("GLOBALEVENTID").distinct().count()
    print(f"silver.mentions_enriched distinct GLOBALEVENTID: {silver_event_ids} (should equal event_mention_counts total)")

    spark.stop()

if __name__ == "__main__":
    main()