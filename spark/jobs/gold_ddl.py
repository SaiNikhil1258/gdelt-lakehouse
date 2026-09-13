def create_gold_namespace(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.gold")


def create_gold_bilateral_table(spark):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.india_bilateral_sentiment_daily (
            event_date DATE,
            partner_country STRING,
            event_count BIGINT,
            avg_goldstein DOUBLE,
            avg_tone DOUBLE
        )
        USING iceberg
        PARTITIONED BY (months(event_date))
        TBLPROPERTIES ('format-version'='2')
    """)


def create_gold_state_conflict_table(spark):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.india_state_conflict_daily (
            event_date DATE,
            adm1_code STRING,
            conflict_severity STRING,
            event_count BIGINT,
            avg_goldstein DOUBLE
        )
        USING iceberg
        PARTITIONED BY (months(event_date))
        TBLPROPERTIES ('format-version'='2')
    """)


def create_gold_mention_counts_table(spark):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.gold.india_event_mention_counts (
            GLOBALEVENTID BIGINT,
            total_mentions BIGINT,
            total_sources BIGINT,
            running_avg_tone DOUBLE,
            last_updated_ts TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (bucket(16, GLOBALEVENTID))
        TBLPROPERTIES ('format-version'='2')
    """)


def create_all_gold_tables(spark):
    create_gold_namespace(spark)
    create_gold_bilateral_table(spark)
    create_gold_state_conflict_table(spark)
    create_gold_mention_counts_table(spark)