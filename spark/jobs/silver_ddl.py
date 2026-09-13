def create_silver_namespace(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")


def create_silver_events_india_table(spark):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.silver.events_india (
            GLOBALEVENTID BIGINT,
            event_date DATE,
            Actor1CountryCode STRING,
            Actor1Name STRING,
            Actor2CountryCode STRING,
            Actor2Name STRING,
            EventRootCode STRING,
            QuadClass INT,
            conflict_severity STRING,
            GoldsteinScale DOUBLE,
            AvgTone DOUBLE,
            ActionGeo_CountryCode STRING,
            ActionGeo_ADM1Code STRING,
            ActionGeo_Lat DOUBLE,
            ActionGeo_Long DOUBLE,
            SOURCEURL STRING
        )
        USING iceberg
        PARTITIONED BY (days(event_date))
        TBLPROPERTIES ('format-version'='2')
    """)
    spark.sql("ALTER TABLE local.silver.events_india WRITE ORDERED BY (ActionGeo_ADM1Code, event_date)")


def create_silver_mentions_enriched_table(spark):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.silver.mentions_enriched (
            GLOBALEVENTID BIGINT,
            event_date DATE,
            MentionSourceName STRING,
            MentionIdentifier STRING,
            MentionDocTone DOUBLE,
            Confidence INT,
            V2Themes STRING,
            V2Tone STRING
        )
        USING iceberg
        PARTITIONED BY (days(event_date))
        TBLPROPERTIES ('format-version'='2')
    """)


def create_all_silver_tables(spark):
    create_silver_namespace(spark)
    create_silver_events_india_table(spark)
    create_silver_mentions_enriched_table(spark)