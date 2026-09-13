def create_bronze_namespace(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.bronze")


def create_bronze_events_table(spark):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.bronze.events (
            GLOBALEVENTID BIGINT, SQLDATE STRING, MonthYear INT, Year INT, FractionDate DOUBLE,
            Actor1Code STRING, Actor1Name STRING, Actor1CountryCode STRING, Actor1KnownGroupCode STRING,
            Actor1EthnicCode STRING, Actor1Religion1Code STRING, Actor1Religion2Code STRING,
            Actor1Type1Code STRING, Actor1Type2Code STRING, Actor1Type3Code STRING,
            Actor2Code STRING, Actor2Name STRING, Actor2CountryCode STRING, Actor2KnownGroupCode STRING,
            Actor2EthnicCode STRING, Actor2Religion1Code STRING, Actor2Religion2Code STRING,
            Actor2Type1Code STRING, Actor2Type2Code STRING, Actor2Type3Code STRING,
            IsRootEvent INT, EventCode STRING, EventBaseCode STRING, EventRootCode STRING,
            QuadClass INT, GoldsteinScale DOUBLE, NumMentions INT, NumSources INT, NumArticles INT, AvgTone DOUBLE,
            Actor1Geo_Type INT, Actor1Geo_FullName STRING, Actor1Geo_CountryCode STRING,
            Actor1Geo_ADM1Code STRING, Actor1Geo_ADM2Code STRING, Actor1Geo_Lat DOUBLE, Actor1Geo_Long DOUBLE, Actor1Geo_FeatureID STRING,
            Actor2Geo_Type INT, Actor2Geo_FullName STRING, Actor2Geo_CountryCode STRING,
            Actor2Geo_ADM1Code STRING, Actor2Geo_ADM2Code STRING, Actor2Geo_Lat DOUBLE, Actor2Geo_Long DOUBLE, Actor2Geo_FeatureID STRING,
            ActionGeo_Type INT, ActionGeo_FullName STRING, ActionGeo_CountryCode STRING,
            ActionGeo_ADM1Code STRING, ActionGeo_ADM2Code STRING, ActionGeo_Lat DOUBLE, ActionGeo_Long DOUBLE, ActionGeo_FeatureID STRING,
            DATEADDED STRING, SOURCEURL STRING,
            date_added_ts TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(date_added_ts))
        TBLPROPERTIES ('format-version'='2', 'write.target-file-size-bytes'='134217728')
    """)


def create_bronze_mentions_table(spark):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.bronze.mentions (
            GLOBALEVENTID BIGINT, EventTimeDate STRING, MentionTimeDate STRING, MentionType STRING,
            MentionSourceName STRING, MentionIdentifier STRING, SentenceID INT,
            Actor1CharOffset INT, Actor2CharOffset INT, ActionCharOffset INT,
            InRawText INT, Confidence INT, MentionDocLen INT, MentionDocTone DOUBLE,
            MentionDocTranslationInfo STRING, Extras STRING,
            mention_ts TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(mention_ts))
        TBLPROPERTIES ('format-version'='2', 'write.target-file-size-bytes'='134217728')
    """)


def create_bronze_gkg_table(spark):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.bronze.gkg (
            GKGRECORDID STRING, DATE STRING, SourceCollectionIdentifier INT, SourceCommonName STRING,
            DocumentIdentifier STRING, Counts STRING, V2Counts STRING, Themes STRING, V2Themes STRING,
            Locations STRING, V2Locations STRING, Persons STRING, V2Persons STRING,
            Organizations STRING, V2Organizations STRING, V2Tone STRING, Dates STRING,
            GCAM STRING, SharingImage STRING, RelatedImages STRING, SocialImageEmbeds STRING,
            SocialVideoEmbeds STRING, Quotations STRING, AllNames STRING, Amounts STRING,
            TranslationInfo STRING, Extras STRING,
            gkg_ts TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(gkg_ts))
        TBLPROPERTIES ('format-version'='2', 'write.target-file-size-bytes'='134217728')
    """)


def create_all_bronze_tables(spark):
    create_bronze_namespace(spark)
    create_bronze_events_table(spark)
    create_bronze_mentions_table(spark)
    create_bronze_gkg_table(spark)