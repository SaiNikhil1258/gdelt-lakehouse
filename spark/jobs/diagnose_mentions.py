# spark/jobs/diagnose_mentions.py
from pyspark.sql import SparkSession
from schemas import MENTIONS_COLUMNS

spark = SparkSession.builder.appName("diagnose-mentions").getOrCreate()
raw_df = (spark.read.option("sep", "\t")
    .csv("s3a://gdelt-landing/mentions/20260912183000.csv")
    .toDF(*MENTIONS_COLUMNS))
raw_df.select("GLOBALEVENTID", "EventTimeDate", "MentionTimeDate", "MentionIdentifier").show(5, truncate=False)
spark.stop()