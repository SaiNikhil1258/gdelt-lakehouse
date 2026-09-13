# spark/jobs/check_mention_dedup.py
from pyspark.sql import SparkSession, functions as F
from utils import normalize_url

spark = SparkSession.builder.appName("check-mention-dedup").getOrCreate()

mentions = spark.table("local.silver.mentions_enriched")
total = mentions.count()
distinct_pairs = mentions.select("GLOBALEVENTID", "MentionIdentifier").distinct().count()
print(f"Total rows: {total}")
print(f"Distinct (GLOBALEVENTID, MentionIdentifier) pairs: {distinct_pairs}")

print("\n-- Rows where (GLOBALEVENTID, MentionIdentifier) repeats (true duplicates, if any) --")
mentions.groupBy("GLOBALEVENTID", "MentionIdentifier").count() \
    .filter("count > 1").show(20, truncate=80)

print("\n-- Checking the other side: does bronze.gkg have duplicate normalized URLs? --")
gkg = spark.table("local.bronze.gkg").withColumn("norm_url", normalize_url(F.col("DocumentIdentifier")))
gkg_total = gkg.count()
gkg_distinct_url = gkg.select("norm_url").distinct().count()
print(f"bronze.gkg total: {gkg_total}, distinct normalized DocumentIdentifier: {gkg_distinct_url}")
gkg.groupBy("norm_url").count().filter("count > 1").show(20, truncate=80)

spark.stop()