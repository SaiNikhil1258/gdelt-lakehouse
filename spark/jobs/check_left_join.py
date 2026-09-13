# spark/jobs/check_left_join.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("check-left-join").getOrCreate()
mentions = spark.table("local.silver.mentions_enriched")
print("Total rows:", mentions.count())
print("Distinct MentionIdentifier count:", mentions.select("MentionIdentifier").distinct().count())
mentions.select("MentionIdentifier", "V2Themes").show(10, truncate=80)
spark.stop()