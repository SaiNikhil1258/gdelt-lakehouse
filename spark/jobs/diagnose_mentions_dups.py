"""Read-only: explain why bronze.mentions has more rows than distinct
(GLOBALEVENTID, MentionIdentifier, mention_ts) keys.

For each duplicated key it reports:
  - n_files: how many distinct Iceberg data files the copies live in
    (each backfill batch writes one file, so >1 means cross-batch, 1 means within-batch)
  - n_sentence_ids: whether the copies differ by SentenceID
  - n_distinct_rows: whether the copies are fully identical (1) or differ in some column
"""
from pyspark.sql import SparkSession, functions as F

KEY = ["GLOBALEVENTID", "MentionIdentifier", "mention_ts"]


def main():
    spark = SparkSession.builder.appName("diagnose-mention-dups").getOrCreate()

    m = spark.sql("SELECT *, _file AS src_file FROM local.bronze.mentions")
    data_cols = [c for c in m.columns if c != "src_file"]

    per_key = m.groupBy(*KEY).agg(
        F.count("*").alias("n"),
        F.countDistinct("src_file").alias("n_files"),
        F.countDistinct("SentenceID").alias("n_sentence_ids"),
        F.countDistinct(F.sha2(F.to_json(F.struct(*data_cols)), 256)).alias("n_distinct_rows"),
    )
    dups = per_key.filter("n > 1").cache()

    summary = dups.agg(
        F.count("*").alias("dup_groups"),
        F.sum(F.col("n") - 1).alias("extra_rows"),
        F.sum(F.when(F.col("n_files") > 1, 1).otherwise(0)).alias("groups_across_files"),
        F.sum(F.when(F.col("n_files") == 1, 1).otherwise(0)).alias("groups_within_one_file"),
        F.sum(F.when(F.col("n_sentence_ids") > 1, 1).otherwise(0)).alias("groups_differing_sentenceid"),
        F.sum(F.when(F.col("n_distinct_rows") == 1, 1).otherwise(0)).alias("groups_fully_identical"),
    )
    print("=== DUP SUMMARY ===")
    summary.show(truncate=False)

    print("=== SAMPLE DUPLICATED KEYS (5 groups) ===")
    sample_keys = dups.select(*KEY).limit(5)
    (m.join(sample_keys, on=KEY, how="inner")
       .select("GLOBALEVENTID", "MentionIdentifier", "mention_ts", "SentenceID",
               "Actor1CharOffset", "Actor2CharOffset", "ActionCharOffset",
               "InRawText", "Confidence", "MentionDocLen",
               F.regexp_extract("src_file", r"([^/]+)$", 1).alias("src_file"))
       .orderBy("GLOBALEVENTID", "MentionIdentifier", "SentenceID")
       .show(30, truncate=60))

    spark.stop()


if __name__ == "__main__":
    main()