"""Read-only evidence for the Silver mention-grain decision.

1. For India-relevant events (same filter as Silver), how many Bronze mention rows exist
   versus distinct (GLOBALEVENTID, MentionIdentifier) pairs, and for the pairs that have
   several rows, which columns differ between the copies.
2. Whether bronze.gkg has several rows per normalized URL (a LEFT JOIN on URL would then
   multiply mention rows).
"""
from pyspark.sql import SparkSession, functions as F
from utils import normalize_url

CHECK_COLS = ["MentionSourceName", "MentionDocTone", "MentionDocLen",
              "Confidence", "SentenceID", "InRawText"]


def main():
    spark = (
        SparkSession.builder.appName("diagnose-mention-grain")
        .config("spark.executor.memory", "4g")
        .getOrCreate()
    )

    india_ids = (
        spark.table("local.bronze.events")
        .filter(
            (F.col("ActionGeo_CountryCode") == "IN")
            | (F.col("Actor1CountryCode") == "IND")
            | (F.col("Actor2CountryCode") == "IND")
        )
        .filter(F.col("SOURCEURL").isNotNull() & F.col("GLOBALEVENTID").isNotNull())
        .select("GLOBALEVENTID")
        .distinct()
    )
    m = spark.table("local.bronze.mentions").join(F.broadcast(india_ids), "GLOBALEVENTID", "inner")

    def nd(c):
        # distinct count that treats NULL as a value
        return F.countDistinct(F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")))

    per_pair = m.groupBy("GLOBALEVENTID", "MentionIdentifier").agg(
        F.count("*").alias("n"),
        *[nd(c).alias(f"d_{c}") for c in CHECK_COLS],
    ).cache()

    total_rows = m.count()
    pairs = per_pair.count()
    print("=== MENTION GRAIN (India events) ===")
    print(f"mention rows: {total_rows} | distinct (event, article) pairs: {pairs} | extra rows: {total_rows - pairs}")

    multi = per_pair.filter("n > 1")
    print("=== PAIRS WITH SEVERAL ROWS: how many rows per pair ===")
    multi.groupBy("n").count().orderBy("n").show(20, truncate=False)

    print("=== PAIRS WITH SEVERAL ROWS: columns that differ between the copies ===")
    multi.agg(
        F.count("*").alias("pairs_with_multiple_rows"),
        *[F.sum(F.when(F.col(f"d_{c}") > 1, 1).otherwise(0)).alias(f"{c}_differs") for c in CHECK_COLS],
    ).show(vertical=True, truncate=False)

    gkg = (
        spark.table("local.bronze.gkg")
        .filter(F.col("DocumentIdentifier").isNotNull())
        .select(normalize_url(F.col("DocumentIdentifier")).alias("norm_url"))
    )
    gkg_total = gkg.count()
    gkg_distinct = gkg.distinct().count()
    print("=== GKG URL FAN-OUT ===")
    print(f"gkg rows with a DocumentIdentifier: {gkg_total} | distinct normalized URLs: {gkg_distinct} "
          f"| rows that would fan out a URL join: {gkg_total - gkg_distinct}")

    spark.stop()


if __name__ == "__main__":
    main()