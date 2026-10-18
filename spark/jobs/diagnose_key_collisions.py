"""Read-only: after widening the mentions key, show what still collides.

For every group of rows sharing the full MENTIONS_KEY it reports which NON-key
columns differ between the copies, then prints the colliding rows themselves.
"""
from pyspark.sql import SparkSession, functions as F
from bronze_ingest import MENTIONS_KEY


def main():
    spark = SparkSession.builder.appName("diagnose-key-collisions").getOrCreate()

    m = spark.table("local.bronze.mentions")
    other_cols = [c for c in m.columns if c not in MENTIONS_KEY]

    def nd(c):
        # distinct count that treats NULL as a value (plain countDistinct ignores NULLs)
        return F.countDistinct(F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")))

    per_key = m.groupBy(*MENTIONS_KEY).agg(
        F.count("*").alias("n"),
        *[nd(c).alias(f"d_{c}") for c in other_cols],
    )
    dups = per_key.filter("n > 1").cache()

    print("=== COLLISION SUMMARY ===")
    print("(each column = number of colliding groups where that column differs between the copies)")
    dups.agg(
        F.count("*").alias("dup_groups"),
        F.sum(F.col("n") - 1).alias("extra_rows"),
        *[F.sum(F.when(F.col(f"d_{c}") > 1, 1).otherwise(0)).alias(c) for c in other_cols],
    ).show(vertical=True, truncate=False)

    print("=== COLLISION ROWS ===")
    (m.join(dups.select(*MENTIONS_KEY), on=MENTIONS_KEY, how="inner")
       .select("GLOBALEVENTID", "SentenceID", "Actor1CharOffset", "Actor2CharOffset",
               "ActionCharOffset", *other_cols)
       .orderBy("GLOBALEVENTID", "SentenceID")
       .show(40, truncate=40))

    spark.stop()


if __name__ == "__main__":
    main()