"""Standalone check for MERGE INTO ... WHEN MATCHED THEN UPDATE, run once to
build confidence before Gold's incremental counters depend on it. Uses a
disposable scratch table -- doesn't touch bronze/silver/gold."""
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("test-merge-update").getOrCreate()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.scratch")
    spark.sql("DROP TABLE IF EXISTS local.scratch.merge_test")
    spark.sql("""
        CREATE TABLE local.scratch.merge_test (id BIGINT, val STRING)
        USING iceberg TBLPROPERTIES ('format-version'='2')
    """)
    spark.sql("INSERT INTO local.scratch.merge_test VALUES (1, 'original'), (2, 'original')")

    spark.createDataFrame([(1, "updated"), (3, "new")], ["id", "val"]) \
        .createOrReplaceTempView("merge_source")

    spark.sql("""
        MERGE INTO local.scratch.merge_test t
        USING merge_source s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET t.val = s.val
        WHEN NOT MATCHED THEN INSERT *
    """)

    rows = spark.sql("SELECT * FROM local.scratch.merge_test ORDER BY id").collect()
    actual = {row.id: row.val for row in rows}
    expected = {1: "updated", 2: "original", 3: "new"}

    print("Result after MERGE:", actual)
    print("PASS" if actual == expected else f"FAIL -- expected {expected}, got {actual}")

    spark.stop()

if __name__ == "__main__":
    main()