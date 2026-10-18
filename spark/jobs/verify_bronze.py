from pyspark.sql import SparkSession
from bronze_ingest import MENTIONS_KEY


def main():
    spark = SparkSession.builder.appName("verify-bronze").getOrCreate()

    checks = {
        "events": ["GLOBALEVENTID"],
        "mentions": MENTIONS_KEY,
        "gkg": ["GKGRECORDID"],
    }

    for table, key_cols in checks.items():
        df = spark.table(f"local.bronze.{table}")
        total = df.count()
        distinct = df.select(*key_cols).distinct().count()
        nulls = df.filter(" OR ".join(f"{c} IS NULL" for c in key_cols)).count()
        status = "OK" if total == distinct and nulls == 0 else "PROBLEM"
        print(f"{table:10s} total={total:6d}  distinct_key={distinct:6d}  null_keys={nulls:3d}  -> {status}")

    spark.stop()


if __name__ == "__main__":
    main()