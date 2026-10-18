"""Drop every table and namespace in the lakehouse so Iceberg is back to zero.

DROP TABLE ... PURGE removes the table registration in the REST catalog AND
deletes its data/metadata files from MinIO. Namespaces are dropped last.
Recreate everything afterwards with bootstrap_bronze/silver/gold.py.
"""
from pyspark.sql import SparkSession

NAMESPACES = ["bronze", "silver", "gold", "scratch"]


def main():
    spark = SparkSession.builder.appName("reset-lakehouse").getOrCreate()

    existing = {row[0] for row in spark.sql("SHOW NAMESPACES IN local").collect()}
    print(f"Namespaces before reset: {sorted(existing)}")

    for ns in NAMESPACES:
        if ns not in existing:
            print(f"[skip] local.{ns} does not exist")
            continue

        for t in spark.sql(f"SHOW TABLES IN local.{ns}").collect():
            fqn = f"local.{ns}.{t.tableName}"
            spark.sql(f"DROP TABLE {fqn} PURGE")
            print(f"[dropped] {fqn}")

        spark.sql(f"DROP NAMESPACE local.{ns}")
        print(f"[dropped] namespace local.{ns}")

    print("\nNamespaces after reset:")
    spark.sql("SHOW NAMESPACES IN local").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()