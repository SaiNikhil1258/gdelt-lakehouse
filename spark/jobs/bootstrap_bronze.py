from pyspark.sql import SparkSession
from ddl import create_all_bronze_tables

def main():
    spark = SparkSession.builder.appName("bootstrap-bronze").getOrCreate()
    create_all_bronze_tables(spark)
    print("Bronze namespace + tables ready:")
    spark.sql("SHOW TABLES IN local.bronze").show(truncate=False)
    spark.stop()

if __name__ == "__main__":
    main()