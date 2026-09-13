from pyspark.sql import SparkSession
from silver_ddl import create_all_silver_tables

def main():
    spark = SparkSession.builder.appName("bootstrap-silver").getOrCreate()
    create_all_silver_tables(spark)
    spark.sql("SHOW TABLES IN local.silver").show(truncate=False)
    spark.stop()

if __name__ == "__main__":
    main()