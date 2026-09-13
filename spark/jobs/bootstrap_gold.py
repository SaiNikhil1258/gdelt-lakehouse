from pyspark.sql import SparkSession
from gold_ddl import create_all_gold_tables

def main():
    spark = SparkSession.builder.appName("bootstrap-gold").getOrCreate()
    create_all_gold_tables(spark)
    spark.sql("SHOW TABLES IN local.gold").show(truncate=False)
    spark.stop()

if __name__ == "__main__":
    main()