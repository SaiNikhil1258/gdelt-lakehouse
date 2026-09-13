from pyspark.sql import SparkSession, functions

spark = SparkSession.builder.appName("test").getOrCreate()


spark.sql("""
          select * from local.bronze.mentions limit 10;
""").show(truncate=False)
spark.sql("""
          select * from local.bronze.events limit 10;
""").show(truncate=False)
spark.sql("""
          select * from local.bronze.gkg limit 10;
""").show(truncate=False)