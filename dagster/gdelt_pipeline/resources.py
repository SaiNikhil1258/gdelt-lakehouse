import os

from dagster import ConfigurableResource
from pyspark.sql import SparkSession


class IcebergSparkResource(ConfigurableResource):

    app_name: str = "gdelt-lakehouse"

    def get_spark(self) -> SparkSession:

        access_key = os.environ["AWS_ACCESS_KEY_ID"]
        secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]

        iceberg_jars = ",".join(
            [
                "/opt/iceberg-jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar",
                "/opt/iceberg-jars/awssdk-bundle-2.20.160.jar",
                "/opt/iceberg-jars/hadoop-aws-3.3.4.jar",
                "/opt/iceberg-jars/aws-java-sdk-bundle-1.12.262.jar",
            ]
        )

        return (
            SparkSession.builder.appName(self.app_name)
            .master(os.environ["SPARK_MASTER_URL"])
            # Explicitly load Iceberg + AWS dependencies
            .config("spark.jars", iceberg_jars)
            # Iceberg Spark extension
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            # Iceberg REST catalog
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "rest")
            .config("spark.sql.catalog.local.uri", os.environ["CATALOG_URI"])
            .config(
                "spark.sql.catalog.local.warehouse",
                f"s3://{os.environ['WAREHOUSE_BUCKET']}/",
            )
            # Iceberg S3FileIO
            .config(
                "spark.sql.catalog.local.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
            )
            .config("spark.sql.catalog.local.s3.endpoint", "http://minio:9000")
            .config("spark.sql.catalog.local.s3.path-style-access", "true")
            .config("spark.sql.catalog.local.s3.access-key-id", access_key)
            .config("spark.sql.catalog.local.s3.secret-access-key", secret_key)
            # Hadoop S3A
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        )
