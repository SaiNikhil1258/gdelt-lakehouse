import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Small local Spark session shared by all tests (no cluster, no Iceberg)."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("gdelt-unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()