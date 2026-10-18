"""Small helper for building DataFrames in tests without spelling out every column."""


def make_df(spark, schema, defaults, rows):
    """Build a DataFrame from a DDL-style schema string. Each row is a dict that only needs
    the columns it cares about; the rest come from `defaults`."""
    cols = [part.split()[0] for part in schema.split(",")]
    data = [tuple({**defaults, **row}[c] for c in cols) for row in rows]
    return spark.createDataFrame(data, schema)