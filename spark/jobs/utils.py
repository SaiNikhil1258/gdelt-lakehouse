from pyspark.sql import functions as F

def normalize_url(col):
    """Lowercase, trim, strip query string — for the Mentions<->GKG soft join."""
    return F.lower(F.trim(F.regexp_replace(col, r"\?.*$", "")))

def parse_gdelt_ts(col, fmt="yyyyMMddHHmmss"):
    return F.to_timestamp(col, fmt)

def extract_batch_id(file_path: str) -> str:
    """20260910143000.export.CSV -> 20260910143000"""
    return file_path.split("/")[-1].split(".")[0]