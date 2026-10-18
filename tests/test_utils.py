"""Tests for spark/jobs/utils.py -- the logic shared by Silver, Gold and verify_all."""
import pytest
from pyspark.sql import functions as F

from utils import conflict_severity, extract_batch_id, normalize_url, partner_country


# --------------------------------------------------------------------------- normalize_url
@pytest.mark.parametrize(
    "raw, expected",
    [
        # case, whitespace, fragment
        ("  https://Site.com/Story  ", "https://site.com/story"),
        ("https://site.com/a#comments", "https://site.com/a"),
        # tracking parameters are dropped ...
        ("https://site.com/a?utm_source=x", "https://site.com/a"),
        ("https://site.com/a?utm_medium=1&utm_campaign=2", "https://site.com/a"),
        ("https://site.com/a?fbclid=abc", "https://site.com/a"),
        # ... in any position, without breaking the remaining query string
        ("https://site.com/a?utm_source=x&id=5", "https://site.com/a?id=5"),
        ("https://site.com/a?id=5&utm_source=x", "https://site.com/a?id=5"),
        ("https://site.com/a?id=5&fbclid=z&p=2", "https://site.com/a?id=5&p=2"),
        # parameters that identify the article are kept
        ("https://site.com/story.php?id=5", "https://site.com/story.php?id=5"),
        ("https://site.com/?p=123", "https://site.com/?p=123"),
    ],
)
def test_normalize_url(spark, raw, expected):
    df = spark.createDataFrame([(raw,)], ["url"])
    assert df.select(normalize_url(F.col("url"))).first()[0] == expected


def test_normalize_url_keeps_different_articles_apart(spark):
    """The bug this guards against: stripping the whole query string used to merge
    story.php?id=1 and story.php?id=2, so one GKG row got attached to both."""
    df = spark.createDataFrame(
        [("https://site.com/story.php?id=1",), ("https://site.com/story.php?id=2",)], ["url"]
    )
    distinct = df.select(normalize_url(F.col("url")).alias("n")).distinct().count()
    assert distinct == 2


def test_normalize_url_merges_same_article_with_tracking_noise(spark):
    df = spark.createDataFrame(
        [("https://site.com/a?id=1",), ("https://site.com/a?id=1&utm_source=tw",)], ["url"]
    )
    assert df.select(normalize_url(F.col("url")).alias("n")).distinct().count() == 1


def test_normalize_url_null_stays_null(spark):
    df = spark.createDataFrame([(None,)], "url string")
    assert df.select(normalize_url(F.col("url"))).first()[0] is None


# --------------------------------------------------------------------------- conflict_severity
@pytest.mark.parametrize(
    "quad, expected",
    [
        (1, "Verbal Cooperation"),
        (2, "Material Cooperation"),
        (3, "Verbal Conflict"),
        (4, "Material Conflict"),
        (None, None),   # used to become "Verbal Cooperation"
        (9, None),      # unknown code must not be mislabelled either
    ],
)
def test_conflict_severity(spark, quad, expected):
    df = spark.createDataFrame([(quad,)], "quad int")
    assert df.select(conflict_severity(F.col("quad"))).first()[0] == expected


# --------------------------------------------------------------------------- partner_country
@pytest.mark.parametrize(
    "actor1, actor2, expected",
    [
        ("IND", "USA", "USA"),     # India is actor 1
        ("USA", "IND", "USA"),     # India is actor 2
        ("IND", "IND", None),      # domestic
        ("USA", "CHN", None),      # happened in India maybe, but India is not an actor
        (None, "IND", None),       # other actor unknown
        ("IND", None, None),
        (None, None, None),
        ("IND", "", ""),           # empty partner is returned as-is; Gold filters it out
    ],
)
def test_partner_country(spark, actor1, actor2, expected):
    df = spark.createDataFrame([(actor1, actor2)], "a1 string, a2 string")
    result = df.select(partner_country(F.col("a1"), F.col("a2"))).first()[0]
    assert result == expected


# --------------------------------------------------------------------------- extract_batch_id
def test_extract_batch_id():
    assert extract_batch_id("20260910143000.export.CSV") == "20260910143000"
    assert extract_batch_id("s3a://gdelt-landing/events/20260910143000.csv") == "20260910143000"