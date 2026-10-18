"""Tests for the Gold layer logic (spark/jobs/gold_aggregate.py) on tiny fixtures."""
from datetime import date

import pytest

from gold_aggregate import (
    compute_bilateral_sentiment,
    compute_mention_counts,
    compute_state_conflict,
)
from helpers import make_df

SILVER_SCHEMA = (
    "GLOBALEVENTID long, event_date date, Actor1CountryCode string, Actor2CountryCode string, "
    "GoldsteinScale double, AvgTone double, ActionGeo_CountryCode string, "
    "ActionGeo_ADM1Code string, conflict_severity string"
)
SILVER_DEFAULTS = dict(
    GLOBALEVENTID=1, event_date=date(2026, 9, 15), Actor1CountryCode=None,
    Actor2CountryCode=None, GoldsteinScale=0.0, AvgTone=0.0, ActionGeo_CountryCode=None,
    ActionGeo_ADM1Code=None, conflict_severity="Verbal Cooperation",
)

D15, D16 = date(2026, 9, 15), date(2026, 9, 16)


def silver(spark, *rows):
    return make_df(spark, SILVER_SCHEMA, SILVER_DEFAULTS, rows)


def bilateral_fixture(spark):
    return silver(
        spark,
        dict(GLOBALEVENTID=1, Actor1CountryCode="IND", Actor2CountryCode="USA", GoldsteinScale=2.0, AvgTone=1.0),
        dict(GLOBALEVENTID=2, Actor1CountryCode="USA", Actor2CountryCode="IND", GoldsteinScale=4.0, AvgTone=3.0),
        dict(GLOBALEVENTID=3, Actor1CountryCode="IND", Actor2CountryCode="IND"),                 # domestic
        dict(GLOBALEVENTID=4, Actor1CountryCode="USA", Actor2CountryCode="CHN",
             ActionGeo_CountryCode="IN"),                                                        # India not an actor
        dict(GLOBALEVENTID=5, Actor1CountryCode=None, Actor2CountryCode="IND"),                  # partner unknown
        dict(GLOBALEVENTID=6, event_date=D16, Actor1CountryCode="IND", Actor2CountryCode="PAK", GoldsteinScale=-6.0),
        dict(GLOBALEVENTID=7, Actor1CountryCode="IND", Actor2CountryCode=""),                    # empty partner
    )


# --------------------------------------------------------------------------- bilateral
def test_bilateral_only_counts_events_where_india_is_exactly_one_actor(spark):
    rows = {
        (r["event_date"], r["partner_country"]): r
        for r in compute_bilateral_sentiment(bilateral_fixture(spark)).collect()
    }
    assert set(rows) == {(D15, "USA"), (D16, "PAK")}

    usa = rows[(D15, "USA")]
    assert usa["event_count"] == 2
    assert usa["avg_goldstein"] == pytest.approx(3.0)
    assert usa["avg_tone"] == pytest.approx(2.0)

    pak = rows[(D16, "PAK")]
    assert pak["event_count"] == 1
    assert pak["avg_goldstein"] == pytest.approx(-6.0)


def test_bilateral_counts_add_up_to_the_qualifying_events(spark):
    total = sum(r["event_count"] for r in compute_bilateral_sentiment(bilateral_fixture(spark)).collect())
    assert total == 3      # events 1, 2 and 6


# --------------------------------------------------------------------------- state conflict
def test_state_conflict_only_counts_events_that_happened_in_india(spark):
    df = silver(
        spark,
        dict(GLOBALEVENTID=1, ActionGeo_CountryCode="IN", ActionGeo_ADM1Code="IN07",
             conflict_severity="Material Conflict", GoldsteinScale=-5.0),
        dict(GLOBALEVENTID=2, ActionGeo_CountryCode="IN", ActionGeo_ADM1Code="IN07",
             conflict_severity="Material Conflict", GoldsteinScale=-3.0),
        dict(GLOBALEVENTID=3, ActionGeo_CountryCode="IN", ActionGeo_ADM1Code="IN07",
             conflict_severity="Verbal Cooperation", GoldsteinScale=3.0),
        dict(GLOBALEVENTID=4, ActionGeo_CountryCode="US", ActionGeo_ADM1Code="USCA",
             Actor1CountryCode="IND"),                                    # India actor, foreign region
        dict(GLOBALEVENTID=5, ActionGeo_CountryCode="IN", ActionGeo_ADM1Code=None),   # no state known
    )
    rows = {
        (r["event_date"], r["adm1_code"], r["conflict_severity"]): r
        for r in compute_state_conflict(df).collect()
    }
    assert set(rows) == {
        (D15, "IN07", "Material Conflict"),
        (D15, "IN07", "Verbal Cooperation"),
    }
    conflict = rows[(D15, "IN07", "Material Conflict")]
    assert conflict["event_count"] == 2
    assert conflict["avg_goldstein"] == pytest.approx(-4.0)
    assert rows[(D15, "IN07", "Verbal Cooperation")]["event_count"] == 1


def test_state_conflict_output_columns_match_the_gold_table(spark):
    df = silver(spark, dict(ActionGeo_CountryCode="IN", ActionGeo_ADM1Code="IN07"))
    assert compute_state_conflict(df).columns == [
        "event_date", "adm1_code", "conflict_severity", "event_count", "avg_goldstein",
    ]


# --------------------------------------------------------------------------- mention counts
def test_mention_counts_are_full_totals_per_event(spark):
    mentions = make_df(
        spark,
        "GLOBALEVENTID long, MentionSourceName string, MentionDocTone double",
        dict(GLOBALEVENTID=1, MentionSourceName="a.com", MentionDocTone=0.0),
        [
            dict(GLOBALEVENTID=1, MentionSourceName="a.com", MentionDocTone=2.0),
            dict(GLOBALEVENTID=1, MentionSourceName="a.com", MentionDocTone=4.0),
            dict(GLOBALEVENTID=1, MentionSourceName="b.com", MentionDocTone=6.0),
            dict(GLOBALEVENTID=2, MentionSourceName="a.com", MentionDocTone=-1.0),
        ],
    )
    rows = {r["GLOBALEVENTID"]: r for r in compute_mention_counts(mentions).collect()}
    assert set(rows) == {1, 2}
    assert (rows[1]["total_mentions"], rows[1]["total_sources"]) == (3, 2)
    assert rows[1]["running_avg_tone"] == pytest.approx(4.0)
    assert (rows[2]["total_mentions"], rows[2]["total_sources"]) == (1, 1)
    assert rows[2]["running_avg_tone"] == pytest.approx(-1.0)