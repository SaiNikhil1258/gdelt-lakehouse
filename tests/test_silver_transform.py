"""Tests for the Silver layer logic (spark/jobs/silver_transform.py) on tiny fixtures."""
from datetime import date, datetime

from helpers import make_df
from silver_transform import build_events_india, build_mentions_enriched

# --------------------------------------------------------------------------- fixtures
EVENT_SCHEMA = (
    "GLOBALEVENTID long, SQLDATE string, Actor1CountryCode string, Actor1Name string, "
    "Actor2CountryCode string, Actor2Name string, EventRootCode string, QuadClass int, "
    "GoldsteinScale double, AvgTone double, ActionGeo_CountryCode string, "
    "ActionGeo_ADM1Code string, ActionGeo_Lat double, ActionGeo_Long double, SOURCEURL string"
)
EVENT_DEFAULTS = dict(
    GLOBALEVENTID=1, SQLDATE="20260915", Actor1CountryCode=None, Actor1Name=None,
    Actor2CountryCode=None, Actor2Name=None, EventRootCode="01", QuadClass=1,
    GoldsteinScale=1.0, AvgTone=0.0, ActionGeo_CountryCode=None, ActionGeo_ADM1Code=None,
    ActionGeo_Lat=None, ActionGeo_Long=None, SOURCEURL="http://x.com/a",
)

MENTION_SCHEMA = (
    "GLOBALEVENTID long, MentionIdentifier string, MentionSourceName string, "
    "MentionDocTone double, Confidence int"
)
MENTION_DEFAULTS = dict(
    GLOBALEVENTID=1, MentionIdentifier="http://a.com/x", MentionSourceName="a.com",
    MentionDocTone=0.0, Confidence=50,
)

GKG_SCHEMA = (
    "GKGRECORDID string, gkg_ts timestamp, DocumentIdentifier string, "
    "V2Themes string, V2Tone string"
)
GKG_DEFAULTS = dict(
    GKGRECORDID="20260915000000-1", gkg_ts=datetime(2026, 9, 15, 0, 0, 0),
    DocumentIdentifier="http://a.com/x", V2Themes="THEME", V2Tone="1,2,3",
)

SILVER_EVENT_SCHEMA = "GLOBALEVENTID long, event_date date"
SILVER_EVENT_DEFAULTS = dict(GLOBALEVENTID=1, event_date=date(2026, 9, 15))


def events(spark, *rows):
    return make_df(spark, EVENT_SCHEMA, EVENT_DEFAULTS, rows)


def run_mentions(spark, mentions, gkg=(), silver_events=({},)):
    result = build_mentions_enriched(
        make_df(spark, MENTION_SCHEMA, MENTION_DEFAULTS, mentions),
        make_df(spark, GKG_SCHEMA, GKG_DEFAULTS, gkg),
        make_df(spark, SILVER_EVENT_SCHEMA, SILVER_EVENT_DEFAULTS, silver_events),
    )
    return [r.asDict() for r in result.collect()]


# --------------------------------------------------------------------------- events_india
def test_keeps_events_where_india_is_involved_in_any_way(spark):
    df = events(
        spark,
        dict(GLOBALEVENTID=1, ActionGeo_CountryCode="IN"),                          # happened in India
        dict(GLOBALEVENTID=2, Actor1CountryCode="IND"),                             # India actor 1
        dict(GLOBALEVENTID=3, Actor2CountryCode="IND"),                             # India actor 2
        dict(GLOBALEVENTID=4, Actor1CountryCode="USA", ActionGeo_CountryCode="US"),  # no India
        dict(GLOBALEVENTID=5),                                                       # all NULL
    )
    ids = sorted(r[0] for r in build_events_india(df).select("GLOBALEVENTID").collect())
    assert ids == [1, 2, 3]


def test_drops_events_without_source_url(spark):
    df = events(
        spark,
        dict(GLOBALEVENTID=1, ActionGeo_CountryCode="IN", SOURCEURL=None),
        dict(GLOBALEVENTID=2, ActionGeo_CountryCode="IN"),
    )
    ids = [r[0] for r in build_events_india(df).select("GLOBALEVENTID").collect()]
    assert ids == [2]


def test_event_date_is_parsed_from_sqldate(spark):
    df = events(spark, dict(ActionGeo_CountryCode="IN", SQLDATE="20260915"))
    assert build_events_india(df).first()["event_date"] == date(2026, 9, 15)


def test_conflict_severity_labels(spark):
    df = events(
        spark,
        *[dict(GLOBALEVENTID=i, ActionGeo_CountryCode="IN", QuadClass=q)
          for i, q in [(1, 1), (2, 2), (3, 3), (4, 4), (5, None)]],
    )
    labels = {r["GLOBALEVENTID"]: r["conflict_severity"] for r in build_events_india(df).collect()}
    assert labels == {
        1: "Verbal Cooperation",
        2: "Material Cooperation",
        3: "Verbal Conflict",
        4: "Material Conflict",
        5: None,
    }


def test_duplicate_event_ids_collapse_to_one_row(spark):
    df = events(
        spark,
        dict(GLOBALEVENTID=7, ActionGeo_CountryCode="IN"),
        dict(GLOBALEVENTID=7, ActionGeo_CountryCode="IN"),
    )
    assert build_events_india(df).count() == 1


def test_events_output_columns_match_the_silver_table(spark):
    df = events(spark, dict(ActionGeo_CountryCode="IN"))
    assert build_events_india(df).columns == [
        "GLOBALEVENTID", "event_date", "Actor1CountryCode", "Actor1Name",
        "Actor2CountryCode", "Actor2Name", "EventRootCode", "QuadClass",
        "conflict_severity", "GoldsteinScale", "AvgTone",
        "ActionGeo_CountryCode", "ActionGeo_ADM1Code", "ActionGeo_Lat", "ActionGeo_Long",
        "SOURCEURL",
    ]


# --------------------------------------------------------------------------- mentions_enriched
def test_mentions_of_events_outside_silver_are_dropped(spark):
    rows = run_mentions(
        spark,
        mentions=[dict(GLOBALEVENTID=1), dict(GLOBALEVENTID=2)],
        silver_events=[dict(GLOBALEVENTID=1)],
    )
    assert [r["GLOBALEVENTID"] for r in rows] == [1]


def test_duplicate_event_article_rows_collapse_keeping_highest_confidence(spark):
    rows = run_mentions(
        spark,
        mentions=[dict(Confidence=40), dict(Confidence=60)],   # same (event, article) twice
    )
    assert len(rows) == 1
    assert rows[0]["Confidence"] == 60


def test_same_article_for_two_events_gives_two_rows(spark):
    rows = run_mentions(
        spark,
        mentions=[dict(GLOBALEVENTID=1), dict(GLOBALEVENTID=2)],
        silver_events=[dict(GLOBALEVENTID=1), dict(GLOBALEVENTID=2)],
    )
    assert sorted(r["GLOBALEVENTID"] for r in rows) == [1, 2]


def test_mention_without_gkg_record_is_kept(spark):
    rows = run_mentions(spark, mentions=[dict()], gkg=[])
    assert len(rows) == 1
    assert rows[0]["V2Themes"] is None and rows[0]["V2Tone"] is None


def test_gkg_rows_sharing_a_url_do_not_multiply_mentions_and_latest_wins(spark):
    rows = run_mentions(
        spark,
        mentions=[dict()],
        gkg=[
            dict(GKGRECORDID="20260915000000-1", gkg_ts=datetime(2026, 9, 15, 0, 0, 0), V2Themes="OLD"),
            dict(GKGRECORDID="20260915001500-1", gkg_ts=datetime(2026, 9, 15, 0, 15, 0), V2Themes="NEW"),
        ],
    )
    assert len(rows) == 1
    assert rows[0]["V2Themes"] == "NEW"


def test_gkg_timestamp_tie_is_broken_by_record_id(spark):
    same_ts = datetime(2026, 9, 15, 0, 0, 0)
    rows = run_mentions(
        spark,
        mentions=[dict()],
        gkg=[
            dict(GKGRECORDID="20260915000000-1", gkg_ts=same_ts, V2Themes="FIRST"),
            dict(GKGRECORDID="20260915000000-2", gkg_ts=same_ts, V2Themes="SECOND"),
        ],
    )
    assert len(rows) == 1
    assert rows[0]["V2Themes"] == "SECOND"


def test_tracking_parameters_do_not_prevent_a_gkg_match(spark):
    rows = run_mentions(
        spark,
        mentions=[dict(MentionIdentifier="http://a.com/x?utm_source=tw")],
        gkg=[dict(DocumentIdentifier="http://a.com/x")],
    )
    assert rows[0]["V2Themes"] == "THEME"


def test_articles_that_differ_only_by_id_parameter_do_not_cross_match(spark):
    rows = run_mentions(
        spark,
        mentions=[dict(MentionIdentifier="http://a.com/s.php?id=1")],
        gkg=[dict(DocumentIdentifier="http://a.com/s.php?id=2", V2Themes="OTHER")],
    )
    assert len(rows) == 1
    assert rows[0]["V2Themes"] is None


def test_gkg_rows_without_document_identifier_are_ignored(spark):
    rows = run_mentions(
        spark,
        mentions=[dict()],
        gkg=[
            dict(GKGRECORDID="broken", DocumentIdentifier=None, V2Themes="JUNK"),
            dict(),
        ],
    )
    assert len(rows) == 1
    assert rows[0]["V2Themes"] == "THEME"