from pyspark.sql import functions as F

# Query parameters that only track where a click came from. Anything else in the query
# string (id=, p=, story=, ...) can identify the article itself and must be kept.
TRACKING_PARAMS = (
    r"utm_[a-z0-9_]*|fbclid|gclid|dclid|msclkid|mc_cid|mc_eid|igshid|ocid|ncid|cmpid"
)


def normalize_url(col):
    """Lowercase, trim, drop the #fragment and tracking parameters -- for the
    Mentions<->GKG soft join.

    Unlike the previous version this keeps the rest of the query string, so that
    'site.com/story.php?id=1' and 'site.com/story.php?id=2' stay different articles."""
    s = F.lower(F.trim(col))
    s = F.regexp_replace(s, r"#.*$", "")
    s = F.regexp_replace(s, r"[?&](" + TRACKING_PARAMS + r")(=[^&]*)?", "")
    # if the first parameter was removed, the next one starts with '&' instead of '?'
    s = F.regexp_replace(s, r"^([^?]*)&", "$1?")
    return s


def conflict_severity(quad_class):
    """GDELT QuadClass -> label. Unknown or NULL QuadClass stays NULL (it used to be
    silently labelled 'Verbal Cooperation')."""
    return (
        F.when(quad_class == 4, "Material Conflict")
         .when(quad_class == 3, "Verbal Conflict")
         .when(quad_class == 2, "Material Cooperation")
         .when(quad_class == 1, "Verbal Cooperation")
    )


def partner_country(actor1_country, actor2_country):
    """The country India interacts with in an event, or NULL when India is not exactly
    one of the two actors (India-India, no India actor, or the other actor is missing).
    Events that merely happened in India no longer get Actor1 as a 'partner'."""
    return (
        F.when((actor1_country == "IND") & (actor2_country != "IND"), actor2_country)
         .when((actor2_country == "IND") & (actor1_country != "IND"), actor1_country)
    )

def parse_gdelt_ts(col, fmt="yyyyMMddHHmmss"):
    return F.to_timestamp(col, fmt)

def extract_batch_id(file_path: str) -> str:
    """20260910143000.export.CSV -> 20260910143000"""
    return file_path.split("/")[-1].split(".")[0]