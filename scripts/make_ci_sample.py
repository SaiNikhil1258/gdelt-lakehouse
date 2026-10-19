#!/usr/bin/env python3
"""Write a tiny, deterministic GDELT sample for the CI pipeline test.

Two 15-minute slots on one day, three streams, laid out like the landing bucket:
    <out>/events/20260915000000.csv   <out>/mentions/...   <out>/gkg/...

The rows are chosen to exercise the rules we fixed (who counts as an India event, bilateral
partner, state conflict, URL matching, GKG de-duplication). The expected pipeline results
are asserted in spark/jobs/ci_sample_assertions.py; change one, change the other.

Pure standard library, so it runs on a bare CI runner.
"""
import argparse
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "spark", "jobs"))
from schemas import EVENTS_COLUMNS, GKG_COLUMNS, MENTIONS_COLUMNS  # noqa: E402

DAY = "20260915"
SLOT0, SLOT1 = f"{DAY}000000", f"{DAY}001500"
SITE = "http://news.example"


def event(eid, slot, a1, a2, geo_cc, adm1, quad, goldstein, tone, url, lat="28.6", lon="77.2"):
    return {
        "GLOBALEVENTID": eid, "SQLDATE": DAY, "MonthYear": "202609", "Year": "2026",
        "FractionDate": "2026.7", "Actor1CountryCode": a1, "Actor2CountryCode": a2,
        "Actor1Name": a1, "Actor2Name": a2, "IsRootEvent": "1", "EventCode": "190",
        "EventBaseCode": "190", "EventRootCode": "19", "QuadClass": quad,
        "GoldsteinScale": goldstein, "NumMentions": "1", "NumSources": "1", "NumArticles": "1",
        "AvgTone": tone, "ActionGeo_Type": "4", "ActionGeo_FullName": f"somewhere, {geo_cc}",
        "ActionGeo_CountryCode": geo_cc, "ActionGeo_ADM1Code": adm1, "ActionGeo_Lat": lat,
        "ActionGeo_Long": lon, "ActionGeo_FeatureID": "1", "DATEADDED": slot, "SOURCEURL": url,
    }


def mention(eid, slot, url, source, sentence="1", confidence="80", tone="0.5"):
    return {
        "GLOBALEVENTID": eid, "EventTimeDate": slot, "MentionTimeDate": slot,
        "MentionType": "1", "MentionSourceName": source, "MentionIdentifier": url,
        "SentenceID": sentence, "Actor1CharOffset": "10", "Actor2CharOffset": "20",
        "ActionCharOffset": "30", "InRawText": "1", "Confidence": confidence,
        "MentionDocLen": "1000", "MentionDocTone": tone,
    }


def gkg(record, slot, url, themes):
    return {
        "GKGRECORDID": record, "DATE": slot, "SourceCollectionIdentifier": "1",
        "SourceCommonName": "news.example", "DocumentIdentifier": url,
        "V2Themes": themes, "V2Tone": "1.0,2.0,3.0,4.0,5.0,6.0,7.0",
    }


EVENTS = {
    SLOT0: [
        # 1: India vs USA, happened in India          -> Silver, bilateral USA, state IN07
        event("1", SLOT0, "IND", "USA", "IN", "IN07", "3", "-4.0", "-2.5", f"{SITE}/a1"),
        # 2: USA vs India, happened in the US         -> Silver, bilateral USA, NOT state
        event("2", SLOT0, "USA", "IND", "US", "USCA", "1", "3.0", "1.5", f"{SITE}/a2", "34.0", "-118.0"),
        # 3: USA vs China, happened in India          -> Silver, state IN12, NOT bilateral
        event("3", SLOT0, "USA", "CHN", "IN", "IN12", "2", "5.0", "0.5", f"{SITE}/a3"),
        # 4: India vs India (domestic)                -> Silver, state IN07, NOT bilateral
        event("4", SLOT0, "IND", "IND", "IN", "IN07", "4", "-8.0", "-6.0", f"{SITE}/a4"),
        # 5: no India involvement at all              -> not in Silver
        event("5", SLOT0, "USA", "GBR", "UK", "UKH9", "1", "2.0", "0.0", f"{SITE}/a5", "51.5", "-0.1"),
    ],
    SLOT1: [
        # 6: India vs Pakistan, happened in Pakistan  -> Silver, bilateral PAK, NOT state
        event("6", SLOT1, "IND", "PAK", "PK", "PK04", "4", "-6.5", "-4.0", f"{SITE}/a6", "33.7", "73.0"),
        # 7: India, no second actor, no state known   -> Silver only
        event("7", SLOT1, "IND", "", "IN", "", "1", "1.0", "0.2", f"{SITE}/a7"),
        # 8: India vs USA, happened in India          -> Silver, bilateral USA, state IN07
        event("8", SLOT1, "IND", "USA", "IN", "IN07", "2", "6.0", "2.0", f"{SITE}/a8"),
        # 9: India event without a source URL         -> dropped from Silver
        event("9", SLOT1, "IND", "USA", "IN", "IN07", "1", "1.0", "0.0", ""),
    ],
}

MENTIONS = {
    SLOT0: [
        mention("1", SLOT0, f"{SITE}/a1", "news.example", tone="-2.5"),
        # same article as above plus a tracking parameter: still matches the same GKG record
        mention("1", SLOT0, f"{SITE}/a1?utm_source=tw", "social.example", tone="-1.5"),
        mention("2", SLOT0, f"{SITE}/a2", "news.example"),
        # two different articles that differ only by ?id=: must NOT share a GKG record
        mention("3", SLOT0, f"{SITE}/story.php?id=3", "news.example"),
        mention("4", SLOT0, f"{SITE}/story.php?id=4", "news.example"),
    ],
    SLOT1: [
        mention("6", SLOT1, f"{SITE}/a6", "news.example"),
        mention("7", SLOT1, f"{SITE}/a7", "news.example"),
        # same (event, article) twice, different sentence: both stay in Bronze, one in Silver
        mention("8", SLOT1, f"{SITE}/a8", "news.example", sentence="1", confidence="60"),
        mention("8", SLOT1, f"{SITE}/a8", "news.example", sentence="2", confidence="90"),
        mention("5", SLOT1, f"{SITE}/a5", "news.example"),     # event 5 is not in Silver
        mention("9", SLOT1, f"{SITE}/a9", "news.example"),     # event 9 is not in Silver
    ],
}

GKG = {
    SLOT0: [
        gkg(f"{SLOT0}-1", SLOT0, f"{SITE}/a1", "T1_OLD"),
        gkg(f"{SLOT0}-2", SLOT0, f"{SITE}/a2", "T2"),
        gkg(f"{SLOT0}-3", SLOT0, f"{SITE}/story.php?id=3", "T3"),
        gkg(f"{SLOT0}-4", SLOT0, f"{SITE}/story.php?id=4", "T4"),
        gkg(f"{SLOT0}-5", SLOT0, f"{SITE}/a5", "T5"),
    ],
    SLOT1: [
        # a later record for the same URL as T1_OLD: the latest one must win
        gkg(f"{SLOT1}-9", SLOT1, f"{SITE}/a1", "T1_NEW"),
        gkg(f"{SLOT1}-6", SLOT1, f"{SITE}/a6", "T6"),
        gkg(f"{SLOT1}-7", SLOT1, f"{SITE}/a7", "T7"),
        gkg(f"{SLOT1}-8", SLOT1, f"{SITE}/a8", "T8"),
    ],
}


def write(out_dir, stream, columns, rows_by_slot):
    folder = os.path.join(out_dir, stream)
    os.makedirs(folder, exist_ok=True)
    for slot, rows in rows_by_slot.items():
        path = os.path.join(folder, f"{slot}.csv")
        with open(path, "w", encoding="utf-8", newline="") as fh:
            for row in rows:
                unknown = set(row) - set(columns)
                assert not unknown, f"{stream}: unknown columns {unknown}"
                values = [row.get(col, "") for col in columns]
                assert not any("\t" in v or "\n" in v for v in values)
                fh.write("\t".join(values) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", required=True, help="directory to write events/ mentions/ gkg/ into")
    args = parser.parse_args()
    write(args.out, "events", EVENTS_COLUMNS, EVENTS)
    write(args.out, "mentions", MENTIONS_COLUMNS, MENTIONS)
    write(args.out, "gkg", GKG_COLUMNS, GKG)
    print(f"sample for {DAY} written to {args.out}")


if __name__ == "__main__":
    main()