"""Tests for the pure logic in scripts/backfill.py. No network, MinIO or Spark needed."""
import argparse
from datetime import date
from types import SimpleNamespace

import pytest

import backfill


# --------------------------------------------------------------------------- parse_date
def test_parse_date_ok():
    assert backfill.parse_date("20260901") == date(2026, 9, 1)


@pytest.mark.parametrize("bad", ["2026-09-01", "20261301", "abc", "", "202609"])
def test_parse_date_rejects_bad_input(bad):
    with pytest.raises(argparse.ArgumentTypeError):
        backfill.parse_date(bad)


# --------------------------------------------------------------------------- timestamp_range
def test_timestamp_range_one_day_has_96_slots():
    stamps = list(backfill.timestamp_range(date(2026, 9, 1), date(2026, 9, 1)))
    assert len(stamps) == 96
    assert stamps[0] == "20260901000000"
    assert stamps[1] == "20260901001500"
    assert stamps[-1] == "20260901234500"
    assert len(set(stamps)) == 96


def test_timestamp_range_crosses_midnight_and_month_end():
    stamps = list(backfill.timestamp_range(date(2026, 9, 30), date(2026, 10, 1)))
    assert len(stamps) == 192
    assert stamps[95] == "20260930234500"
    assert stamps[96] == "20261001000000"


# --------------------------------------------------------------------------- split_into_chunks
def test_split_into_chunks_september_by_7_days():
    chunks = list(backfill.split_into_chunks(date(2026, 9, 1), date(2026, 9, 30), 7))
    assert len(chunks) == 5
    assert chunks[0] == (date(2026, 9, 1), date(2026, 9, 7))
    assert chunks[-1] == (date(2026, 9, 29), date(2026, 9, 30))


@pytest.mark.parametrize("chunk_days", [1, 3, 7, 10, 45])
def test_chunks_cover_range_exactly_once(chunk_days):
    start, end = date(2026, 9, 1), date(2026, 10, 17)
    chunks = list(backfill.split_into_chunks(start, end, chunk_days))
    assert chunks[0][0] == start and chunks[-1][1] == end
    for (_, prev_end), (next_start, _) in zip(chunks, chunks[1:]):
        assert (next_start - prev_end).days == 1          # no gap, no overlap
    assert all((b - a).days + 1 <= chunk_days for a, b in chunks)
    assert sum((b - a).days + 1 for a, b in chunks) == (end - start).days + 1


def test_split_into_chunks_single_day():
    assert list(backfill.split_into_chunks(date(2026, 9, 1), date(2026, 9, 1), 7)) == [
        (date(2026, 9, 1), date(2026, 9, 1))
    ]


# --------------------------------------------------------------------------- validate_arguments
def make_args(**overrides):
    base = dict(
        start_date=date(2026, 9, 1),
        end_date=date(2026, 9, 30),
        workers=6,
        retries=4,
        chunk_days=7,
        streams=["events", "mentions", "gkg"],
        download_only=False,
        ingest_only=False,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def test_validate_arguments_accepts_good_args():
    backfill.validate_arguments(make_args())


@pytest.mark.parametrize(
    "overrides, message",
    [
        (dict(start_date=date(2026, 10, 1), end_date=date(2026, 9, 1)), "cannot be after"),
        (dict(workers=0), "--workers"),
        (dict(retries=0), "--retries"),
        (dict(chunk_days=0), "--chunk-days"),
        (dict(streams=["events", "bogus"]), "Invalid streams"),
        (dict(download_only=True, ingest_only=True), "cannot be used together"),
    ],
)
def test_validate_arguments_rejects_bad_args(overrides, message):
    with pytest.raises(SystemExit) as exc:
        backfill.validate_arguments(make_args(**overrides))
    assert message in str(exc.value)


# --------------------------------------------------------------------------- acquire_lock
def test_second_backfill_cannot_take_the_lock(tmp_path, monkeypatch):
    monkeypatch.setattr(backfill, "LOCK_PATH", str(tmp_path / ".backfill.lock"))
    first = backfill.acquire_lock()
    try:
        with pytest.raises(SystemExit) as exc:
            backfill.acquire_lock()
        assert "already running" in str(exc.value)
    finally:
        first.close()


def test_lock_is_released_when_the_holder_closes(tmp_path, monkeypatch):
    """The lock is an OS flock, so a leftover .backfill.lock FILE does not block anything."""
    monkeypatch.setattr(backfill, "LOCK_PATH", str(tmp_path / ".backfill.lock"))
    backfill.acquire_lock().close()
    again = backfill.acquire_lock()      # must not raise
    again.close()