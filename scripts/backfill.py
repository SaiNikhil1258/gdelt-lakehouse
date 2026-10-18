"""Download GDELT slot files into MinIO, then ingest them into the Bronze Iceberg tables.

Hardened version. Compared with the first one:
  * preflight: MinIO and the Spark master UI must be reachable
  * refuses to start if another backfill is already running (host lock file + the Spark
    master's list of running applications); override with --force
  * on SIGINT / SIGTERM it kills its own Spark application on the master, so an interrupted
    run no longer leaves an orphan job holding the cluster's cores
  * honest ending: BACKFILL COMPLETE only when nothing failed. Files GDELT answered 404 for
    are listed as warnings (exit 0, or exit 1 with --fail-on-missing)
  * exit code is non-zero whenever a download failed or a Spark chunk failed
"""
import argparse
import concurrent.futures
import fcntl
import io
import os
import signal
import subprocess
import sys
import time
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_URL = "http://data.gdeltproject.org/gdeltv2"
S3_ENDPOINT = os.getenv("GDELT_S3_ENDPOINT", "http://localhost:9000")
S3_BUCKET = os.getenv("GDELT_S3_BUCKET", "gdelt-landing")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "changeme123")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

SPARK_MASTER_UI = os.getenv("SPARK_MASTER_UI", "http://localhost:8080")
SPARK_APP_NAME = "gdelt-backfill-ingest"   # must match the appName in spark/jobs/backfill_ingest.py
LOCK_PATH = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, ".backfill.lock")
)

STREAM_SUFFIX = {
    "events": "export.CSV.zip",
    "mentions": "mentions.CSV.zip",
    "gkg": "gkg.csv.zip",
}
DEFAULT_STREAMS = ["events", "mentions", "gkg"]
SLOT_MINUTES = 15
DEFAULT_WORKERS = 6
DEFAULT_RETRIES = 4
DEFAULT_CHUNK_DAYS = 7
REQUEST_TIMEOUT = 120
RETRY_BACKOFF = [15, 30, 60, 120]   # seconds between attempts for transient failures


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass
class DownloadResult:
    timestamp: str
    stream: str
    status: str            # downloaded | already_exists | missing | failed
    bytes_written: int = 0
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Date / timestamp helpers
# ---------------------------------------------------------------------------
def parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'. Expected YYYYMMDD.")


def timestamp_range(start: date, end: date):
    """Every 15-minute GDELT timestamp in the inclusive date range (96 per day)."""
    current = datetime.combine(start, datetime.min.time())
    final = datetime.combine(end, datetime.max.time())
    while current <= final:
        yield current.strftime("%Y%m%d%H%M%S")
        current += timedelta(minutes=SLOT_MINUTES)


def split_into_chunks(start: date, end: date, chunk_days: int):
    """Split a long backfill into bounded date ranges, e.g. 09-01..09-30 with 7 -> 5 chunks."""
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


# ---------------------------------------------------------------------------
# S3 / MinIO
# ---------------------------------------------------------------------------
def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
        config=Config(connect_timeout=10, read_timeout=60, retries={"max_attempts": 3}),
    )


def object_exists(client, key: str) -> bool:
    """Makes the downloader resumable: a file already in MinIO is never fetched again."""
    try:
        client.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


# ---------------------------------------------------------------------------
# Spark master: running applications and cleanup
# ---------------------------------------------------------------------------
def running_backfill_apps():
    resp = requests.get(f"{SPARK_MASTER_UI}/json/", timeout=5)
    resp.raise_for_status()
    return [a for a in resp.json().get("activeapps", []) if a.get("name") == SPARK_APP_NAME]


def kill_backfill_apps() -> int:
    """Kill running backfill Spark applications via the master's kill endpoint
    (the same POST the master UI's '(kill)' link sends). Returns how many were targeted."""
    try:
        apps = running_backfill_apps()
    except requests.RequestException as exc:
        print(f"Could not reach the Spark master to stop the app: {exc}")
        return 0
    for app in apps:
        try:
            requests.post(
                f"{SPARK_MASTER_UI}/app/kill/",
                data={"id": app["id"], "terminate": "true"},
                timeout=10,
                allow_redirects=False,
            )
            print(f"Asked the master to kill {app['id']}")
        except requests.RequestException as exc:
            print(f"Could not kill {app['id']}: {exc}")
    return len(apps)


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------
def acquire_lock():
    """One backfill.py per project checkout. The OS drops the lock when the process dies."""
    handle = open(LOCK_PATH, "w")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit(
            "Another backfill.py is already running here (lock held: "
            f"{LOCK_PATH}). Wait for it, or pass --force to override."
        )
    return handle


def preflight(args):
    try:
        s3_client().head_bucket(Bucket=S3_BUCKET)
    except Exception as exc:
        raise SystemExit(
            f"Preflight failed: cannot reach MinIO bucket '{S3_BUCKET}' at {S3_ENDPOINT} ({exc}).\n"
            "Is the stack up?  docker compose up -d spark-worker"
        )
    if args.download_only:
        return
    try:
        apps = running_backfill_apps()
    except requests.RequestException as exc:
        raise SystemExit(
            f"Preflight failed: cannot reach the Spark master UI at {SPARK_MASTER_UI} ({exc}).\n"
            "Is the stack up?  docker compose up -d spark-worker"
        )
    if apps and not args.force:
        ids = ", ".join(a["id"] for a in apps)
        raise SystemExit(
            f"A backfill Spark application is already running on the cluster ({ids}). "
            "Starting a second writer can duplicate rows. Wait for it, stop it from "
            f"{SPARK_MASTER_UI}, or pass --force."
        )


# ---------------------------------------------------------------------------
# GDELT download
# ---------------------------------------------------------------------------
def download_one(timestamp: str, stream: str, max_retries: int) -> DownloadResult:
    key = f"{stream}/{timestamp}.csv"
    url = f"{BASE_URL}/{timestamp}.{STREAM_SUFFIX[stream]}"
    client = s3_client()

    try:
        if object_exists(client, key):
            return DownloadResult(timestamp, stream, "already_exists")
    except Exception as exc:
        return DownloadResult(timestamp, stream, "failed", error=f"S3 HEAD failed: {exc}")

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)

            # 404 is special for GDELT: the file may still be processing, or a slot may
            # genuinely never be published. Retry with backoff, then report 'missing'.
            if response.status_code == 404:
                if attempt < max_retries:
                    wait = RETRY_BACKOFF[min(attempt - 1, len(RETRY_BACKOFF) - 1)]
                    print(f"[{timestamp}] {stream}: 404 (attempt {attempt}/{max_retries}), retrying in {wait}s")
                    time.sleep(wait)
                    continue
                return DownloadResult(timestamp, stream, "missing", error="GDELT returned 404 after retries")

            response.raise_for_status()

            # Unzip one file in memory; workers are bounded, so memory is bounded.
            with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
                members = archive.namelist()
                if not members:
                    raise RuntimeError("Downloaded ZIP contains no files")
                data = archive.read(members[0])

            client.put_object(Bucket=S3_BUCKET, Key=key, Body=data)
            return DownloadResult(timestamp, stream, "downloaded", bytes_written=len(data))

        except (requests.RequestException, zipfile.BadZipFile, ClientError) as exc:
            if attempt >= max_retries:
                return DownloadResult(timestamp, stream, "failed", error=str(exc))
            wait = RETRY_BACKOFF[min(attempt - 1, len(RETRY_BACKOFF) - 1)]
            print(f"[{timestamp}] {stream}: {type(exc).__name__} (attempt {attempt}/{max_retries}), retrying in {wait}s")
            time.sleep(wait)
        except Exception as exc:
            return DownloadResult(timestamp, stream, "failed", error=str(exc))

    return DownloadResult(timestamp, stream, "failed", error="Unknown download failure")


def download_chunk(start: date, end: date, streams: list, workers: int, retries: int):
    """Download one chunk with bounded parallelism.
    Returns (failed_results, missing_results, counts)."""
    tasks = [(ts, stream) for ts in timestamp_range(start, end) for stream in streams]
    total = len(tasks)

    print()
    print("=" * 72)
    print(f"Downloading {start} -> {end}")
    print(f"Slots: {total // len(streams)} | Streams: {', '.join(streams)} | Files: {total} | Workers: {workers}")
    print("=" * 72)

    counts = {"downloaded": 0, "already_exists": 0, "missing": 0, "failed": 0}
    failed, missing = [], []
    completed = 0

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=workers)
    try:
        future_map = {
            executor.submit(download_one, ts, stream, retries): (ts, stream) for ts, stream in tasks
        }
        for future in concurrent.futures.as_completed(future_map):
            ts, stream = future_map[future]
            try:
                result = future.result()
            except Exception as exc:
                result = DownloadResult(ts, stream, "failed", error=str(exc))

            completed += 1
            counts[result.status] += 1
            if result.status == "failed":
                failed.append(result)
            elif result.status == "missing":
                missing.append(result)

            if result.status == "downloaded":
                print(f"[{completed}/{total}] {ts} {stream}: downloaded ({result.bytes_written / (1024 * 1024):.1f} MB)")
            elif result.status == "already_exists":
                print(f"[{completed}/{total}] {ts} {stream}: already exists")
            elif result.status == "missing":
                print(f"[{completed}/{total}] {ts} {stream}: MISSING")
            else:
                print(f"[{completed}/{total}] {ts} {stream}: FAILED: {result.error}")
    except KeyboardInterrupt:
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    else:
        executor.shutdown(wait=True)

    print()
    print("Download summary:")
    print(f"  Downloaded:      {counts['downloaded']}")
    print(f"  Already existed: {counts['already_exists']}")
    print(f"  Missing:         {counts['missing']}")
    print(f"  Failed:          {counts['failed']}")
    return failed, missing, counts


# ---------------------------------------------------------------------------
# Spark ingestion
# ---------------------------------------------------------------------------
def run_spark_ingestion(start: date, end: date, streams: list):
    """Run spark/jobs/backfill_ingest.py inside the spark-master container
    (spark/jobs is mounted at /opt/jobs there)."""
    command = [
        "docker", "compose", "exec", "-T", "spark-master",
        "spark-submit", "--master", "spark://spark-master:7077",
        "/opt/jobs/backfill_ingest.py",
        "--start-date", start.strftime("%Y%m%d"),
        "--end-date", end.strftime("%Y%m%d"),
        "--streams", *streams,
    ]
    print()
    print("=" * 72)
    print(f"Starting Spark ingestion: {start} -> {end}")
    print("=" * 72)
    print("Command:", " ".join(command))

    result = subprocess.run(command, check=False, stdin=subprocess.DEVNULL)
    if result.returncode != 0:
        raise RuntimeError(
            f"Spark backfill exited with code {result.returncode} "
            "(look for Traceback or MISMATCH lines above)"
        )
    print()
    print(f"Spark ingestion completed successfully: {start} -> {end}")


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def validate_arguments(args):
    if args.start_date > args.end_date:
        raise SystemExit("--start-date cannot be after --end-date")
    if args.workers < 1:
        raise SystemExit("--workers must be >= 1")
    if args.retries < 1:
        raise SystemExit("--retries must be >= 1")
    if args.chunk_days < 1:
        raise SystemExit("--chunk-days must be >= 1")
    invalid = set(args.streams) - set(DEFAULT_STREAMS)
    if invalid:
        raise SystemExit(f"Invalid streams: {', '.join(sorted(invalid))}. Valid: {', '.join(DEFAULT_STREAMS)}")
    if args.download_only and args.ingest_only:
        raise SystemExit("--download-only and --ingest-only cannot be used together")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def _on_sigterm(signum, frame):
    raise KeyboardInterrupt


def run(args):
    start, end = args.start_date, args.end_date
    total_days = (end - start).days + 1

    print()
    print("=" * 72)
    print("GDELT BACKFILL")
    print("=" * 72)
    print(f"Date range:    {start} -> {end}")
    print(f"Days:          {total_days:,}")
    print(f"15-min slots:  {total_days * 96:,}")
    print(f"Files:         {total_days * 96 * len(args.streams):,}")
    print(f"Streams:       {', '.join(args.streams)}")
    print(f"Workers:       {args.workers}")
    print(f"Chunk size:    {args.chunk_days} days")
    print(f"S3 endpoint:   {S3_ENDPOINT}")
    print(f"S3 bucket:     {S3_BUCKET}")
    print("=" * 72)

    chunks = list(split_into_chunks(start, end, args.chunk_days))
    print(f"\nBackfill will run in {len(chunks)} chunk(s).")

    all_failed, all_missing, skipped_chunks = [], [], []
    totals = {"downloaded": 0, "already_exists": 0, "missing": 0, "failed": 0}

    # Each chunk is downloaded, then ingested. If chunk 27 fails, chunks 1-26 are done and a
    # re-run skips them (existing files are not re-downloaded; complete days are not re-ingested).
    for index, (chunk_start, chunk_end) in enumerate(chunks, start=1):
        print()
        print("#" * 72)
        print(f"CHUNK {index}/{len(chunks)}: {chunk_start} -> {chunk_end}")
        print("#" * 72)

        if not args.ingest_only:
            failed, missing, counts = download_chunk(
                chunk_start, chunk_end, args.streams, args.workers, args.retries
            )
            all_failed.extend(failed)
            all_missing.extend(missing)
            for k, v in counts.items():
                totals[k] += v

            # Never ingest a chunk with failed downloads (a 404 'missing' slot is fine).
            if failed:
                print()
                print(f"Chunk {chunk_start} -> {chunk_end} has {len(failed)} failed download(s).")
                print("Spark ingestion will NOT run for this chunk. Re-run the same command to retry.")
                skipped_chunks.append((chunk_start, chunk_end))
                continue

        if not args.download_only:
            try:
                run_spark_ingestion(chunk_start, chunk_end, args.streams)
            except Exception as exc:
                print()
                print(f"ERROR: Spark ingestion failed for {chunk_start} -> {chunk_end}")
                print(f"Reason: {exc}")
                print("Fix the cause and re-run the same command: downloaded files are kept "
                      "and days that are already complete are skipped.")
                raise SystemExit(1)

    # -------------------------------------------------------------------
    # Final summary
    # -------------------------------------------------------------------
    incomplete = bool(all_failed or skipped_chunks)
    print()
    print("=" * 72)
    print("BACKFILL INCOMPLETE" if incomplete else "BACKFILL COMPLETE")
    print("=" * 72)
    print(f"Range:   {start} -> {end}")
    print(f"Streams: {', '.join(args.streams)}")
    if not args.ingest_only:
        print(f"Downloads: {totals['downloaded']} new, {totals['already_exists']} already present, "
              f"{totals['missing']} missing (404), {totals['failed']} failed")

    if all_failed:
        print()
        print("Failed downloads:")
        for r in all_failed[:100]:
            print(f"  {r.timestamp} {r.stream}: {r.error}")
        if len(all_failed) > 100:
            print(f"  ... and {len(all_failed) - 100} more")
    if skipped_chunks:
        print()
        print("Chunks NOT ingested because of failed downloads:")
        for s, e in skipped_chunks:
            print(f"  {s} -> {e}")
    if all_missing:
        print()
        print(f"WARNING: {len(all_missing)} file(s) are not published by GDELT (404 after retries); "
              "their slots are simply absent:")
        for r in all_missing[:100]:
            print(f"  {r.timestamp} {r.stream}")
        if len(all_missing) > 100:
            print(f"  ... and {len(all_missing) - 100} more")
    if incomplete:
        print()
        print("Re-run the same command to retry; finished work is skipped.")

    if incomplete or (args.fail_on_missing and all_missing):
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Download and backfill GDELT data into the Iceberg Bronze tables."
    )
    parser.add_argument("--start-date", required=True, type=parse_date, help="First date, YYYYMMDD")
    parser.add_argument("--end-date", required=True, type=parse_date, help="Last date, YYYYMMDD, inclusive")
    parser.add_argument("--streams", nargs="+", default=DEFAULT_STREAMS, choices=DEFAULT_STREAMS,
                        help="Streams to backfill. Default: events mentions gkg")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Parallel download workers. Default: {DEFAULT_WORKERS}")
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES,
                        help=f"Attempts per download. Default: {DEFAULT_RETRIES}")
    parser.add_argument("--chunk-days", type=int, default=DEFAULT_CHUNK_DAYS,
                        help=f"Days per download+Spark chunk. Default: {DEFAULT_CHUNK_DAYS}")
    parser.add_argument("--download-only", action="store_true",
                        help="Download files but do not run Spark ingestion.")
    parser.add_argument("--ingest-only", action="store_true",
                        help="Skip downloading; ingest the files already in MinIO.")
    parser.add_argument("--force", action="store_true",
                        help="Skip the 'another backfill is running' guards.")
    parser.add_argument("--fail-on-missing", action="store_true",
                        help="Exit 1 if GDELT never published some slots (default: warn only).")
    args = parser.parse_args()
    validate_arguments(args)

    signal.signal(signal.SIGTERM, _on_sigterm)

    lock = None
    if not args.force:
        lock = acquire_lock()      # keep a reference so it is held until the process exits
    preflight(args)

    try:
        run(args)
    except KeyboardInterrupt:
        print("\nInterrupted. Stopping this backfill's Spark application on the master...", flush=True)
        if not args.download_only:
            stopped = kill_backfill_apps()
            print(f"Targeted {stopped} Spark application(s).")
        print("Re-run the same command to resume; finished work is skipped.", flush=True)
        os._exit(130)   # do not wait for download threads that are sleeping in backoff


if __name__ == "__main__":
    main()