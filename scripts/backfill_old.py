import argparse
import concurrent.futures
import io
import os
import subprocess
import sys
import time
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional
import boto3
import requests
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
# Retry delays for transient failures.
RETRY_BACKOFF = [15, 30, 60, 120]
# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass
class DownloadResult:
    timestamp: str
    stream: str
    status: str
    bytes_written: int = 0
    error: Optional[str] = None
@dataclass
class ChunkResult:
    start_date: date
    end_date: date
    success: bool
# ---------------------------------------------------------------------------
# Date / timestamp helpers
# ---------------------------------------------------------------------------
def parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}'. Expected YYYYMMDD."
        )
def daterange(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)
def timestamp_range(start: date, end: date):
    """
    Generate every 15-minute GDELT timestamp in the inclusive date range.
    96 timestamps per day.
    """
    current = datetime.combine(start, datetime.min.time())
    final = datetime.combine(end, datetime.max.time())
    while current <= final:
        yield current.strftime("%Y%m%d%H%M%S")
        current += timedelta(minutes=SLOT_MINUTES)
def split_into_chunks(
    start: date,
    end: date,
    chunk_days: int,
):
    """
    Split a potentially huge backfill into bounded date ranges.
    Example:
        2026-09-01 -> 2026-09-30
        chunk_days=7
    becomes:
        09-01 -> 09-07
        09-08 -> 09-14
        09-15 -> 09-21
        09-22 -> 09-28
        09-29 -> 09-30
    """
    current = start
    while current <= end:
        chunk_end = min(
            current + timedelta(days=chunk_days - 1),
            end,
        )
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
    )
def object_exists(client, key: str) -> bool:
    """
    Check whether the landing object already exists.
    This is what makes the downloader resumable.
    """
    try:
        client.head_object(
            Bucket=S3_BUCKET,
            Key=key,
        )
        return True
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise
# ---------------------------------------------------------------------------
# GDELT download
# ---------------------------------------------------------------------------
def download_one(
    timestamp: str,
    stream: str,
    max_retries: int,
) -> DownloadResult:
    key = f"{stream}/{timestamp}.csv"
    url = (
        f"{BASE_URL}/"
        f"{timestamp}."
        f"{STREAM_SUFFIX[stream]}"
    )
    client = s3_client()
    # ---------------------------------------------------------------
    # Resume support
    # ---------------------------------------------------------------
    try:
        if object_exists(client, key):
            return DownloadResult(
                timestamp=timestamp,
                stream=stream,
                status="already_exists",
            )
    except Exception as exc:
        return DownloadResult(
            timestamp=timestamp,
            stream=stream,
            status="failed",
            error=f"S3 HEAD failed: {exc}",
        )
    # ---------------------------------------------------------------
    # Download
    # ---------------------------------------------------------------
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(
                url,
                timeout=REQUEST_TIMEOUT,
            )
            # -------------------------------------------------------
            # 404 is special for GDELT.
            #
            # A file can temporarily be unavailable because GDELT is
            # still processing it.
            # -------------------------------------------------------
            if response.status_code == 404:
                if attempt < max_retries:
                    wait = RETRY_BACKOFF[
                        min(attempt - 1, len(RETRY_BACKOFF) - 1)
                    ]
                    print(
                        f"[{timestamp}] "
                        f"{stream}: 404 "
                        f"(attempt {attempt}/{max_retries}), "
                        f"retrying in {wait}s"
                    )
                    time.sleep(wait)
                    continue
                return DownloadResult(
                    timestamp=timestamp,
                    stream=stream,
                    status="missing",
                    error="GDELT returned 404 after retries",
                )
            response.raise_for_status()
            # -------------------------------------------------------
            # Extract ZIP completely in memory for ONE file only.
            #
            # Because workers are bounded, memory usage is bounded.
            # -------------------------------------------------------
            with zipfile.ZipFile(
                io.BytesIO(response.content)
            ) as archive:
                members = archive.namelist()
                if not members:
                    raise RuntimeError(
                        "Downloaded ZIP contains no files"
                    )
                data = archive.read(members[0])
            # -------------------------------------------------------
            # Upload to MinIO
            # -------------------------------------------------------
            client.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=data,
            )
            return DownloadResult(
                timestamp=timestamp,
                stream=stream,
                status="downloaded",
                bytes_written=len(data),
            )
        except (
            requests.RequestException,
            zipfile.BadZipFile,
            ClientError,
        ) as exc:
            if attempt >= max_retries:
                return DownloadResult(
                    timestamp=timestamp,
                    stream=stream,
                    status="failed",
                    error=str(exc),
                )
            wait = RETRY_BACKOFF[
                min(attempt - 1, len(RETRY_BACKOFF) - 1)
            ]
            print(
                f"[{timestamp}] "
                f"{stream}: {type(exc).__name__} "
                f"(attempt {attempt}/{max_retries}), "
                f"retrying in {wait}s"
            )
            time.sleep(wait)
        except Exception as exc:
            return DownloadResult(
                timestamp=timestamp,
                stream=stream,
                status="failed",
                error=str(exc),
            )
    return DownloadResult(
        timestamp=timestamp,
        stream=stream,
        status="failed",
        error="Unknown download failure",
    )
# ---------------------------------------------------------------------------
# Parallel downloading
# ---------------------------------------------------------------------------
def download_chunk(
    start: date,
    end: date,
    streams: list[str],
    workers: int,
    retries: int,
):
    """
    Download one chunk using bounded parallelism.
    We submit only timestamp/stream identifiers to workers.
    The actual file contents never accumulate in the main process.
    """
    tasks = [
        (timestamp, stream)
        for timestamp in timestamp_range(start, end)
        for stream in streams
    ]
    total = len(tasks)
    print()
    print("=" * 72)
    print(
        f"Downloading {start} -> {end}"
    )
    print(
        f"Slots: {total // len(streams)}"
        f" | Streams: {', '.join(streams)}"
        f" | Files: {total}"
        f" | Workers: {workers}"
    )
    print("=" * 72)
    counts = {
        "downloaded": 0,
        "already_exists": 0,
        "missing": 0,
        "failed": 0,
    }
    failed_results = []
    completed = 0
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=workers
    ) as executor:
        future_map = {
            executor.submit(
                download_one,
                timestamp,
                stream,
                retries,
            ): (timestamp, stream)
            for timestamp, stream in tasks
        }
        for future in concurrent.futures.as_completed(
            future_map
        ):
            timestamp, stream = future_map[future]
            try:
                result = future.result()
            except Exception as exc:
                result = DownloadResult(
                    timestamp=timestamp,
                    stream=stream,
                    status="failed",
                    error=str(exc),
                )
            completed += 1
            counts[result.status] += 1
            if result.status == "failed":
                failed_results.append(result)
            if result.status == "downloaded":
                size_mb = result.bytes_written / (1024 * 1024)
                print(
                    f"[{completed}/{total}] "
                    f"{timestamp} {stream}: "
                    f"downloaded ({size_mb:.1f} MB)"
                )
            elif result.status == "already_exists":
                print(
                    f"[{completed}/{total}] "
                    f"{timestamp} {stream}: "
                    f"already exists"
                )
            elif result.status == "missing":
                print(
                    f"[{completed}/{total}] "
                    f"{timestamp} {stream}: "
                    f"MISSING"
                )
            else:
                print(
                    f"[{completed}/{total}] "
                    f"{timestamp} {stream}: "
                    f"FAILED: {result.error}"
                )
    print()
    print("Download summary:")
    print(f"  Downloaded:      {counts['downloaded']}")
    print(f"  Already existed: {counts['already_exists']}")
    print(f"  Missing:         {counts['missing']}")
    print(f"  Failed:          {counts['failed']}")
    return failed_results
# ---------------------------------------------------------------------------
# Spark ingestion
# ---------------------------------------------------------------------------
def run_spark_ingestion(
    start: date,
    end: date,
    streams: list[str],
):
    """
    Invoke the optimized Spark backfill job inside spark-master.
    scripts/ is not mounted into the Spark container, but
    spark/jobs/ is mounted at /opt/jobs, so we execute the existing
    /opt/jobs/backfill_ingest.py there.
    """
    command = [
        "docker",
        "compose",
        "exec",
        "-T",
        "spark-master",
        "spark-submit",
        "--master",
        "spark://spark-master:7077",
        "/opt/jobs/backfill_ingest.py",
        "--start-date",
        start.strftime("%Y%m%d"),
        "--end-date",
        end.strftime("%Y%m%d"),
        "--streams",
        *streams,
    ]
    print()
    print("=" * 72)
    print(
        f"Starting Spark ingestion: "
        f"{start} -> {end}"
    )
    print("=" * 72)
    print(
        "Command:",
        " ".join(command),
    )
    result = subprocess.run(
        command,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Spark backfill failed with exit code "
            f"{result.returncode}"
        )
    print()
    print(
        f"Spark ingestion completed successfully: "
        f"{start} -> {end}"
    )
# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def validate_arguments(args):
    if args.start_date > args.end_date:
        raise SystemExit(
            "--start-date cannot be after --end-date"
        )
    if args.workers < 1:
        raise SystemExit(
            "--workers must be >= 1"
        )
    if args.retries < 1:
        raise SystemExit(
            "--retries must be >= 1"
        )
    if args.chunk_days < 1:
        raise SystemExit(
            "--chunk-days must be >= 1"
        )
    invalid = set(args.streams) - set(DEFAULT_STREAMS)
    if invalid:
        raise SystemExit(
            f"Invalid streams: {', '.join(sorted(invalid))}. "
            f"Valid streams: {', '.join(DEFAULT_STREAMS)}"
        )
    if args.download_only and args.ingest_only:
        raise SystemExit(
            "--download-only and --ingest-only "
            "cannot be used together"
        )
# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description=(
            "Download and backfill GDELT data into "
            "the Iceberg bronze tables."
        )
    )
    parser.add_argument(
        "--start-date",
        required=True,
        type=parse_date,
        help="First date, YYYYMMDD",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        type=parse_date,
        help="Last date, YYYYMMDD, inclusive",
    )
    parser.add_argument(
        "--streams",
        nargs="+",
        default=DEFAULT_STREAMS,
        choices=DEFAULT_STREAMS,
        help=(
            "Streams to backfill. "
            "Default: events mentions gkg"
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=(
            f"Parallel download workers. "
            f"Default: {DEFAULT_WORKERS}"
        ),
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=DEFAULT_RETRIES,
        help=(
            f"Retries for failed downloads. "
            f"Default: {DEFAULT_RETRIES}"
        ),
    )
    parser.add_argument(
        "--chunk-days",
        type=int,
        default=DEFAULT_CHUNK_DAYS,
        help=(
            f"Number of days processed per Spark chunk. "
            f"Default: {DEFAULT_CHUNK_DAYS}"
        ),
    )
    parser.add_argument(
        "--download-only",
        action="store_true",
        help="Download files but do not run Spark ingestion.",
    )
    parser.add_argument(
        "--ingest-only",
        action="store_true",
        help=(
            "Skip downloading and ingest the files "
            "already present in MinIO."
        ),
    )
    args = parser.parse_args()
    validate_arguments(args)
    start = args.start_date
    end = args.end_date
    total_days = (
        end - start
    ).days + 1
    total_slots = total_days * 96
    total_files = total_slots * len(args.streams)
    print()
    print("=" * 72)
    print("GDELT BACKFILL")
    print("=" * 72)
    print(f"Date range:    {start} -> {end}")
    print(f"Days:          {total_days:,}")
    print(f"15-min slots:  {total_slots:,}")
    print(f"Files:         {total_files:,}")
    print(f"Streams:       {', '.join(args.streams)}")
    print(f"Workers:       {args.workers}")
    print(f"Chunk size:    {args.chunk_days} days")
    print(f"S3 endpoint:   {S3_ENDPOINT}")
    print(f"S3 bucket:     {S3_BUCKET}")
    print("=" * 72)
    chunks = list(
        split_into_chunks(
            start,
            end,
            args.chunk_days,
        )
    )
    print(
        f"\nBackfill will run in {len(chunks)} chunk(s)."
    )
    all_failed = []
    # ---------------------------------------------------------------
    # Process one bounded chunk at a time.
    #
    # This is the important part for very large backfills.
    #
    # Example:
    #
    #   365 days + chunk_days=7
    #
    # becomes roughly 53 independent download/ingestion stages.
    #
    # If chunk 27 fails, chunks 1-26 are already complete.
    # ---------------------------------------------------------------
    for index, (chunk_start, chunk_end) in enumerate(
        chunks,
        start=1,
    ):
        print()
        print("#" * 72)
        print(
            f"CHUNK {index}/{len(chunks)}: "
            f"{chunk_start} -> {chunk_end}"
        )
        print("#" * 72)
        # -----------------------------------------------------------
        # Download
        # -----------------------------------------------------------
        if not args.ingest_only:
            failed = download_chunk(
                chunk_start,
                chunk_end,
                args.streams,
                args.workers,
                args.retries,
            )
            all_failed.extend(failed)
            # Don't send incomplete chunks into Spark unless the user
            # explicitly chooses ingest-only mode.
            if failed:
                print()
                print(
                    f"Chunk {chunk_start} -> {chunk_end} "
                    f"has {len(failed)} failed download(s)."
                )
                print(
                    "Spark ingestion will NOT run for this chunk."
                )
                print(
                    "Fix/re-run the backfill; existing files "
                    "will automatically be skipped."
                )
                continue
        # -----------------------------------------------------------
        # Spark ingestion
        # -----------------------------------------------------------
        if not args.download_only:
            try:
                run_spark_ingestion(
                    chunk_start,
                    chunk_end,
                    args.streams,
                )
            except Exception as exc:
                print()
                print(
                    f"ERROR: Spark ingestion failed for "
                    f"{chunk_start} -> {chunk_end}"
                )
                print(f"Reason: {exc}")
                print(
                    "You can re-run this same backfill. "
                    "Downloads already present in MinIO "
                    "will be skipped."
                )
                raise SystemExit(1)
    # ---------------------------------------------------------------
    # Final summary
    # ---------------------------------------------------------------
    print()
    print("=" * 72)
    print("BACKFILL COMPLETE")
    print("=" * 72)
    if all_failed:
        print(
            f"Download failures/missing files: "
            f"{len(all_failed)}"
        )
        print()
        print("Failed downloads:")
        for result in all_failed[:100]:
            print(
                f"  {result.timestamp} "
                f"{result.stream}: "
                f"{result.error}"
            )
        if len(all_failed) > 100:
            print(
                f"  ... and "
                f"{len(all_failed) - 100} more"
            )
        print()
        print(
            "Re-run the same command to retry them. "
            "Existing successful downloads are skipped."
        )
    else:
        print(
            "All requested downloads completed or "
            "were already present."
        )
    print(
        f"Range: {start} -> {end}"
    )
    print(
        f"Streams: {', '.join(args.streams)}"
    )
if __name__ == "__main__":
    main()