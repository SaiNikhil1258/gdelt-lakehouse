import argparse
import io
import time
import zipfile
import boto3
import requests

MANIFEST_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
BASE_URL = "http://data.gdeltproject.org/gdeltv2"

STREAM_SUFFIX = {
    "events": "export.CSV.zip",
    "mentions": "mentions.CSV.zip",
    "gkg": "gkg.csv.zip",
}

def get_latest_timestamp() -> str:
    resp = requests.get(MANIFEST_URL, timeout=30)
    resp.raise_for_status()
    first_line = resp.text.strip().split("\n")[0]
    _, _, url = first_line.split()
    return url.rsplit("/", 1)[-1].split(".")[0]

def s3_client():
    return boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="changeme123",
    )

def fetch_and_upload(timestamp: str, stream: str, client, max_retries=3, backoff_base=30):
    url = f"{BASE_URL}/{timestamp}.{STREAM_SUFFIX[stream]}"

    for attempt in range(1, max_retries + 1):
        resp = requests.get(url, timeout=60)

        if resp.status_code == 404:
            if attempt < max_retries:
                wait = backoff_base * attempt   # 30s, 60s, 90s
                print(f"  [{stream}] 404 on attempt {attempt}/{max_retries} — "
                      f"GDELT may still be processing this slot, retrying in {wait}s")
                time.sleep(wait)
                continue
            print(f"  [{stream}] SKIPPED after {max_retries} attempts — "
                  f"this slot genuinely has no {stream} file")
            return False

        resp.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            data = z.read(z.namelist()[0])

        key = f"{stream}/{timestamp}.csv"
        client.put_object(Bucket="gdelt-landing", Key=key, Body=data)
        print(f"  [{stream}] uploaded s3://gdelt-landing/{key} ({len(data):,} bytes)")
        return True

    return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--timestamp", help="YYYYMMDDHHMMSS, defaults to latest available")
    parser.add_argument("--streams", nargs="+", default=["events", "mentions", "gkg"])
    args = parser.parse_args()

    ts = args.timestamp or get_latest_timestamp()
    print(f"Batch timestamp: {ts}")

    client = s3_client()
    results = {stream: fetch_and_upload(ts, stream, client) for stream in args.streams}

    failed = [s for s, ok in results.items() if not ok]
    if failed:
        print(f"\nIncomplete batch — missing: {failed}")
    else:
        print(f"\nComplete batch: {ts}")