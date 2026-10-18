"""Ported from scripts/download_gdelt.py, adapted for in-container use
(minio:9000 instead of localhost:9000) plus the per-stream retry we
added after discovering GKG's slower-processing 404 behavior."""

import io
import os
import time
import zipfile
import requests
import boto3

MANIFEST_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

BASE_URL = "http://data.gdeltproject.org/gdeltv2"

STREAM_SUFFIX = {
    "events": "export.CSV.zip",
    "mentions": "mentions.CSV.zip",
    "gkg": "gkg.csv.zip",
}


def get_latest_timestamps() -> dict:
    resp = requests.get(
        MANIFEST_URL,
        timeout=30,
    )
    resp.raise_for_status()
    latest = {}
    for line in resp.text.strip().split("\n"):
        _, _, url = line.split()
        for stream, suffix in STREAM_SUFFIX.items():
            if url.endswith(suffix):
                latest[stream] = (
                    url.rsplit("/", 1)[-1]
                    .split(".")[0]
                )
    return latest


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def fetch_and_upload(
    timestamp: str, stream: str, client, max_retries=3, backoff_base=30
) -> bool:
    url = f"{BASE_URL}/{timestamp}.{STREAM_SUFFIX[stream]}"
    for attempt in range(1, max_retries + 1):
        resp = requests.get(url, timeout=60)
        if resp.status_code == 404:
            if attempt < max_retries:
                time.sleep(backoff_base * attempt)
                continue
            return False
        resp.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            data = z.read(z.namelist()[0])
        client.put_object(
            Bucket="gdelt-landing", Key=f"{stream}/{timestamp}.csv", Body=data
        )
        return True
    return False
