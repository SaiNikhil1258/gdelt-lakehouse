#!/usr/bin/env bash
# Try to pull every image the pipeline test needs and report ALL the ones that fail, so one CI
# run shows every dead image instead of one per run. Exits 1 if any pull failed.
# (minio/minio and minio/mc are skipped: ci/docker-compose.ci.yml replaces them.)
set -uo pipefail
cd "$(dirname "$0")/.."

images=$(
  {
    grep -E '^\s+image:' docker-compose.yml | awk '{print $2}'
    grep -h '^FROM' spark/Dockerfile | awk '{print $2}'
  } | grep -v -E '^(minio/minio|minio/mc|gdelt-)' | sort -u
)

failed=0
for image in $images; do
  if docker pull -q "$image" > /dev/null 2> /tmp/pull_err.txt; then
    echo "ok       $image"
  else
    echo "MISSING  $image  -- $(head -n 1 /tmp/pull_err.txt | cut -c1-160)"
    failed=1
  fi
done
exit $failed