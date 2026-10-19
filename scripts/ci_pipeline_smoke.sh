#!/usr/bin/env bash
# CI pipeline test: Compose stack -> synthetic sample -> Bronze -> Silver -> Gold -> checks.
# Needs a .env (in CI: cp ci/ci.env .env).
# Usage: bash scripts/ci_pipeline_smoke.sh
#
# WARNING: this writes fake sample rows into Bronze and then REBUILDS Silver and Gold from
# Bronze. Never run it against your real lakehouse. It only runs where CI=true (GitHub sets
# that), or on a throw-away stack if you set I_AM_ON_A_THROWAWAY_STACK=yes.
set -euo pipefail
cd "$(dirname "$0")/.."

# Use the CI override (MinIO built from source) for every `docker compose` call below.
export COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml:ci/docker-compose.ci.yml}"
MINIO_IMAGE="minio-local:RELEASE.2025-10-15T17-29-55Z"

if [ "${CI:-}" != "true" ] && [ "${I_AM_ON_A_THROWAWAY_STACK:-}" != "yes" ]; then
  echo "Refusing to run: this would add fake rows to the lakehouse that is running here."
  echo "It is meant for the CI runner. For a throw-away local stack, set I_AM_ON_A_THROWAWAY_STACK=yes."
  exit 1
fi

DAY=20260915
SAMPLE_DIR="${SAMPLE_DIR:-/tmp/ci_sample}"
LOGDIR="${LOGDIR:-logs/ci_smoke}"
SUBMIT="docker compose exec -T spark-master spark-submit --master spark://spark-master:7077"
mkdir -p "$LOGDIR"

step() { printf '\n=== %s ===\n' "$1"; }

# run_job <log-name> <script> [args...]
run_job() {
  local name=$1 script=$2
  shift 2
  step "$name"
  if ! $SUBMIT "/opt/jobs/$script" "$@" > "$LOGDIR/$name.log" 2>&1 < /dev/null; then
    echo "FAILED: $script"
    grep -hE "^FAIL|^SUMMARY" "$LOGDIR/$name.log" | cut -c1-240 || true
    echo "--- last log lines ---"
    tail -n 25 "$LOGDIR/$name.log" | cut -c1-240
    exit 1
  fi
  grep -hE "^(PASS|FAIL)|^SUMMARY|rows written|merged against|new rows|ready" "$LOGDIR/$name.log" | cut -c1-240 || true
}

step "Start the stack"
docker compose up -d --build spark-worker
ready=0
for _ in $(seq 1 60); do
  workers=$(curl -sf http://localhost:8080/json/ 2>/dev/null \
    | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["aliveworkers"]))' 2>/dev/null || echo 0)
  if curl -sf http://localhost:8181/v1/config > /dev/null && [ "$workers" -ge 1 ]; then
    ready=1
    break
  fi
  sleep 5
done
if [ "$ready" != 1 ]; then
  echo "Stack not ready after 5 minutes."
  docker compose ps -a
  exit 1
fi
echo "catalog reachable, $workers Spark worker(s) registered"

step "Generate the sample and upload it to the landing bucket"
rm -rf "$SAMPLE_DIR"
python3 scripts/make_ci_sample.py --out "$SAMPLE_DIR"
set -a; . ./.env; set +a
docker run --rm --network lakehouse-net -v "$SAMPLE_DIR":/sample:ro --entrypoint /bin/sh "$MINIO_IMAGE" -c \
  "mc alias set local http://minio:9000 '$MINIO_ROOT_USER' '$MINIO_ROOT_PASSWORD' > /dev/null && mc mirror /sample local/gdelt-landing"

run_job bootstrap_bronze bootstrap_bronze.py
run_job bootstrap_silver bootstrap_silver.py
run_job bootstrap_gold   bootstrap_gold.py
run_job bronze_ingest    backfill_ingest.py --date "$DAY"
run_job silver_transform silver_transform.py
run_job gold_aggregate   gold_aggregate.py
run_job verify_all       verify_all.py --start-date "$DAY" --end-date "$DAY" --slots-per-day 2
run_job sample_assertions ci_sample_assertions.py

printf '\nCI PIPELINE TEST PASSED. Logs in %s\n' "$LOGDIR"