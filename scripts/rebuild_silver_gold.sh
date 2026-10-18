#!/usr/bin/env bash
# Rebuild Silver (with the current silver_ddl.py) and Gold from Bronze, then verify.
# Safe to re-run: Silver and Gold are full recomputes. Bronze is never modified.
# Stops at the first failure and prints where the log is.
# Usage (from anywhere):  bash scripts/rebuild_silver_gold.sh
set -euo pipefail
cd "$(dirname "$0")/.."

CATALOG="http://localhost:8181"
SILVER_TABLES="$CATALOG/v1/namespaces/silver/tables"
SUBMIT="docker compose exec -T spark-master spark-submit --master spark://spark-master:7077"
MC="docker run --rm --network lakehouse-net --entrypoint /bin/sh minio/mc -c"
LOGDIR="logs/rebuild_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$LOGDIR"

step() { printf '\n=== %s ===\n' "$1"; }

# run_job <name> <script> <grep-pattern-for-the-lines-to-show>
run_job() {
  local name=$1 script=$2 pattern=$3
  step "$name"
  if ! $SUBMIT "/opt/jobs/$script" > "$LOGDIR/$name.log" 2>&1; then
    echo "FAILED: $script. Last log lines:"
    tail -n 15 "$LOGDIR/$name.log" | cut -c1-200
    echo "Full log: $LOGDIR/$name.log"
    exit 1
  fi
  grep -hE "$pattern" "$LOGDIR/$name.log" | cut -c1-200 || true
  if grep -q "PROBLEM" "$LOGDIR/$name.log"; then
    echo "CHECK FAILED: $script reported PROBLEM (full log: $LOGDIR/$name.log)"
    exit 1
  fi
}

step "Preflight"
if ! curl -sf "$CATALOG/v1/config" > /dev/null; then
  echo "Iceberg REST catalog not reachable at $CATALOG."
  echo "Start the stack first:  docker compose up -d spark-worker   (then wait ~20 s)"
  exit 1
fi
echo "catalog reachable"

# Bronze is the base for everything below; refuse to rebuild on top of a damaged one.
run_job verify_bronze verify_bronze.py "total="

step "Unregister the Silver tables from the catalog"
echo "before: $(curl -sf "$SILVER_TABLES" || echo '<could not list>')"
for t in events_india mentions_enriched; do
  code=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "$SILVER_TABLES/$t" || true)
  echo "$t: HTTP $code (200/204 = removed, 404 = was not registered)"
  case "$code" in
    200|204|404) ;;
    *) echo "unexpected status from the catalog, stopping"; exit 1 ;;
  esac
done
after=$(curl -sf "$SILVER_TABLES" || echo '<could not list>')
echo "after:  $after"
# Trust the listing, not the status codes: nothing may remain registered.
if [[ "$after" != *'"identifiers":[]'* ]]; then
  echo "Silver tables are still registered after the delete, stopping"
  exit 1
fi

step "Remove leftover Silver files from MinIO"
$MC "mc alias set local http://minio:9000 admin changeme123 >/dev/null && (mc rm --recursive --force local/warehouse/silver || true); mc ls --summarize --recursive local/warehouse/silver | tail -n 2" || true

run_job bootstrap_silver bootstrap_silver.py "events_india|mentions_enriched"
run_job silver_transform silver_transform.py "rows written"
run_job gold_aggregate gold_aggregate.py "rows written|merged against"
run_job verify_silver verify_silver.py "total=|match_rate"
run_job verify_gold verify_gold.py "total=|distinct GLOBALEVENTID"

step "Table health (Silver and Gold)"
if ! $SUBMIT /opt/jobs/table_health.py > "$LOGDIR/table_health.log" 2>&1; then
  echo "FAILED: table_health.py (full log: $LOGDIR/table_health.log)"
  exit 1
fi
sed -n '/=== TABLE HEALTH ===/,/=== SILVER EVENT DATES ===/p' "$LOGDIR/table_health.log" | grep -E "^(silver|gold)\." || true

printf '\nREBUILD COMPLETE. Logs in %s\n' "$LOGDIR"