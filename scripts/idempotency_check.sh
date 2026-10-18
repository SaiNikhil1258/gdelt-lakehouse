#!/usr/bin/env bash
# Prove that re-running ingest, Silver and Gold over a date range changes nothing.
#   Bronze:        the bulk re-ingest must add 0 rows in every stream; row counts must not move.
#   Silver / Gold: row count AND a content hash must be identical before and after.
# Finishes with the full verify_all suite for the same range.
#
# Usage: bash scripts/idempotency_check.sh [START_YYYYMMDD] [END_YYYYMMDD]   (default: 20260901 20260930)
# Needs the stack up and no Spark application running. Takes a while: it re-reads every landing file.
set -euo pipefail
cd "$(dirname "$0")/.."

START="${1:-20260901}"
END="${2:-20260930}"
LOGDIR="logs/idempotency_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$LOGDIR"

step() { printf '\n=== %s ===\n' "$1"; }
fail() { echo "FAIL: $1"; exit 1; }

# < /dev/null: `docker compose exec -T` still reads stdin and would freeze a background job.
spark() {
  local script=$1; shift
  docker compose exec -T spark-master spark-submit --master spark://spark-master:7077 \
    "/opt/jobs/$script" "$@" < /dev/null
}

step "Preflight"
curl -sf http://localhost:8181/v1/config > /dev/null \
  || fail "Iceberg catalog not reachable (start it: docker compose up -d spark-worker)"
active=$(curl -sf http://localhost:8080/json/ \
  | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["activeapps"]))' || echo "unreachable")
[[ "$active" == "0" ]] || fail "Spark master reports '$active' running application(s); stop them first"
echo "catalog reachable, no Spark application running, range $START -> $END"

step "Fingerprint BEFORE"
spark table_fingerprint.py > "$LOGDIR/before.log" 2>&1 \
  || { tail -n 15 "$LOGDIR/before.log" | cut -c1-200; fail "table_fingerprint.py failed (see $LOGDIR/before.log)"; }
grep '^FP ' "$LOGDIR/before.log" > "$LOGDIR/before.fp" || fail "no FP lines in $LOGDIR/before.log"
cat "$LOGDIR/before.fp"

step "Re-ingest $START -> $END (bulk, --no-skip)"
spark backfill_ingest.py --start-date "$START" --end-date "$END" --no-skip > "$LOGDIR/ingest.log" 2>&1 \
  || { grep -E "MISMATCH|Traceback" "$LOGDIR/ingest.log" | head -n 5 | cut -c1-200 || true
       fail "bulk re-ingest failed (see $LOGDIR/ingest.log)"; }
summary=$(grep -E "^(events|mentions|gkg): [0-9]+ days ingested" "$LOGDIR/ingest.log" || true)
echo "$summary" | cut -c1-200
[[ $(echo "$summary" | grep -c .) -eq 3 ]] || fail "expected 3 stream summary lines in $LOGDIR/ingest.log"
if echo "$summary" | grep -qv ", 0 new rows ("; then
  fail "re-ingest added rows (see the summary above)"
fi
echo "PASS: re-ingest added 0 rows in every stream"

step "Re-run Silver and Gold"
spark silver_transform.py > "$LOGDIR/silver.log" 2>&1 \
  || { tail -n 15 "$LOGDIR/silver.log" | cut -c1-200; fail "silver_transform.py failed (see $LOGDIR/silver.log)"; }
spark gold_aggregate.py > "$LOGDIR/gold.log" 2>&1 \
  || { tail -n 15 "$LOGDIR/gold.log" | cut -c1-200; fail "gold_aggregate.py failed (see $LOGDIR/gold.log)"; }
grep -hE "rows written|merged against" "$LOGDIR/silver.log" "$LOGDIR/gold.log" | cut -c1-200 || true

step "Fingerprint AFTER and compare"
spark table_fingerprint.py > "$LOGDIR/after.log" 2>&1 \
  || { tail -n 15 "$LOGDIR/after.log" | cut -c1-200; fail "table_fingerprint.py failed (see $LOGDIR/after.log)"; }
grep '^FP ' "$LOGDIR/after.log" > "$LOGDIR/after.fp" || fail "no FP lines in $LOGDIR/after.log"
if diff "$LOGDIR/before.fp" "$LOGDIR/after.fp" > "$LOGDIR/fp.diff"; then
  echo "PASS: all 8 table fingerprints identical before and after"
else
  echo "FAIL: fingerprints differ (< before, > after):"
  cat "$LOGDIR/fp.diff"
  exit 1
fi

step "verify_all"
if spark verify_all.py --start-date "$START" --end-date "$END" > "$LOGDIR/verify_all.log" 2>&1; then
  grep -E "^SUMMARY" "$LOGDIR/verify_all.log"
else
  grep -E "^(FAIL|SUMMARY)" "$LOGDIR/verify_all.log" || true
  fail "verify_all reported failures (see $LOGDIR/verify_all.log)"
fi

printf '\nIDEMPOTENCY CHECK PASSED. Logs in %s\n' "$LOGDIR  "