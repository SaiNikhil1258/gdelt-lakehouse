#!/usr/bin/env bash
# Test the tests: break the lakehouse on purpose, check that verify_all goes red on exactly the
# expected checks, undo the damage with Iceberg's rollback_to_snapshot, and check it is green again.
#
# SAFETY: before each fault the affected tables' current snapshot ids are written to
# <logdir>/pending_restore.txt, and an EXIT trap rolls them back even if this script fails midway.
# If the script is killed hard (kill -9, power loss), run the "--restore" command it printed.
#
# Usage: bash scripts/fault_injection_test.sh [START_YYYYMMDD] [END_YYYYMMDD]   (default: 20260901 20260930)
# Needs the stack up, no Spark application running, and Dagster OFF.
set -uo pipefail
cd "$(dirname "$0")/.."

START="${1:-20260901}"
END="${2:-20260930}"
LOGDIR="logs/fault_injection_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$LOGDIR"
PENDING="$LOGDIR/pending_restore.txt"
: > "$PENDING"
FAILED_SCENARIOS=0

step() { printf '\n=== %s ===\n' "$1"; }

# < /dev/null: `docker compose exec -T` still reads stdin and would freeze a background job.
spark() {
  local script=$1; shift
  docker compose exec -T spark-master spark-submit --master spark://spark-master:7077 \
    "/opt/jobs/$script" "$@" < /dev/null
}

restore_pending() {
  [[ -s "$PENDING" ]] || return 0
  local spec
  spec=$(paste -sd, "$PENDING")
  echo "  restoring: $spec"
  if spark inject_fault.py --restore "$spec" > "$LOGDIR/restore.log" 2>&1; then
    : > "$PENDING"
    echo "  restored"
  else
    echo "  RESTORE FAILED. Undo it manually with:"
    echo "    docker compose exec -T spark-master spark-submit --master spark://spark-master:7077 /opt/jobs/inject_fault.py --restore \"$spec\" < /dev/null"
    return 1
  fi
}
trap restore_pending EXIT

# run_scenario <name> <required-failing-check-substrings, '|'-separated> <allowed-extra-failures, '|'-separated>
run_scenario() {
  local name=$1 required=$2 allowed=$3
  local dir="$LOGDIR/$name" ok=1 rc
  mkdir -p "$dir"
  step "Scenario: $name"

  spark inject_fault.py --scenario "$name" > "$dir/inject.log" 2>&1
  rc=$?
  # Record the snapshot ids BEFORE judging the result, so a half-applied fault is still undone.
  grep '^SNAP ' "$dir/inject.log" | awk '{print $2 "=" $3}' > "$PENDING" || true
  grep -E '^(INJECTED|APPENDED|DELETED|BLANKED)' "$dir/inject.log" || true
  if [[ $rc -ne 0 ]]; then
    echo "  FAIL: the fault could not be injected (see $dir/inject.log)"
    tail -n 8 "$dir/inject.log" | cut -c1-200
    restore_pending
    FAILED_SCENARIOS=$((FAILED_SCENARIOS + 1))
    return 1
  fi

  spark verify_all.py --start-date "$START" --end-date "$END" > "$dir/verify_after_fault.log" 2>&1
  rc=$?
  grep -E '^FAIL ' "$dir/verify_after_fault.log" | cut -c1-220 > "$dir/fails.txt" || true
  echo "  checks that failed after the fault:"
  if [[ -s "$dir/fails.txt" ]]; then sed 's/^/    /' "$dir/fails.txt"; else echo "    (none)"; fi

  if [[ $rc -eq 0 ]]; then
    echo "  FAIL: verify_all PASSED although the data was broken"
    ok=0
  fi

  local -a req alw
  IFS='|' read -ra req <<< "$required"
  IFS='|' read -ra alw <<< "$allowed"
  local r a line matched
  for r in "${req[@]}"; do
    grep -qF -- "$r" "$dir/fails.txt" || { echo "  FAIL: expected check did not fail: $r"; ok=0; }
  done
  while IFS= read -r line; do
    matched=0
    for r in "${req[@]}"; do [[ "$line" == *"$r"* ]] && matched=1; done
    for a in "${alw[@]}"; do [[ -n "$a" && "$line" == *"$a"* ]] && matched=1; done
    if [[ $matched -eq 0 ]]; then echo "  FAIL: unexpected extra failure: $line"; ok=0; fi
  done < "$dir/fails.txt"

  restore_pending || ok=0
  spark verify_all.py --start-date "$START" --end-date "$END" > "$dir/verify_after_restore.log" 2>&1
  if grep -qE '^SUMMARY: [0-9]+ passed, 0 failed' "$dir/verify_after_restore.log"; then
    echo "  PASS: verify_all is clean again after the rollback"
  else
    echo "  FAIL: verify_all is NOT clean after the rollback"
    grep -E '^(FAIL|SUMMARY)' "$dir/verify_after_restore.log" | cut -c1-200 || true
    ok=0
  fi

  if [[ $ok -eq 1 ]]; then
    echo "RESULT $name: PASS (the fault was detected by the right checks, and the rollback restored a clean state)"
  else
    echo "RESULT $name: FAIL"
    FAILED_SCENARIOS=$((FAILED_SCENARIOS + 1))
  fi
}

step "Preflight"
curl -sf http://localhost:8181/v1/config > /dev/null || { echo "FAIL: Iceberg catalog not reachable (docker compose up -d spark-worker)"; exit 1; }
active=$(curl -sf http://localhost:8080/json/ \
  | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["activeapps"]))' || echo "unreachable")
[[ "$active" == "0" ]] || { echo "FAIL: Spark master reports '$active' running application(s); stop them first"; exit 1; }
echo "catalog reachable, no Spark application running, range $START -> $END"

step "Baseline: verify_all must be clean before anything is broken"
if ! spark verify_all.py --start-date "$START" --end-date "$END" > "$LOGDIR/baseline.log" 2>&1; then
  grep -E '^(FAIL|SUMMARY)' "$LOGDIR/baseline.log" | cut -c1-200 || true
  echo "Baseline is not clean; refusing to inject faults. Log: $LOGDIR/baseline.log"
  exit 1
fi
grep -E '^SUMMARY' "$LOGDIR/baseline.log"

run_scenario dup_event    "events: key is unique" ""
run_scenario missing_slot "mentions: every day has 96 slots|mentions: landing files == Bronze slots per day" "Bronze -> Silver: mentions_enriched rows"
run_scenario gold_drift   "bilateral event_count sums" ""
run_scenario gkg_stall    "GKG match rate above threshold" ""

printf '\n'
if [[ $FAILED_SCENARIOS -eq 0 ]]; then
  echo "FAULT INJECTION TEST PASSED: all 4 faults were detected and rolled back. Logs in $LOGDIR"
  exit 0
fi
echo "FAULT INJECTION TEST FAILED: $FAILED_SCENARIOS scenario(s). Logs in $LOGDIR"
exit 1