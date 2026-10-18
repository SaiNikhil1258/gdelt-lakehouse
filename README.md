docker run --rm --network lakehouse-net --entrypoint /bin/sh minio/mc -c  "mc alias set local http://minio:9000 admin changeme123 >/dev/null &&   mc ls --summarize --recursive local/gdelt-landing"

docker compose exec -T spark-master spark-submit  --master spark://spark-master:7077 /opt/jobs/summarize_bronze.py 2>&1 | grep "total rows"




time python scripts/backfill.py --start-date 20260901 --end-date 20260901 --workers 6  2>&1 | tee backfill_0901.log



docker compose exec -T spark-master spark-submit  --master spark://spark-master:7077 /opt/jobs/verify_bronze.py 2>&1 | grep "total="


docker compose exec -T spark-master spark-submit  --master spark://spark-master:7077 /opt/jobs/diagnose_mention_dups.py 2>&1  | sed -n '/=== DUP SUMMARY ===/,$p' | grep -v " INFO \| WARN "



docker compose exec -T spark-master spark-submit  --master spark://spark-master:7077 /opt/jobs/diagnose_mentions_dups.py > diag.log 2>&1
echo "exit code: $?"
grep -n "DUP SUMMARY\|Traceback\|Error\|Exception" diag.log | head -20




docker compose exec -T spark-master spark-submit  --master spark://spark-master:7077 /opt/jobs/reset_bronze.py 2>&1 | grep "Dropped"

docker run --rm --network lakehouse-net --entrypoint /bin/sh minio/mc -c  "mc alias set local http://minio:9000 admin changeme123 >/dev/null && mc rm --recursive --force local/warehouse/bronze;   mc ls --summarize --recursive local/warehouse/bronze | tail -n 2"

docker compose exec -T spark-master spark-submit  --master spark://spark-master:7077 /opt/jobs/bootstrap_bronze.py 2>&1 | grep -A6 "Bronze namespace"



python scripts/backfill.py --start-date 20260901 --end-date 20260901 --ingest-only  2>&1 | tee backfill_0901_v2.log | tail -n 12

docker compose exec -T spark-master spark-submit  --master spark://spark-master:7077 /opt/jobs/verify_bronze.py 2>&1 | grep "total="



docker compose exec -T spark-master spark-submit  --master spark://spark-master:7077 /opt/jobs/diagnose_key_collisions.py > diag2.log 2>&1
echo "exit code: $?"
grep -c "COLLISION SUMMARY" diag2.log
sed -n '/=== COLLISION SUMMARY ===/,$p' diag2.log | grep -v " INFO \| WARN "



docker compose exec -T spark-master spark-submit  --master spark://spark-master:7077 /opt/jobs/verify_bronze.py 2>&1 | grep "total="



python scripts/backfill.py --start-date 20260901 --end-date 20260901 --ingest-only > backfill_0901_v3.log 2>&1

grep -A5 "BACKFILL SUMMARY" backfill_0901_v3.log

docker compose exec -T spark-master spark-submit  --master spark://spark-master:7077 /opt/jobs/verify_bronze.py 2>&1 | grep "total="



time python3 scripts/backfill.py --start-date 20260902 --end-date 20260907  > backfill_0902_0907.log 2>&1
grep -A5 "Download summary\|BACKFILL SUMMARY" backfill_0902_0907.log

docker compose exec -T spark-master spark-submit  --master spark://spark-master:7077 /opt/jobs/verify_bronze.py 2>&1 | grep "total="


for f in backfill_900log backfill_0901_v3.log; do echo $f;  grep -E '^26/10/11 [0-9:]{8}' $f | sed -n '1p;$p' | cut -c1-17; done



count() { docker run --rm --network lakehouse-net --entrypoint /bin/sh minio/mc -c  "mc alias set local http://minio:9000 admin changeme123 >/dev/null &&   mc ls local/warehouse/bronze/events/metadata/ | grep -c 'metadata.json'"; }
a=$(count); sleep 60; b=$(count)
echo "events commits: $a -> $b (+$((b-a)) in ~1 min); finish line is about 673"


count() { docker run --rm --network lakehouse-net --entrypoint /bin/sh minio/mc -c  "mc alias set local http://minio:9000 admin changeme123 >/dev/null && mc ls local/warehouse/bronze/$1/metadata/" | grep -c 'metadata.json'; }
for t in events mentions gkg; do echo "$t: $(count $t)"; done; sleep 60
for t in events mentions gkg; do echo "$t (+60s): $(count $t)"; done


docker run --rm --network lakehouse-net --entrypoint /bin/sh minio/mc -c  "mc alias set local http://minio:9000 admin changeme123 >/dev/null && mc ls --recursive local/gdelt-landing/"  | awk '{print $NF}' | awk -F/ '{print $1, substr($2,1,8)}' | sort | uniq -c



docker compose exec -T spark-master spark-submit  --master spark://spark-master:7077 /opt/jobs/verify_bronze.py 2>&1 | grep "total="

docker compose exec -T spark-master spark-submit  --master spark://spark-master:7077 /opt/jobs/summarize_days.py > days.log 2>&1
sed -n '/=== DAYS events ===/,$p' days.log | grep -E "^\||=== DAYS"



docker compose exec -T spark-master spark-submit  --master spark://spark-master:7077 /opt/jobs/backfill_ingest.py  --date 20260907 --no-skip > bulk_test_0907.log 2>&1
echo "exit code: $?"
grep -E '^2026[0-9]{4} (events|mentions|gkg)|SUMMARY|Traceback|Error' bulk_test_0907.log




time python -u scripts/backfill.py --start-date 20260908 --end-date 20260908  > bulk_0908.log 2>&1
grep -E '^2026[0-9]{4} (events|mentions|gkg)|SUMMARY|Traceback|Error|Failed|Missing' bulk_0908.log

docker compose exec -T spark-master spark-submit  --master spark://spark-master:7077 /opt/jobs/verify_bronze.py 2>&1 | grep "total="


docker run --rm --network lakehouse-net --entrypoint /bin/sh minio/mc -c  "mc alias set local http://minio:9000 admin changeme123 >/dev/null && mc du local/warehouse && mc du local/gdelt-landing"
df -h / | tail -n 1
docker compose ps -a --format 'table {{.Service}}\t{{.Status}}'
head -n 5 bulk_test_0907.log; echo ---; head -n 5 bulk_0908.log
which python python3; python3 -c "import boto3, requests; print('deps ok')"


docker compose exec -T spark-master spark-submit  --master spark://spark-master:7077 /opt/jobs/verify_bronze.py 2>&1 | grep "total="




docker run --rm --network lakehouse-net --entrypoint /bin/sh minio/mc -c  "mc alias set local http://minio:9000 admin changeme123 >/dev/null && mc du local/warehouse && mc du local/gdelt-landing"
docker system df


nohup python -u scripts/backfill.py --start-date 20260901 --end-date 20260930  > backfill_sep.log 2>&1 &
echo "started, PID $!"



grep -E "BULK BACKFILL SUMMARY|BACKFILL COMPLETE|Failed downloads|MISMATCH|Traceback|^(events|mentions|gkg):|Missing: +[1-9]" backfill_sep.log

docker compose exec -T spark-master spark-submit  --master spark://spark-master:7077 /opt/jobs/verify_bronze.py 2>&1 | grep "total="




grep -E "BULK BACKFILL SUMMARY|Failed downloads|MISMATCH|Traceback|^(events|mentions|gkg):|Missing: +[1-9]|Failed: +[1-9]" backfill_sep.log

docker compose exec -T spark-master spark-submit  --master spark://spark-master:7077 /opt/jobs/verify_bronze.py 2>&1 | grep "total="

docker compose exec -T spark-master spark-submit  --master spark://spark-master:7077 /opt/jobs/summarize_days.py > days.log 2>&1
sed -n '/=== DAYS events ===/,$p' days.log | grep -E "^\||=== DAYS"



docker compose exec -T spark-master spark-submit  --master spark://spark-master:7077 /opt/jobs/diagnose_null_gkg_ts.py > diag3.log 2>&1
echo "exit code: $?"
sed -n '/=== NULL TS COUNTS ===/,$p' diag3.log | grep -v " INFO \| WARN "

grep -n -B4 -A6 "withInternalError" bulk_0908.log | head -n 40 | cut -c1-220




docker compose exec -T spark-master spark-submit  --master spark://spark-master:7077 /opt/jobs/diagnose_mention_grain.py > diag4.log 2>&1
echo "exit code: $?"
sed -n '/=== MENTION GRAIN/,$p' diag4.log | grep -E "^===|^mention rows|^gkg rows|^\||^-RECORD|^ [a-zA-Z_]+ +\|"



docker compose exec -T spark-master spark-submit --master spark://spark-master:7077  /opt/jobs/silver_transform.py > silver_transform.log 2>&1 && docker compose exec -T spark-master spark-submit --master spark://spark-master:7077  /opt/jobs/gold_aggregate.py > gold_aggregate.log 2>&1
echo "exit code: $?"
grep -hE "rows written|merged against|Traceback|Exception| ERROR " silver_transform.log gold_aggregate.log | cut -c1-200


for f in verify_silver verify_gold; do
  docker compose exec -T spark-master spark-submit --master spark://spark-master:7077    /opt/jobs/$f.py 2>&1 | grep -E "total=|match_rate|distinct GLOBALEVENTID"
done




docker compose exec -T spark-master spark-submit --master spark://spark-master:7077  /opt/jobs/silver_transform.py > silver_transform2.log 2>&1 && docker compose exec -T spark-master spark-submit --master spark://spark-master:7077  /opt/jobs/gold_aggregate.py > gold_aggregate2.log 2>&1
echo "exit code: $?"
grep -hE "rows written|merged against|Traceback|Exception| ERROR " silver_transform2.log gold_aggregate2.log | cut -c1-200

for f in verify_silver verify_gold; do
  docker compose exec -T spark-master spark-submit --master spark://spark-master:7077    /opt/jobs/$f.py 2>&1 | grep -E "total=|match_rate|distinct GLOBALEVENTID"
done



docker compose exec -T spark-master spark-submit  --master spark://spark-master:7077 /opt/jobs/table_health.py > health.log 2>&1
echo "exit code: $?"
sed -n '/=== TABLE HEALTH ===/,$p' health.log | grep -E "^===|^(bronze|silver|gold)\.|^\|"



docker compose exec -T spark-master spark-submit  --master spark://spark-master:7076 /opt/jobs/reset_silver.py 2>&1 | grep "Dropped"

docker run --rm --network lakehouse-net --entrypoint /bin/sh minio/mc -c  "mc alias set local http://minio:9000 admin changeme123 >/dev/null &&   mc rm --recursive --force local/warehouse/silver;   mc ls --summarize --recursive local/warehouse/silver | tail -n 2"




docker compose exec -T spark-master spark-submit --master spark://spark-master:7077  /opt/jobs/bootstrap_silver.py > bootstrap_silver.log 2>&1 && docker compose exec -T spark-master spark-submit --master spark://spark-master:7077  /opt/jobs/silver_transform.py > silver_transform3.log 2>&1 && docker compose exec -T spark-master spark-submit --master spark://spark-master:7077  /opt/jobs/gold_aggregate.py > gold_aggregate3.log 2>&1
echo "exit code: $?"
grep -hE "rows written|merged against|Traceback|Exception| ERROR " silver_transform3.log gold_aggregate3.log | cut -c1-200

for f in verify_silver verify_gold; do
  docker compose exec -T spark-master spark-submit --master spark://spark-master:7077    /opt/jobs/$f.py 2>&1 | grep -E "total=|match_rate|distinct GLOBALEVENTID"
done

docker compose exec -T spark-master spark-submit --master spark://spark-master:7077  /opt/jobs/table_health.py > health2.log 2>&1
sed -n '/=== TABLE HEALTH ===/,/=== SILVER EVENT DATES ===/p' health2.log | grep -E "^(silver|gold)\."


curl -s http://localhost:8181/v1/namespaces/silver/tables; echo
for t in events_india mentions_enriched; do
  printf '%s: HTTP ' $t
  curl -s -o /dev/null -w '%{http_code}\n' -X DELETE http://localhost:8181/v1/namespaces/silver/tables/$t
done
curl -s http://localhost:8181/v1/namespaces/silver/tables; echo


docker compose exec -T spark-master spark-submit --master spark://spark-master:7077  /opt/jobs/verify_all.py --start-date 20260901 --end-date 20260930 > verify_all.log 2>&1
echo "exit code: $?"
grep -E "^(PASS|FAIL)|^===|^SUMMARY|^  FAILED|Traceback" verify_all.log | cut -c1-260


docker run --rm --network lakehouse-net --entrypoint /bin/sh minio/mc -c  "mc alias set local http://minio:9000 admin changeme123 >/dev/null && mc du local/warehouse && mc du local/gdelt-landing"
docker system df


python -u scripts/backfill.py --start-date 20261001 --end-date 20261017 > bf_oct.log 2>&1 &
BF_PID=$!; echo "PID $BF_PID"
until grep -q "Starting Spark ingestion" bf_oct.log; do sleep 10; done
sleep 45
grep -E '^2026[0-9]{4} (events|mentions|gkg)' bf_oct.log | tail -n 3
curl -s http://localhost:8080/json/ | python3 -c 'import json,sys; print([(a["id"],a["name"],a["state"]) for a in json.load(sys.stdin)["activeapps"]])'
kill -TERM $BF_PID; wait $BF_PID; echo "exit code: $?"
sleep 5
tail -n 5 bf_oct.log
curl -s http://localhost:8080/json/ | python3 -c 'import json,sys; print([(a["id"],a["name"],a["state"]) for a in json.load(sys.stdin)["activeapps"]])'
python -u scripts/backfill.py --start-date 20261001 --end-date 20261017 > bf_oct2.log 2>&1 &
BF_PID=$!; wait $BF_PID; echo "exit code: $?"
grep -E "CHUNK [0-9]+/|BACKFILL (COMPLETE|INCOMPLETE)|Downloads:|^(events|mentions|gkg): [0-9]+ days|MISMATCH|Traceback" bf_oct2.log




docker compose exec -T spark-master spark-submit --master spark://spark-master:7077  /opt/jobs/silver_transform.py > silver_oct.log 2>&1 < /dev/null && docker compose exec -T spark-master spark-submit --master spark://spark-master:7077  /opt/jobs/gold_aggregate.py > gold_oct.log 2>&1 < /dev/null
echo "exit code: $?"
grep -hE "rows written|merged against" silver_oct.log gold_oct.log

docker compose exec -T spark-master spark-submit --master spark://spark-master:7077  /opt/jobs/verify_all.py --start-date 20260901 --end-date 20261017 > verify_oct.log 2>&1 < /dev/null
echo "exit code: $?"
grep -E "^(PASS|FAIL)|^SUMMARY" verify_oct.log | cut -c1-240



docker compose exec -T spark-master spark-submit --master spark://spark-master:7077 /opt/jobs/silver_transform.py > silver_oct2.log 2>&1 < /dev/null && docker compose exec -T spark-master spark-submit --master spark://spark-master:7077 /opt/jobs/gold_aggregate.py > gold_oct2.log 2>&1 < /dev/null && docker compose exec -T spark-master spark-submit --master spark://spark-master:7077 /opt/jobs/verify_all.py --start-date 20260901 --end-date 20261017 > verify_oct2.log 2>&1 < /dev/null; grep -E "^(FAIL|SUMMARY)" verify_oct2.log | cut -c1-240