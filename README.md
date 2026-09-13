# 1. Create Gold tables
docker compose exec -w /opt/jobs spark-master  spark-submit --master spark://spark-master:7077 bootstrap_gold.py

# 2. Populate Gold from Silver
docker compose exec -w /opt/jobs spark-master  spark-submit --master spark://spark-master:7077 gold_aggregate.py

# 3. Verify
docker compose exec -w /opt/jobs spark-master  spark-submit --master spark://spark-master:7077 verify_gold.py

# 4. Re-run + verify again -- counts must stay IDENTICAL (idempotency,
#    same discipline we applied to Bronze and Silver)
docker compose exec -w /opt/jobs spark-master  spark-submit --master spark://spark-master:7077 gold_aggregate.py
docker compose exec -w /opt/jobs spark-master  spark-submit --master spark://spark-master:7077 verify_gold.py