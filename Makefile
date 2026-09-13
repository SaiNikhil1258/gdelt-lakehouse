up:
	docker compose up --build -d
down:
	docker compose down
logs:
	docker compose logs -f
reset:
	docker compose down -v   # WARNING: wipes MinIO, Iceberg catalog, and Dagster volumes
restart-code:
	docker compose restart dagster-user-code   # fast dev loop, no rebuild