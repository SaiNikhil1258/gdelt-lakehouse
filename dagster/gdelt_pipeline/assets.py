from dagster import asset, AssetExecutionContext, Config, RetryPolicy, Backoff, Jitter, AutomationCondition
from .resources import IcebergSparkResource
from .gdelt_fetch import get_latest_timestamps, s3_client, fetch_and_upload
from .ingestion_logic import (
    ingest_events_batch, ingest_mentions_batch, ingest_gkg_batch,
    transform_events_india, transform_mentions_enriched,
    build_bilateral_sentiment, build_state_conflict, merge_mention_counts,
)

# Events/Mentions: 99.9% arrive within 8 min of slot per the arrival analysis -- a short,
# fast retry is correct here. No reason to wait on GKG's much longer tail.
FAST_RETRY = RetryPolicy(max_retries=3, delay=20, backoff=Backoff.EXPONENTIAL, jitter=Jitter.PLUS_MINUS)
RETRY = RetryPolicy(max_retries=3, delay=30, backoff=Backoff.EXPONENTIAL, jitter=Jitter.PLUS_MINUS)


@asset(retry_policy=FAST_RETRY)
def bronze_events(context: AssetExecutionContext, spark: IcebergSparkResource):
    ts = get_latest_timestamps()["events"]
    ok = fetch_and_upload(ts, "events", s3_client(), max_retries=3, backoff_base=20)
    if not ok:
        raise Exception(f"events file missing for {ts}")
    session = spark.get_spark()
    new_rows, total_rows = ingest_events_batch(session, f"s3a://gdelt-landing/events/{ts}.csv")
    context.add_output_metadata({"timestamp": ts, "new_rows": new_rows, "batch_rows": total_rows})
    session.stop()


@asset(retry_policy=FAST_RETRY)
def bronze_mentions(context: AssetExecutionContext, spark: IcebergSparkResource):
    ts = get_latest_timestamps()["mentions"]
    ok = fetch_and_upload(ts, "mentions", s3_client(), max_retries=3, backoff_base=20)
    if not ok:
        raise Exception(f"mentions file missing for {ts}")
    session = spark.get_spark()
    new_rows, total_rows = ingest_mentions_batch(session, f"s3a://gdelt-landing/mentions/{ts}.csv")
    context.add_output_metadata({"timestamp": ts, "new_rows": new_rows, "batch_rows": total_rows})
    session.stop()


class GkgIngestConfig(Config):
    timestamp: str  # supplied by the sensor once it has confirmed the file actually exists


@asset(retry_policy=RETRY)
def bronze_gkg(context: AssetExecutionContext, spark: IcebergSparkResource, config: GkgIngestConfig):
    ok = fetch_and_upload(config.timestamp, "gkg", s3_client(), max_retries=2, backoff_base=15)
    if not ok:
        raise Exception(f"gkg file for {config.timestamp} vanished between sensor check and run start")
    session = spark.get_spark()
    new_rows, total_rows = ingest_gkg_batch(session, f"s3a://gdelt-landing/gkg/{config.timestamp}.csv")
    context.add_output_metadata({"timestamp": config.timestamp, "new_rows": new_rows, "batch_rows": total_rows})
    session.stop()


@asset(deps=[bronze_events], retry_policy=RETRY)
def silver_events_india(context: AssetExecutionContext, spark: IcebergSparkResource):
    session = spark.get_spark()
    context.add_output_metadata({"row_count": transform_events_india(session)})
    session.stop()


# Driven off the 15-min cron via AutomationCondition.on_cron rather than the main_job
# selection: this asset only fires once ALL of silver_events_india, bronze_mentions,
# and bronze_gkg have updated since the last tick. If bronze_gkg is behind, the tick
# is simply skipped -- so this always recomputes over a fully-fresh snapshot instead
# of mixing current events/mentions with stale GKG.
@asset(
    deps=[silver_events_india, bronze_mentions, bronze_gkg],
    retry_policy=RETRY,
    automation_condition=AutomationCondition.on_cron("*/15 * * * *"),
)
def silver_mentions_enriched(context: AssetExecutionContext, spark: IcebergSparkResource):
    session = spark.get_spark()
    context.add_output_metadata({"row_count": transform_mentions_enriched(session)})
    session.stop()


@asset(deps=[silver_events_india], retry_policy=RETRY)
def gold_bilateral_sentiment(context: AssetExecutionContext, spark: IcebergSparkResource):
    session = spark.get_spark()
    context.add_output_metadata({"row_count": build_bilateral_sentiment(session)})
    session.stop()


@asset(deps=[silver_events_india], retry_policy=RETRY)
def gold_state_conflict(context: AssetExecutionContext, spark: IcebergSparkResource):
    session = spark.get_spark()
    context.add_output_metadata({"row_count": build_state_conflict(session)})
    session.stop()


# Same reasoning as silver_mentions_enriched above -- this only recomputes once its
# single upstream has actually refreshed since the last tick, so it inherits that
# same "whole data, not partial" guarantee automatically.
@asset(
    deps=[silver_mentions_enriched],
    retry_policy=RETRY,
    automation_condition=AutomationCondition.on_cron("*/15 * * * *"),
)
def gold_mention_counts(context: AssetExecutionContext, spark: IcebergSparkResource):
    session = spark.get_spark()
    context.add_output_metadata({"distinct_events_merged": merge_mention_counts(session)})
    session.stop()