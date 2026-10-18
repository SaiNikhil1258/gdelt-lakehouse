from dagster import Definitions, define_asset_job, ScheduleDefinition
from .resources import IcebergSparkResource
from .sensors import gkg_availability_sensor
from .assets import (
    bronze_events, bronze_mentions, bronze_gkg,
    silver_events_india, silver_mentions_enriched,
    gold_bilateral_sentiment, gold_state_conflict, gold_mention_counts,
)

# silver_mentions_enriched and gold_mention_counts are deliberately left out of this
# selection -- they're now driven by their own AutomationCondition.on_cron instead of
# the fixed schedule (see assets.py), so main_job only covers the chain that has no
# dependency on bronze_gkg's async arrival.
main_job = define_asset_job(
    name="gdelt_main_cycle",
    selection=[
        bronze_events, bronze_mentions,
        silver_events_india,
        gold_bilateral_sentiment, gold_state_conflict,
    ],
)
main_schedule = ScheduleDefinition(job=main_job, cron_schedule="*/15 * * * *")

gkg_job = define_asset_job(name="gdelt_gkg_ingest", selection=[bronze_gkg])

defs = Definitions(
    assets=[
        bronze_events, bronze_mentions, bronze_gkg,
        silver_events_india, silver_mentions_enriched,
        gold_bilateral_sentiment, gold_state_conflict, gold_mention_counts,
    ],
    jobs=[main_job, gkg_job],
    schedules=[main_schedule],
    sensors=[gkg_availability_sensor],
    resources={"spark": IcebergSparkResource()},
)