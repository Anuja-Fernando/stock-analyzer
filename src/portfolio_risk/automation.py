import dagster as dg

from .dbt_assets import portfolio_dbt_models
from .reports import daily_risk_report
from .settings import HOLDINGS_PATH, MARKET_TZ, daily_partitions

ingest_job = dg.define_asset_job(
    "ingest_market_data_job",
    selection=dg.AssetSelection.groups("ingestion"),
    partitions_def=daily_partitions,
)
transform_job = dg.define_asset_job(
    "transform_and_report_job",
    selection=dg.AssetSelection.assets(portfolio_dbt_models, daily_risk_report),
)
reference_job = dg.define_asset_job(
    "refresh_reference_and_rebuild_job",
    selection=dg.AssetSelection.all() - dg.AssetSelection.groups("ingestion"),
)

# Daily 03:30 in MARKET_TZ, for the day that just finished.
daily_ingest_schedule = dg.build_schedule_from_partitioned_job(
    ingest_job, hour_of_day=3, minute_of_hour=30)

# Sundays 06:00: refresh sector / market-cap / beta.
weekly_reference_schedule = dg.ScheduleDefinition(
    job=reference_job, cron_schedule="0 6 * * 0", execution_timezone=MARKET_TZ)


@dg.run_status_sensor(run_status=dg.DagsterRunStatus.SUCCESS,
                      monitored_jobs=[ingest_job], request_job=transform_job)
def transform_after_ingest(context: dg.RunStatusSensorContext):
    """Ingest succeeded (blocking checks passed) -> run dbt + report."""
    if "dagster/backfill" in context.dagster_run.tags:
        return dg.SkipReason("From a backfill; run transform_and_report_job when it ends.")
    return dg.RunRequest(run_key=context.dagster_run.run_id)


@dg.sensor(job=reference_job, minimum_interval_seconds=30)
def holdings_file_sensor(context: dg.SensorEvaluationContext):
    """Edit holdings.csv -> reload the seed, refresh profiles, rebuild marts."""
    if not HOLDINGS_PATH.exists():
        return dg.SkipReason("holdings.csv not found")
    mtime = str(HOLDINGS_PATH.stat().st_mtime)
    previous = context.cursor
    context.update_cursor(mtime)
    if previous is None:
        return dg.SkipReason("First evaluation: recorded current file state")
    if previous == mtime:
        return dg.SkipReason("holdings.csv unchanged")
    return dg.RunRequest(run_key=mtime)