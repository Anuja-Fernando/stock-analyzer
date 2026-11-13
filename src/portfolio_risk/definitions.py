import dagster as dg

from .automation import (daily_ingest_schedule, holdings_file_sensor, ingest_job,
                         reference_job, transform_after_ingest, transform_job,
                         weekly_reference_schedule)
from .checks import fx_rates_valid, stock_prices_fresh, stock_prices_valid
from .dbt_assets import dbt_resource, portfolio_dbt_models
from .ingestion import company_profiles, fx_rates, stock_prices
from .reports import daily_risk_report
from .resources import FrankfurterResource, MySQLResource, YahooFinanceResource

defs = dg.Definitions(
    assets=[stock_prices, fx_rates, company_profiles, portfolio_dbt_models, daily_risk_report],
    asset_checks=[stock_prices_valid, stock_prices_fresh, fx_rates_valid],
    jobs=[ingest_job, transform_job, reference_job],
    schedules=[daily_ingest_schedule, weekly_reference_schedule],
    sensors=[transform_after_ingest, holdings_file_sensor],
    resources={
        "mysql": MySQLResource(
            host=dg.EnvVar("MYSQL_HOST"), port=dg.EnvVar.int("MYSQL_PORT"),
            user=dg.EnvVar("MYSQL_USER"), password=dg.EnvVar("MYSQL_PASSWORD"),
            database="stocks_raw"),
        "yahoo": YahooFinanceResource(),
        "frankfurter": FrankfurterResource(),
        "dbt": dbt_resource,
    },
)