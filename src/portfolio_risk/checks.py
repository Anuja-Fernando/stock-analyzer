import datetime as dt

import dagster as dg

from .ingestion import fx_rates, stock_prices
from .resources import MySQLResource


@dg.asset_check(asset=stock_prices, blocking=True)
def stock_prices_valid(mysql: MySQLResource) -> dg.AssetCheckResult:
    bad = mysql.scalar(
        "SELECT COUNT(*) FROM stock_prices WHERE close_price IS NULL OR close_price <= 0")
    total = mysql.scalar("SELECT COUNT(*) FROM stock_prices")
    return dg.AssetCheckResult(passed=(bad == 0 and total > 0),
                               metadata={"bad_rows": bad, "total_rows": total})


@dg.asset_check(asset=stock_prices)
def stock_prices_fresh(mysql: MySQLResource) -> dg.AssetCheckResult:
    latest = mysql.scalar("SELECT MAX(trade_date) FROM stock_prices")
    age_days = (dt.date.today() - latest).days if latest else 9999
    return dg.AssetCheckResult(
        passed=age_days <= 4, severity=dg.AssetCheckSeverity.WARN,
        metadata={"latest_trade_date": str(latest), "age_days": age_days})


@dg.asset_check(asset=fx_rates, blocking=True)
def fx_rates_valid(mysql: MySQLResource) -> dg.AssetCheckResult:
    bad = mysql.scalar("SELECT COUNT(*) FROM fx_rates WHERE rate IS NULL OR rate <= 0")
    total = mysql.scalar("SELECT COUNT(*) FROM fx_rates")
    return dg.AssetCheckResult(passed=(bad == 0 and total > 0),
                               metadata={"bad_rows": bad, "total_rows": total})