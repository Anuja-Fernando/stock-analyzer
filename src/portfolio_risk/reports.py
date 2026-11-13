import dagster as dg
from dagster_dbt import get_asset_key_for_model

from .dbt_assets import portfolio_dbt_models
from .resources import MySQLResource
from .settings import REPORTS_DIR


@dg.asset(
    deps=[
        get_asset_key_for_model([portfolio_dbt_models], "fct_portfolio_daily"),
        get_asset_key_for_model([portfolio_dbt_models], "fct_positions_daily"),
        get_asset_key_for_model([portfolio_dbt_models], "risk_alerts"),
    ],
    group_name="reporting",
)
def daily_risk_report(context: dg.AssetExecutionContext,
                      mysql: MySQLResource) -> dg.MaterializeResult:
    latest = mysql.query(
        "SELECT * FROM stocks_analytics.fct_portfolio_daily ORDER BY trade_date DESC LIMIT 1")
    if not latest:
        raise dg.Failure("fct_portfolio_daily is empty - check ingestion runs and holdings.csv")
    day = latest[0]
    positions = mysql.query(
        "SELECT ticker, currency, market_value_inr, unrealized_pnl_pct "
        "FROM stocks_analytics.fct_positions_daily WHERE trade_date = :d "
        "ORDER BY market_value_inr DESC", d=day["trade_date"])
    alerts = mysql.query(
        "SELECT alert_type, subject, metric_value, threshold "
        "FROM stocks_analytics.risk_alerts ORDER BY alert_type, subject")

    def num(value, digits=2):
        return "n/a" if value is None else f"{float(value):,.{digits}f}"

    lines = [
        f"# Portfolio risk report - {day['trade_date']}", "",
        f"- Portfolio value: **INR {num(day['portfolio_value_inr'], 0)}**",
        f"- Daily return: {num(day['daily_return_pct'])}%",
        f"- Drawdown from peak: {num(day['drawdown_pct'])}%",
        f"- 30-day annualized volatility: {num(day['volatility_30d_annualized_pct'])}%",
        "", "## Alerts",
    ]
    if alerts:
        for a in alerts:
            lines.append(f"- **{a['alert_type']}** ({a['subject']}): "
                         f"{num(a['metric_value'])} vs threshold {num(a['threshold'])}")
    else:
        lines.append("- No alerts. All metrics are within thresholds.")
    lines += ["", "## Positions", "",
              "| Ticker | Ccy | Value (INR) | P&L vs cost % |", "|---|---|---:|---:|"]
    for p in positions:
        lines.append(f"| {p['ticker']} | {p['currency']} | "
                     f"{num(p['market_value_inr'], 0)} | {num(p['unrealized_pnl_pct'])} |")
    text = "\n".join(lines) + "\n"

    REPORTS_DIR.mkdir(exist_ok=True)
    path = REPORTS_DIR / f"risk_report_{day['trade_date']}.md"
    path.write_text(text, encoding="utf-8")
    return dg.MaterializeResult(metadata={
        "report": dg.MetadataValue.md(text),
        "path": dg.MetadataValue.path(str(path)),
        "alert_count": len(alerts)})