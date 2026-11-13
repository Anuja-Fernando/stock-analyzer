import csv
from pathlib import Path

import dagster as dg

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project"
HOLDINGS_PATH = DBT_PROJECT_DIR / "seeds" / "holdings.csv"
REPORTS_DIR = PROJECT_ROOT / "reports"

MARKET_TZ = "Asia/Kolkata"   # partition days are cut at midnight here; change to yours

daily_partitions = dg.DailyPartitionsDefinition(
    start_date="2026-08-01", timezone=MARKET_TZ
)


def load_tickers() -> list[str]:
    with HOLDINGS_PATH.open(newline="", encoding="utf-8") as f:
        return sorted(
            {row["ticker"].strip().upper() for row in csv.DictReader(f) if row.get("ticker")}
        )