import datetime as dt
import math

import dagster as dg

from .resources import FrankfurterResource, MySQLResource, YahooFinanceResource
from .settings import daily_partitions, load_tickers

RETRY = dg.RetryPolicy(max_retries=2, delay=60, backoff=dg.Backoff.EXPONENTIAL)


def _clean(value):
    if value is None:
        return None
    number = float(value)
    return None if math.isnan(number) else number


# ---- stock prices -----------------------------------------------------------
PRICES_DDL = """
CREATE TABLE IF NOT EXISTS stock_prices (
    ticker       VARCHAR(16)   NOT NULL,
    trade_date   DATE          NOT NULL,
    close_price  DECIMAL(18,4) NOT NULL,
    volume       BIGINT,
    dividends    DECIMAL(18,6) NOT NULL DEFAULT 0,
    stock_splits DECIMAL(10,4) NOT NULL DEFAULT 0,
    loaded_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                           ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, trade_date)
)
"""
PRICES_UPSERT = """
INSERT INTO stock_prices (ticker, trade_date, close_price, volume, dividends, stock_splits)
VALUES (:ticker, :trade_date, :close_price, :volume, :dividends, :stock_splits)
ON DUPLICATE KEY UPDATE
    close_price = VALUES(close_price), volume = VALUES(volume),
    dividends = VALUES(dividends), stock_splits = VALUES(stock_splits)
"""


@dg.asset(partitions_def=daily_partitions,
          backfill_policy=dg.BackfillPolicy.single_run(),
          group_name="ingestion", retry_policy=RETRY)
def stock_prices(context: dg.AssetExecutionContext, mysql: MySQLResource,
                 yahoo: YahooFinanceResource) -> dg.MaterializeResult:
    window = context.partition_time_window
    start, end = window.start.date().isoformat(), window.end.date().isoformat()
    tickers = load_tickers()

    rows: list[dict] = []
    for ticker in tickers:
        frame = yahoo.history(ticker, start, end)
        if frame.empty:
            context.log.info(f"{ticker}: no bars in {start}..{end} (weekend/holiday/bad ticker?)")
            continue
        for ts, bar in frame.iterrows():
            close = _clean(bar["Close"])
            if close is None:
                continue
            volume = _clean(bar["Volume"])
            rows.append({
                "ticker": ticker,
                "trade_date": ts.date().isoformat(),
                "close_price": close,
                "volume": int(volume) if volume is not None else None,
                "dividends": _clean(bar.get("Dividends")) or 0.0,
                "stock_splits": _clean(bar.get("Stock Splits")) or 0.0,
            })

    count = mysql.upsert(PRICES_DDL, PRICES_UPSERT, rows)
    return dg.MaterializeResult(metadata={
        "window": f"{start} .. {end} (end exclusive)",
        "tickers": ", ".join(tickers), "rows_upserted": count})


# ---- FX rates ---------------------------------------------------------------
FX_DDL = """
CREATE TABLE IF NOT EXISTS fx_rates (
    rate_date      DATE          NOT NULL,
    base_currency  VARCHAR(3)    NOT NULL,
    quote_currency VARCHAR(3)    NOT NULL,
    rate           DECIMAL(18,6) NOT NULL,
    loaded_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                             ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (rate_date, base_currency, quote_currency)
)
"""
FX_UPSERT = """
INSERT INTO fx_rates (rate_date, base_currency, quote_currency, rate)
VALUES (:rate_date, :base_currency, :quote_currency, :rate)
ON DUPLICATE KEY UPDATE rate = VALUES(rate)
"""


@dg.asset(partitions_def=daily_partitions,
          backfill_policy=dg.BackfillPolicy.single_run(),
          group_name="ingestion", retry_policy=RETRY)
def fx_rates(context: dg.AssetExecutionContext, mysql: MySQLResource,
             frankfurter: FrankfurterResource) -> dg.MaterializeResult:
    window = context.partition_time_window
    start = window.start.date()
    end_inclusive = window.end.date() - dt.timedelta(days=1)
    series = frankfurter.get_rates(start.isoformat(), end_inclusive.isoformat(),
                                   base="USD", quote="INR")
    rows = [{"rate_date": d, "base_currency": "USD", "quote_currency": "INR", "rate": r}
            for d, r in series.items()]
    count = mysql.upsert(FX_DDL, FX_UPSERT, rows)
    return dg.MaterializeResult(metadata={
        "window": f"{start} .. {end_inclusive}", "rows_upserted": count})


# ---- company profiles (slow-changing reference data) ------------------------
PROFILES_DDL = """
CREATE TABLE IF NOT EXISTS company_profiles (
    ticker        VARCHAR(16) NOT NULL,
    snapshot_date DATE        NOT NULL,
    short_name    VARCHAR(255),
    sector        VARCHAR(100),
    industry      VARCHAR(150),
    currency      VARCHAR(8),
    market_cap    BIGINT,
    beta          DECIMAL(10,4),
    PRIMARY KEY (ticker, snapshot_date)
)
"""
PROFILES_UPSERT = """
INSERT INTO company_profiles
    (ticker, snapshot_date, short_name, sector, industry, currency, market_cap, beta)
VALUES
    (:ticker, :snapshot_date, :short_name, :sector, :industry, :currency, :market_cap, :beta)
ON DUPLICATE KEY UPDATE
    short_name = VALUES(short_name), sector = VALUES(sector), industry = VALUES(industry),
    currency = VALUES(currency), market_cap = VALUES(market_cap), beta = VALUES(beta)
"""


@dg.asset(group_name="reference", retry_policy=RETRY)
def company_profiles(context: dg.AssetExecutionContext, mysql: MySQLResource,
                     yahoo: YahooFinanceResource) -> dg.MaterializeResult:
    today = dt.date.today().isoformat()
    rows = []
    for ticker in load_tickers():
        info = yahoo.info(ticker)
        market_cap = _clean(info.get("marketCap"))
        rows.append({
            "ticker": ticker, "snapshot_date": today,
            "short_name": info.get("shortName"), "sector": info.get("sector"),
            "industry": info.get("industry"), "currency": info.get("currency"),
            "market_cap": int(market_cap) if market_cap is not None else None,
            "beta": _clean(info.get("beta")),
        })
        context.log.info(f"{ticker}: sector={info.get('sector')}")
    count = mysql.upsert(PROFILES_DDL, PROFILES_UPSERT, rows)
    return dg.MaterializeResult(metadata={"snapshot_date": today, "rows_upserted": count})