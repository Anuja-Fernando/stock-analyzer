import time

import dagster as dg
import requests
import yfinance as yf
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


class MySQLResource(dg.ConfigurableResource):
    host: str
    port: int = 3306
    user: str
    password: str
    database: str

    def get_engine(self):
        url = URL.create("mysql+pymysql", username=self.user, password=self.password,
                         host=self.host, port=self.port, database=self.database)
        return create_engine(url)

    def upsert(self, create_sql: str, upsert_sql: str, rows: list[dict]) -> int:
        with self.get_engine().begin() as conn:
            conn.execute(text(create_sql))
            if rows:
                conn.execute(text(upsert_sql), rows)
        return len(rows)

    def scalar(self, sql: str):
        with self.get_engine().connect() as conn:
            return conn.execute(text(sql)).scalar()

    def query(self, sql: str, **params) -> list[dict]:
        with self.get_engine().connect() as conn:
            return [dict(r) for r in conn.execute(text(sql), params).mappings().all()]


class YahooFinanceResource(dg.ConfigurableResource):
    """yfinance wrapper with retries; Yahoo throttles aggressive callers."""
    max_attempts: int = 3
    pause_seconds: float = 1.0

    def history(self, ticker: str, start: str, end: str):
        """Raw daily bars, start inclusive, end exclusive."""
        for attempt in range(1, self.max_attempts + 1):
            try:
                df = yf.Ticker(ticker).history(
                    start=start, end=end, auto_adjust=False, actions=True)
                time.sleep(self.pause_seconds)
                return df
            except Exception:
                if attempt == self.max_attempts:
                    raise
                time.sleep(5 * attempt)

    def info(self, ticker: str) -> dict:
        for attempt in range(1, self.max_attempts + 1):
            try:
                data = yf.Ticker(ticker).info or {}
                time.sleep(self.pause_seconds)
                return data
            except Exception:
                if attempt == self.max_attempts:
                    raise
                time.sleep(5 * attempt)
        return {}


class FrankfurterResource(dg.ConfigurableResource):
    base_url: str = "https://api.frankfurter.dev/v1"

    def get_rates(self, start: str, end_inclusive: str, base: str, quote: str) -> dict:
        response = requests.get(f"{self.base_url}/{start}..{end_inclusive}",
                                params={"base": base, "symbols": quote}, timeout=30)
        if response.status_code == 404:      # range with no business days
            return {}
        response.raise_for_status()
        rates = response.json().get("rates", {})
        return {day: v[quote] for day, v in rates.items() if quote in v}