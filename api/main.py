import datetime as dt
import os
from pathlib import Path

import requests
from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

app = FastAPI(title="Pipeline Workflow API")

DAGSTER_GRAPHQL = os.environ.get("DAGSTER_GRAPHQL_URL", "http://localhost:3000/graphql")
REPORTS_DIR = Path(__file__).resolve().parents[1] / "reports"

engine = create_engine(
    URL.create(
        "mysql+pymysql",
        username=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        host=os.environ.get("MYSQL_HOST", "localhost"),
        port=int(os.environ.get("MYSQL_PORT", "3306")),
    ),
    pool_pre_ping=True,
)


def rows(sql: str, **params) -> list[dict]:
    with engine.connect() as conn:
        return [dict(r) for r in conn.execute(text(sql), params).mappings().all()]


def scalar(sql: str, **params):
    with engine.connect() as conn:
        return conn.execute(text(sql), params).scalar()


# ---- table previews (allowlist: the browser never sends raw SQL) -------------
TABLES = {
    "stock_prices": ("stocks_raw.stock_prices", "trade_date DESC, ticker"),
    "fx_rates": ("stocks_raw.fx_rates", "rate_date DESC"),
    "company_profiles": ("stocks_raw.company_profiles", "snapshot_date DESC, ticker"),
    "holdings": ("stocks_analytics.holdings", None),
    "stg_stock_prices": ("stocks_analytics.stg_stock_prices", "trade_date DESC, ticker"),
    "stg_fx_rates": ("stocks_analytics.stg_fx_rates", "rate_date DESC"),
    "stg_company_profiles": ("stocks_analytics.stg_company_profiles", "ticker"),
    "fct_positions_daily": ("stocks_analytics.fct_positions_daily", "trade_date DESC, ticker"),
    "fct_portfolio_daily": ("stocks_analytics.fct_portfolio_daily", "trade_date DESC"),
    "risk_alerts": ("stocks_analytics.risk_alerts", "alert_type, subject"),
}


@app.get("/api/table/{name}")
def table(name: str):
    if name not in TABLES:
        raise HTTPException(404, "unknown table")
    ref, order = TABLES[name]
    try:
        count = scalar(f"SELECT COUNT(*) FROM {ref}")
        order_sql = f" ORDER BY {order}" if order else ""
        preview = rows(f"SELECT * FROM {ref}{order_sql} LIMIT 10")
    except Exception as e:
        return {"exists": False, "error": str(e)[:200]}
    return {"exists": True, "row_count": count, "preview": preview}


# ---- the same rules as the Dagster checks, evaluated live --------------------
@app.get("/api/checks")
def checks():
    results = []
    try:
        def valid(name, tbl, col):
            bad = scalar(f"SELECT COUNT(*) FROM {tbl} WHERE {col} IS NULL OR {col} <= 0")
            total = scalar(f"SELECT COUNT(*) FROM {tbl}")
            results.append({"name": name, "blocking": True,
                            "passed": bad == 0 and total > 0,
                            "detail": f"{bad} invalid rows out of {total}"})

        valid("stock_prices_valid", "stocks_raw.stock_prices", "close_price")
        valid("fx_rates_valid", "stocks_raw.fx_rates", "rate")

        latest = scalar("SELECT MAX(trade_date) FROM stocks_raw.stock_prices")
        age = (dt.date.today() - latest).days if latest else None
        results.append({"name": "stock_prices_fresh", "blocking": False,
                        "passed": age is not None and age <= 4,
                        "detail": f"latest price date {latest}, {age} days old"})

        try:
            n_hold = scalar("SELECT COUNT(*) FROM stocks_analytics.holdings")
            short = rows(
                "SELECT trade_date, COUNT(*) AS n FROM stocks_raw.stock_prices "
                "GROUP BY trade_date HAVING COUNT(*) < :h "
                "ORDER BY trade_date DESC LIMIT 5", h=n_hold)
            results.append({
                "name": "price_coverage (info)", "blocking": False, "passed": not short,
                "detail": ", ".join(f"{r['trade_date']}: {r['n']}/{n_hold}" for r in short)
                          or "every day has a price for every holding"})
        except Exception:
            pass  # holdings table not built yet
    except Exception as e:
        return {"results": [], "error": str(e)[:200]}
    return {"results": results}


# ---- Dagster run and asset status (via its GraphQL API) ----------------------
def gql(query: str) -> dict:
    r = requests.post(DAGSTER_GRAPHQL, json={"query": query}, timeout=5)
    r.raise_for_status()
    body = r.json()
    if body.get("errors"):
        raise RuntimeError(body["errors"][0]["message"])
    return body["data"]


@app.get("/api/dagster/assets")
def dagster_assets():
    try:
        data = gql("""query { assetNodes { assetKey { path }
                     assetMaterializations(limit: 1) { timestamp runId } } }""")
    except Exception as e:
        return {"online": False, "error": str(e)[:300], "assets": {}}
    out = {}
    for n in data["assetNodes"]:
        mats = n["assetMaterializations"]
        out["/".join(n["assetKey"]["path"])] = {
            "last_materialized_ms": int(mats[0]["timestamp"]) if mats else None,
            "run_id": mats[0]["runId"] if mats else None,
        }
    return {"online": True, "assets": out}


@app.get("/api/dagster/runs")
def dagster_runs():
    try:
        data = gql("""query { runsOrError(limit: 12) { ... on Runs { results {
                     runId jobName status startTime endTime } } } }""")
    except Exception as e:
        return {"online": False, "error": str(e)[:300], "runs": []}
    return {"online": True, "runs": data["runsOrError"].get("results", [])}


# ---- the final output of the workflow ----------------------------------------
@app.get("/api/report/latest")
def latest_report():
    files = sorted(REPORTS_DIR.glob("risk_report_*.md")) if REPORTS_DIR.exists() else []
    if not files:
        return {"exists": False}
    return {"exists": True, "name": files[-1].name,
            "text": files[-1].read_text(encoding="utf-8")}