# Copyright 2026 arup-kumar-maiti
"""Update daily global stock history in Delta."""

import json
import sys
import tomllib
from contextlib import closing
from datetime import date, datetime, time, timedelta
from http import HTTPStatus
from http.client import HTTPSConnection
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urlencode
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from pandas_market_calendars import get_calendar

from shared.config import GCS_BUCKET
from shared.delta import load_or_create_delta_table, upsert_into_delta_table

if TYPE_CHECKING:
    from deltalake import DeltaTable

# Keep column types consistent across symbol partitions.
OHLCV_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("date", pa.date32(), nullable=False),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("volume", pa.float64()),
    ],
)

TABLE_URI = f"gs://{GCS_BUCKET}/data_pipeline/global_markets/bronze/yahoo_finance"


def update_symbol(
    symbol: str,
    *,
    table: DeltaTable,
    calendar_name: str,
    end: date | None = None,
) -> None:
    """Merge new completed sessions into Delta."""
    symbol = symbol.strip().upper()
    history = table.to_pyarrow_table(
        columns=["date"],
        filters=ds.field("symbol") == symbol,
    )
    latest_date = pc.max(history["date"]).as_py()
    start = latest_date + timedelta(days=1) if latest_date is not None else None
    if start is not None and end is not None and start > end:
        sys.stdout.write(f"{symbol}: already stored through {end}.\n")
        return

    updates = download_history(
        symbol, calendar_name=calendar_name, start=start, end=end
    )
    if not updates.num_rows:
        sys.stdout.write(f"{symbol}: no new completed sessions.\n")
        return
    upsert_into_delta_table(table, updates, keys=["symbol", "date"])


def download_history(
    symbol: str,
    *,
    calendar_name: str,
    start: date | None = None,
    end: date | None = None,
) -> pa.Table:
    """Fetch daily OHLCV and validate session completeness."""
    symbol = symbol.strip().upper()
    if not symbol:
        message = "Symbol cannot be blank."
        raise ValueError(message)
    start = start or date(1900, 1, 1)
    if end is not None and start > end:
        message = f"{symbol}: start {start} is after end {end}."
        raise ValueError(message)

    metadata = _fetch_chart(symbol, {"range": "1d", "interval": "1d"})["meta"]
    timezone = ZoneInfo(metadata["exchangeTimezoneName"])
    now = datetime.now(timezone)
    first_trade = datetime.fromtimestamp(metadata["firstTradeDate"], timezone).date()
    start = max(start, first_trade)
    end = min(end or now.date(), now.date())
    if start > end:
        return pa.Table.from_pylist([], schema=OHLCV_SCHEMA)

    schedule = get_calendar(calendar_name).schedule(start_date=start, end_date=end)
    sessions = schedule.index[schedule["market_close"] <= now]
    if sessions.empty:
        return pa.Table.from_pylist([], schema=OHLCV_SCHEMA)
    end = sessions[-1].date()

    # Yahoo Finance uses an exclusive end timestamp in the exchange's timezone.
    params = {
        "period1": int(datetime.combine(start, time.min, timezone).timestamp()),
        "period2": int(
            datetime.combine(end + timedelta(days=1), time.min, timezone).timestamp(),
        ),
        "interval": "1d",
    }
    result = _fetch_chart(symbol, params)
    dates = [
        datetime.fromtimestamp(value, timezone).date()
        for value in result.get("timestamp") or []
    ]
    history = pa.Table.from_pylist([], schema=OHLCV_SCHEMA)
    if dates:
        columns = {
            **result["indicators"]["quote"][0],
            "symbol": [symbol] * len(dates),
            "date": dates,
        }
        history = pa.table(columns, schema=OHLCV_SCHEMA).filter(
            (ds.field("date") >= start) & (ds.field("date") <= end),
        )

    missing = sorted(set(sessions.date) - set(history.drop_null()["date"].to_pylist()))
    if missing:
        message = (
            f"{symbol}: missing session data on {', '.join(map(str, missing))}. "
            "Batch not saved."
        )
        raise ValueError(message)
    return history


def _fetch_chart(symbol: str, params: dict[str, str | int]) -> dict[str, Any]:
    """Fetch a chart from Yahoo Finance's unofficial endpoint."""
    path = f"/v8/finance/chart/{quote(symbol, safe='')}?{urlencode(params)}"
    with closing(HTTPSConnection("query1.finance.yahoo.com", timeout=30)) as connection:
        connection.request(
            "GET",
            path,
            headers={
                "Accept": "application/json",
                "User-Agent": "money-global-markets/0.1",
            },
        )
        with connection.getresponse() as response:
            if response.status != HTTPStatus.OK:
                message = f"{symbol}: Yahoo Finance HTTP {response.status}."
                raise RuntimeError(message)
            chart = json.loads(response.read())["chart"]
    if chart["error"] is not None:
        message = f"{symbol}: Yahoo Finance: {chart['error']['description']}"
        raise ValueError(message)
    if not chart["result"]:
        message = f"{symbol}: Yahoo Finance returned no chart."
        raise ValueError(message)
    return chart["result"][0]


def main() -> None:
    """Update configured symbols through their last closed sessions."""
    with Path(__file__).with_name("config.toml").open("rb") as config_file:
        config = tomllib.load(config_file)
    symbols = config["symbols"]
    if not symbols:
        message = "No symbols configured in config.toml."
        raise ValueError(message)
    table = load_or_create_delta_table(
        TABLE_URI,
        schema=OHLCV_SCHEMA,
        partition_by=["symbol"],
    )
    for symbol, calendar_name in symbols.items():
        update_symbol(symbol, table=table, calendar_name=calendar_name)


if __name__ == "__main__":
    main()
