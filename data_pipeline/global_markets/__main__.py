# Copyright 2026 arup-kumar-maiti
"""Update stock data and register its BigQuery tables."""

from pathlib import Path

from google.cloud import bigquery

from shared.config import BIGQUERY_REGION

from . import price_levels, yahoo_finance

SQL_DIR = Path(__file__).with_name("bigquery")


def main() -> None:
    """Ingest history, refresh price levels, and register BigQuery tables."""
    yahoo_finance.main()
    price_levels.main()
    with bigquery.Client() as client:
        for name in ("yahoo_finance", "price_levels"):
            sql = (SQL_DIR / f"{name}.sql").read_text(encoding="utf-8")
            client.query(sql, location=BIGQUERY_REGION).result()


if __name__ == "__main__":
    main()
