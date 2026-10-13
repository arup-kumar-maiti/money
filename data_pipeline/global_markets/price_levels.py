# Copyright 2026 arup-kumar-maiti
"""Compute current stock price levels."""

from decimal import ROUND_FLOOR, Decimal

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from shared.config import GCS_BUCKET

from .yahoo_finance import TABLE_URI as SOURCE_TABLE_URI

GAIN_PERCENTAGES = (30, 25, 20, 15)
TABLE_URI = f"gs://{GCS_BUCKET}/data_pipeline/global_markets/silver/price_levels"


def calculate_price_levels(history: pa.Table) -> pa.Table:
    """Compute latest closes, record highs, entry prices, and gain to the high."""
    if not history.num_rows:
        message = "No stock history to process."
        raise ValueError(message)

    levels = (
        history.group_by("symbol")
        .aggregate([("date", "max"), ("high", "max")])
        .rename_columns(["symbol", "date", "all_time_high"])
        .join(
            history.select(["symbol", "date", "close"]),
            keys=["symbol", "date"],
            join_type="inner",
        )
        .select(["symbol", "date", "all_time_high", "close"])
        .sort_by("symbol")
    )
    highs = [Decimal(high) for high in levels["all_time_high"].to_pylist()]
    for gain in GAIN_PERCENTAGES:
        multiplier = 1 + Decimal(gain) / 100
        # Round down so the gain cannot exceed the stored high.
        entry_prices = [
            (high / multiplier).quantize(Decimal("0.01"), rounding=ROUND_FLOOR)
            for high in highs
        ]
        levels = levels.append_column(
            f"entry_price_{gain}_pct",
            pa.array(entry_prices, type=pa.decimal128(20, 2)),
        )
    gain_to_high = [
        ((high / Decimal(close) - 1) * 100).quantize(Decimal("0.01"))
        for high, close in zip(highs, levels["close"].to_pylist(), strict=True)
    ]
    return levels.append_column(
        "gain_to_high_pct",
        pa.array(gain_to_high, type=pa.decimal128(20, 2)),
    )


def main() -> None:
    """Calculate and store price levels from stock history."""
    history = DeltaTable(SOURCE_TABLE_URI).to_pyarrow_table(
        columns=["symbol", "date", "high", "close"],
    )
    write_deltalake(
        TABLE_URI,
        calculate_price_levels(history),
        mode="overwrite",
        schema_mode="overwrite",
    )


if __name__ == "__main__":
    main()
