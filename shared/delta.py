# Copyright 2026 arup-kumar-maiti
"""Shared Delta table operations."""

from typing import TYPE_CHECKING

from deltalake import DeltaTable

if TYPE_CHECKING:
    import pyarrow as pa


def load_or_create_delta_table(
    location: str,
    *,
    schema: pa.Schema,
    partition_by: list[str] | None = None,
    storage_options: dict[str, str] | None = None,
    configuration: dict[str, str] | None = None,
) -> DeltaTable:
    """Load a Delta table, creating it if absent."""
    return DeltaTable.create(
        location,
        schema,
        mode="ignore",
        partition_by=partition_by,
        storage_options=storage_options,
        configuration=configuration,
    )


def upsert_into_delta_table(
    table: DeltaTable,
    data: pa.Table,
    *,
    keys: list[str],
) -> None:
    """Upsert Delta rows by unique input keys."""
    if data.group_by(keys).aggregate([]).num_rows != data.num_rows:
        message = "Duplicate merge keys in input."
        raise ValueError(message)

    identifiers = ['"' + key.replace('"', '""') + '"' for key in keys]
    predicate = " AND ".join(f"target.{key} = source.{key}" for key in identifiers)
    (
        table.merge(
            data,
            predicate=predicate,
            source_alias="source",
            target_alias="target",
            # In-memory source statistics let Delta prune unrelated partitions.
            streamed_exec=False,
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
