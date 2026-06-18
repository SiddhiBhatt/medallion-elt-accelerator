"""
utils/delta_utils.py

Delta Lake helper functions used across all three layers.
Covers: merge/upsert, optimize, vacuum, table history, schema evolution.
"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
import logging

logger = logging.getLogger(__name__)


def merge_into_delta(
    spark: SparkSession,
    df: DataFrame,
    target_table: str,
    merge_keys: list,
    update_cols: list = None
):
    """
    Upsert (SCD Type 1) a DataFrame into a Delta table.
    Matches on merge_keys; updates all columns or only update_cols if specified.

    Args:
        spark        : SparkSession
        df           : Source DataFrame
        target_table : Target Delta table name (catalog.schema.table)
        merge_keys   : Columns to match on, e.g. ['vendor_id']
        update_cols  : Columns to update on match (None = all columns)
    """
    delta_tbl = DeltaTable.forName(spark, target_table)
    join_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])

    merge_builder = (
        delta_tbl.alias("target")
        .merge(df.alias("source"), join_condition)
    )

    if update_cols:
        update_map = {c: f"source.{c}" for c in update_cols}
        merge_builder = merge_builder.whenMatchedUpdate(set=update_map)
    else:
        merge_builder = merge_builder.whenMatchedUpdateAll()

    merge_builder.whenNotMatchedInsertAll().execute()
    logger.info(f"Merge complete into {target_table} on keys: {merge_keys}")


def optimize_table(spark: SparkSession, table_name: str, zorder_cols: list = None):
    """
    Run OPTIMIZE on a Delta table, with optional Z-ORDER for query acceleration.
    Z-ORDER is most impactful on high-cardinality filter columns (e.g. date, account_code).
    """
    sql = f"OPTIMIZE {table_name}"
    if zorder_cols:
        sql += f" ZORDER BY ({', '.join(zorder_cols)})"
    spark.sql(sql)
    logger.info(f"OPTIMIZE complete: {table_name}" + (f" ZORDER BY {zorder_cols}" if zorder_cols else ""))


def vacuum_table(spark: SparkSession, table_name: str, retain_hours: int = 168):
    """
    Remove old Delta files beyond the retention window.
    Default retention: 7 days (168 hours) — do not go below 168 in production
    unless time travel is not required.
    """
    spark.sql(f"VACUUM {table_name} RETAIN {retain_hours} HOURS")
    logger.info(f"VACUUM complete: {table_name} (retain {retain_hours}h)")


def get_table_history(spark: SparkSession, table_name: str, limit: int = 10) -> DataFrame:
    """Return recent Delta table version history — useful for audit and rollback."""
    return spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT {limit}")


def restore_table(spark: SparkSession, table_name: str, version: int = None, timestamp: str = None):
    """
    Restore a Delta table to a previous version or timestamp.
    Exactly one of version or timestamp must be provided.

    Args:
        version   : Delta version number to restore to
        timestamp : ISO timestamp string, e.g. '2024-01-15T06:00:00'
    """
    if version is not None:
        spark.sql(f"RESTORE TABLE {table_name} TO VERSION AS OF {version}")
        logger.warning(f"RESTORED {table_name} to version {version}")
    elif timestamp:
        spark.sql(f"RESTORE TABLE {table_name} TO TIMESTAMP AS OF '{timestamp}'")
        logger.warning(f"RESTORED {table_name} to timestamp {timestamp}")
    else:
        raise ValueError("Provide either version or timestamp for restore.")


def clone_table(spark: SparkSession, source_table: str, target_table: str, shallow: bool = True):
    """
    Clone a Delta table (shallow by default for dev/test purposes).
    Shallow clone shares data files; deep clone copies them.
    """
    clone_type = "SHALLOW" if shallow else "DEEP"
    spark.sql(f"CREATE OR REPLACE TABLE {target_table} {clone_type} CLONE {source_table}")
    logger.info(f"{clone_type} CLONE: {source_table} → {target_table}")
