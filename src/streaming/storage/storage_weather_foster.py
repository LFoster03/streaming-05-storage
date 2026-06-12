"""src/streaming/storage/storage_weather_foster.py.

Project-specific DuckDB storage functions
used by the weather Kafka consumer.

This module creates two DuckDB tables:
  - consumed_valid_weather: records that passed all validation checks
  - consumed_rejected_weather: records that failed, with error details

For each consumed message the consumer calls:
  1. init_db() once at startup to initialize and clear the tables.
  2. write_valid_record() or write_rejected_record() for each message.
  3. log_storage_summary() at the end to report what was stored.

Author: Lindsay Foster
Date: 2026-05

"""

# === DECLARE IMPORTS ===

from pathlib import Path
from typing import Any, Final

from datafun_streaming.core.types import DataRecordDict
from datafun_streaming.storage.duckdb_sql import (
    build_clear_table_sql,
    build_create_table_sql,
    build_insert_sql,
)
from datafun_toolkit.logger import get_logger
import duckdb

from streaming.data_validation.data_contract_weather_foster import (
    REJECTED_WEATHER_FIELDNAMES,
    VALID_WEATHER_FIELDNAMES,
)
from streaming.data_validation.data_validation_foster import add_validation_errors

# === DECLARE EXPORTS ===

__all__ = [
    "clear_storage_tables",
    "create_storage_tables",
    "init_db",
    "log_storage_summary",
    "write_rejected_record",
    "write_valid_record",
]

# === CONFIGURE LOGGER ===

LOG = get_logger("C05-WEATHER-STORAGE", level="DEBUG")

# === DECLARE GLOBAL CONSTANTS FOR TABLES ===

VALID_TABLE_NAME: Final[str] = "consumed_valid_weather"
REJECTED_TABLE_NAME: Final[str] = "consumed_rejected_weather"

CONSUMED_VALID_FIELDNAMES: Final[list[str]] = [
    *VALID_WEATHER_FIELDNAMES,
    "heat_index",
    "wind_chill",
    "_kafka_key",
    "_kafka_partition",
    "_kafka_offset",
]

CONSUMED_REJECTED_FIELDNAMES: Final[list[str]] = [
    *REJECTED_WEATHER_FIELDNAMES,
    "_kafka_key",
    "_kafka_partition",
    "_kafka_offset",
]


# === DEFINE HELPER FUNCTIONS ===


def clean_valid_consumed_record(record: dict[str, Any]) -> dict[str, Any]:
    """Keep only the fields written to the valid consumed table.

    Arguments:
        record: A consumed Kafka message record.

    Returns:
        A dictionary containing only the expected table fields.
    """
    return {field: record.get(field, "") for field in CONSUMED_VALID_FIELDNAMES}


def clean_rejected_consumed_record(record: dict[str, Any]) -> dict[str, Any]:
    """Keep only the fields written to the rejected consumed table.

    Arguments:
        record: A consumed Kafka message record with validation errors.

    Returns:
        A dictionary containing only the expected table fields.
    """
    return {field: record.get(field, "") for field in CONSUMED_REJECTED_FIELDNAMES}


def create_storage_tables(db_path: Path) -> None:
    """Create the consumed message tables if they do not exist.

    Arguments:
        db_path: Path to the DuckDB database file.
    """
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            build_create_table_sql(VALID_TABLE_NAME, CONSUMED_VALID_FIELDNAMES)
        )
        conn.execute(
            build_create_table_sql(REJECTED_TABLE_NAME, CONSUMED_REJECTED_FIELDNAMES)
        )


def clear_storage_tables(db_path: Path) -> None:
    """Clear prior consumed message rows for a fresh run.

    Arguments:
        db_path: Path to the DuckDB database file.
    """
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(build_clear_table_sql(VALID_TABLE_NAME))
        conn.execute(build_clear_table_sql(REJECTED_TABLE_NAME))


def init_db(db_path: Path) -> None:
    """Initialize the DuckDB database for this project.

    Arguments:
        db_path: Path to the DuckDB database file.
    """
    create_storage_tables(db_path)
    clear_storage_tables(db_path)


def write_valid_record(db_path: Path, record: DataRecordDict) -> None:
    """Write one valid consumed weather record to DuckDB.

    Arguments:
        db_path: Path to the DuckDB database file.
        record: A valid consumed Kafka message record.
    """
    clean_record = clean_valid_consumed_record(record)
    insert_sql = build_insert_sql(VALID_TABLE_NAME, CONSUMED_VALID_FIELDNAMES)
    insert_values: list[Any] = [
        clean_record[field] for field in CONSUMED_VALID_FIELDNAMES
    ]
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(insert_sql, insert_values)


def write_rejected_record(
    db_path: Path, record: DataRecordDict, errors: list[str]
) -> None:
    """Write one rejected consumed weather record to DuckDB.

    Arguments:
        db_path: Path to the DuckDB database file.
        record: A rejected consumed Kafka message record.
        errors: Validation errors explaining why the record was rejected.
    """
    rejected_record = add_validation_errors(record=record, errors=errors)
    clean_record = clean_rejected_consumed_record(rejected_record)
    insert_sql = build_insert_sql(REJECTED_TABLE_NAME, CONSUMED_REJECTED_FIELDNAMES)
    insert_values = [clean_record[field] for field in CONSUMED_REJECTED_FIELDNAMES]
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(insert_sql, insert_values)


def log_storage_summary(db_path: Path) -> None:
    """Log DuckDB query results after consuming messages.

    Arguments:
        db_path: Path to the DuckDB database file.
    """
    sql_valid_count = f"SELECT COUNT(*) FROM {VALID_TABLE_NAME}"  # noqa: S608
    sql_rejected_count = f"SELECT COUNT(*) FROM {REJECTED_TABLE_NAME}"  # noqa: S608
    sql_by_station = f"""
        SELECT station_id, COUNT(*) AS reading_count,
               ROUND(AVG(CAST(temperature AS FLOAT)), 1) AS avg_temp,
               ROUND(AVG(CAST(heat_index AS FLOAT)), 1) AS avg_heat_index,
               ROUND(AVG(CAST(wind_chill AS FLOAT)), 1) AS avg_wind_chill
        FROM {VALID_TABLE_NAME}
        GROUP BY station_id
        ORDER BY station_id
        """  # noqa: S608

    with duckdb.connect(str(db_path)) as conn:
        valid_result = conn.execute(sql_valid_count).fetchone()
        valid_count = valid_result[0] if valid_result else 0

        rejected_result = conn.execute(sql_rejected_count).fetchone()
        rejected_count = rejected_result[0] if rejected_result else 0

        rows = conn.execute(sql_by_station).fetchall()

    LOG.info(f"DuckDB valid row(s): {valid_count}")
    LOG.info(f"DuckDB rejected row(s): {rejected_count}")
    LOG.info("DuckDB weather summary by station:")
    for station_id, count, avg_temp, avg_hi, avg_wc in rows:
        LOG.info(
            f"  {station_id}: {count} reading(s) | "
            f"avg_temp={avg_temp}F | "
            f"avg_heat_index={avg_hi}F | "
            f"avg_wind_chill={avg_wc}F"
        )
