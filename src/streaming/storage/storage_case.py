"""src/streaming/storage/storage_case.py.

Project-specific DuckDB storage functions
used by the Kafka consumer.

This project persists consumed Kafka messages to disk
so they can be queried, counted, and analyzed while the consumer runs.
Without storage, all consumed data is lost when the consumer stops.

This module creates two DuckDB tables:
  - consumed_valid_sales: records that passed all validation checks
  - consumed_rejected_sales: records that failed, with error details attached

For each consumed message the consumer calls:
  1. create_storage_tables() once at startup to initialize the tables.
  2. clear_storage_tables() once at startup to reset from any prior run.
  3. write_valid_record() or write_rejected_record() for each message.
  4. log_storage_summary() at the end to report what was stored.

The generic SQL builders (CREATE, DELETE, INSERT) come from
datafun_streaming.storage.duckdb_sql and work with any table.
The domain-specific table names and field lists are defined here.

This module is specific to our sales data and consumer application,
but the overall structure can be adapted for other types of streaming data.

Other ideas for domain-specific storage include:
  - Add a table for enriched records with derived fields (subtotal, tax, total).
  - Store a running count per region to track which regions are busiest.
  - Add a summary table that stores one row per consumer run with totals.
  - Store reference data (regions, products) in DuckDB for SQL-based lookups.
  - Add a table for messages that timed out or had Kafka delivery errors.

Author: Denise Case
Date: 2026-05

OBS:
  Don't edit this file - it should remain a working example.
  Copy it, rename it storage_yourname.py,
  and modify your copy for your own storage needs.
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

from streaming.data_validation.data_contract_case import (
    REJECTED_SALES_FIELDNAMES,
    VALID_SALES_FIELDNAMES,
)
from streaming.data_validation.data_validation_case import add_validation_errors

# === DECLARE EXPORTS ===

# Use the built-in __all__ variable to declare a list of
# public objects that this module exports.
# This is a common Python convention that helps other developers understand
# which functions are intended for use outside this module.

__all__ = [
    "clear_storage_tables",
    "create_storage_tables",
    "init_db",
    "log_storage_summary",
    "write_rejected_record",
    "write_valid_record",
]

# === CONFIGURE LOGGER ONCE PER PYTHON FILE (MODULE) ===

LOG = get_logger("C05-STORAGE", level="DEBUG")

# === DECLARE GLOBAL CONSTANTS FOR TABLES ===

# NEW: We need to name two tables,
# one for valid consumed records and
# one for rejected records with validation errors.

VALID_TABLE_NAME: Final[str] = "consumed_valid_sales"
REJECTED_TABLE_NAME: Final[str] = "consumed_rejected_sales"

# NEW: For each of these tables,
# we need to define the expected field names,
# which are based on the data contract and validation rules.
# They also include the Kafka metadata fields for partition and offset,
# which are useful for debugging and tracing messages.

# _kafka_key is the selected key field from the data contract, which is optional and may be empty.
# _kafka_partition refers to the Kafka partition number where the message was consumed from.
# _kafka_offset refers to the Kafka offset number of the consumed message within its partition.
# The offset is the unique identifier for the message in that partition
# and useful for tracking consumption progress and debugging.

CONSUMED_VALID_FIELDNAMES: Final[list[str]] = [
    *VALID_SALES_FIELDNAMES,
    "_kafka_key",
    "_kafka_partition",
    "_kafka_offset",
]

CONSUMED_REJECTED_FIELDNAMES: Final[list[str]] = [
    *REJECTED_SALES_FIELDNAMES,
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
    # Use list comprehension syntax to convert one list to another list.
    # In this case, for each field in the expected field list,
    # get the value from the record.
    # If the field is missing from the record,
    # use an empty string as a default value.
    return {field: record.get(field, "") for field in CONSUMED_VALID_FIELDNAMES}


def clean_rejected_consumed_record(record: dict[str, Any]) -> dict[str, Any]:
    """Keep only the fields written to the rejected consumed table.

    Arguments:
        record: A consumed Kafka message record with validation errors.

    Returns:
        A dictionary containing only the expected table fields.
    """
    # Use list comprehension syntax to convert one list to another list.
    # In this case, for each field in the expected field list,
    # get the value from the record.
    # If the field is missing from the record,
    # use an empty string as a default value.
    return {field: record.get(field, "") for field in CONSUMED_REJECTED_FIELDNAMES}


def create_storage_tables(db_path: Path) -> None:
    """Create the consumed message tables if they do not exist.

    NEW: We need to create two tables, one for valid records and one for rejected records.
    Each table has its own set of expected fields based on the data contract and validation rules.

    Arguments:
        db_path: Path to the DuckDB database file.

    Returns:
        None.
    """
    # Open a connection to the DuckDB database.
    with duckdb.connect(str(db_path)) as conn:
        # Use the connection to execute SQL statements.
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

    Returns:
        None.
    """
    # Open a connection to the DuckDB database.
    with duckdb.connect(str(db_path)) as conn:
        # Use the connection to execute SQL statements.
        conn.execute(build_clear_table_sql(VALID_TABLE_NAME))
        conn.execute(build_clear_table_sql(REJECTED_TABLE_NAME))


def init_db(db_path: Path) -> None:
    """Initialize the DuckDB database for this project.

    NEW: Create the expected storage tables and clear old rows
    so each consumer run starts with empty tables.

    Arguments:
        db_path: Path to the DuckDB database file.

    Returns:
        None.
    """
    create_storage_tables(db_path)
    clear_storage_tables(db_path)


def write_valid_record(db_path: Path, record: DataRecordDict) -> None:
    """Write one valid consumed sales record to DuckDB.

    NEW: Create this helper function to write
    valid records to the consumed_valid_sales table.

    Opens, writes, and closes the database for each record
    so the file is not locked between messages.

    Arguments:
        db_path: Path to the DuckDB database file.
        record: A valid consumed Kafka message record.

    Returns:
        None.
    """
    # Call clean_valid_consumed_record() to keep only
    # the expected fields for the valid table.
    # Save the results as a new dictionary named clean_record.
    clean_record = clean_valid_consumed_record(record)

    # Build the SQL INSERT statement for the valid table.
    insert_sql = build_insert_sql(VALID_TABLE_NAME, CONSUMED_VALID_FIELDNAMES)

    # Initialize an empty list to hold the values to insert.
    insert_values: list[Any] = []

    # For each field in the expected field list:
    for field in CONSUMED_VALID_FIELDNAMES:
        # Get the value from the cleaned record.
        value = clean_record[field]

        # Append the value to the insert values list.
        insert_values.append(value)

    # Open a connection to the DuckDB database.
    with duckdb.connect(str(db_path)) as conn:
        # Use the connection to execute the SQL INSERT statement
        # with the ordered list of values.
        conn.execute(insert_sql, insert_values)


def write_rejected_record(
    db_path: Path, record: DataRecordDict, errors: list[str]
) -> None:
    """Write one rejected consumed sales record to DuckDB.

    NEW: Create this helper function to write
    rejected records to the consumed_rejected_sales table.

    Opens, writes, and closes the database for each record
    so the file is not locked between messages.

    Arguments:
        db_path: Path to the DuckDB database file.
        record: A rejected consumed Kafka message record.
        errors: Validation errors explaining why the record was rejected.

    Returns:
        None.
    """
    # Call add_validation_errors() to add the validation errors
    # passed in to the record as a new field.
    # Save the results as a new dictionary named rejected_record.
    rejected_record = add_validation_errors(record=record, errors=errors)

    # Call clean_rejected_consumed_record() to keep only
    # the expected fields for the rejected table.
    # Save the results as a new dictionary named clean_record.
    clean_record = clean_rejected_consumed_record(rejected_record)

    # Build the SQL INSERT statement for the rejected table.
    insert_sql = build_insert_sql(REJECTED_TABLE_NAME, CONSUMED_REJECTED_FIELDNAMES)

    # Build one ordered list of values to match the field order
    # in CONSUMED_REJECTED_FIELDNAMES.
    insert_values = [clean_record[field] for field in CONSUMED_REJECTED_FIELDNAMES]

    # Open a connection to the DuckDB database.
    with duckdb.connect(str(db_path)) as conn:
        # Use the connection to execute the SQL INSERT statement
        # with the ordered list of values.
        conn.execute(insert_sql, insert_values)


def log_storage_summary(db_path: Path) -> None:
    """Log simple DuckDB query results after consuming messages.

    Arguments:
        db_path: Path to the DuckDB database file.

    Returns:
        None.
    """
    # Constructed SQL statements can be security risks for
    # SQL injection if they include untrusted input.
    # In this case, the table names are constants defined in this module,
    # so they are not influenced by user input and are safe to use directly in SQL.
    # We add the "no QA" annotation to tell security linters
    # that we have reviewed this code and it is not vulnerable to SQL injection.
    sql_valid_count = f"SELECT COUNT(*) FROM {VALID_TABLE_NAME}"  # noqa: S608
    sql_rejected_count = f"SELECT COUNT(*) FROM {REJECTED_TABLE_NAME}"  # noqa: S608
    sql_by_region = f"""
        SELECT region_id, COUNT(*) AS sale_count
        FROM {VALID_TABLE_NAME}
        GROUP BY region_id
        ORDER BY region_id
        """  # noqa: S608

    # Open a connection to the DuckDB database.
    with duckdb.connect(str(db_path)) as conn:
        # use the connection to execute the SQL statements and fetch results.
        valid_result = conn.execute(sql_valid_count).fetchone()

        # valid_result is a tuple like (42,) or None if the query returned no rows.
        # So the count is either the first element of the tuple
        # or 0 if there was no result.
        valid_count = valid_result[0] if valid_result else 0

        # Same thing for rejected messages
        rejected_result = conn.execute(sql_rejected_count).fetchone()
        rejected_count = rejected_result[0] if rejected_result else 0

        # For the by-region query, we expect multiple rows, so we use fetchall() to get a list of tuples.
        # The variable rows will be a list of tuples
        # like [("US-MO", 10), ("US-CA", 5), ...]
        # or an empty list if there are no valid records.
        rows = conn.execute(sql_by_region).fetchall()

    LOG.info(f"DuckDB valid row(s): {valid_count}")
    LOG.info(f"DuckDB rejected row(s): {rejected_count}")

    LOG.info("DuckDB valid sale count by region:")
    for region_id, sale_count in rows:
        LOG.info(f"  {region_id}: {sale_count}")
