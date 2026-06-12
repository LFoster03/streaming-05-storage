"""src/streaming/kafka_consumer_weather_foster.py.

Kafka consumer: weather readings pipeline.

Reads weather messages from a Kafka topic and runs the full pipeline:
  - Validates each message against the weather data contract
  - Computes derived fields (heat_index, wind_chill)
  - Stores each message in a DuckDB database

Start with main() at the bottom.
Work up to see how it all fits together.

Author: Lindsay Foster
Date: 2026-05

Terminal command to run this file from the root project folder:

    uv run python -m streaming.kafka_consumer_weather_foster

"""

# === DECLARE IMPORTS ===

import os
from pathlib import Path
from typing import Any, Final

from confluent_kafka.cimpl import OFFSET_BEGINNING, TopicPartition
from datafun_streaming.io.io_utils import append_csv_row, read_csv_rows
from datafun_streaming.kafka.kafka_admin_utils import (
    create_admin_client,
    get_topic_message_count,
    topic_exists,
)
from datafun_streaming.kafka.kafka_connection_utils import verify_kafka_connection
from datafun_streaming.kafka.kafka_consumer_utils import (
    consume_kafka_message,
    create_consumer,
)
from datafun_streaming.kafka.kafka_settings import KafkaSettings
from datafun_streaming.stats.stats_utils import RunningStats
from datafun_toolkit.logger import get_logger, log_header, log_path
from dotenv import load_dotenv

from streaming.core.utils import log_env_vars
from streaming.data_engineering.derived_fields_weather_foster import enrich_message
from streaming.data_validation.data_contract_weather_foster import (
    CONSUMED_FIELDNAMES,
    validate_weather_record,
)
from streaming.data_validation.data_validation_foster import (
    make_lookup_set,
)
from streaming.storage.storage_weather_foster import (
    init_db,
    log_storage_summary,
    write_valid_record,
)

# === CONFIGURE LOGGER ===

LOG = get_logger("C-WEATHER", level="DEBUG")

# === LOAD ENVIRONMENT VARIABLES ===

load_dotenv(override=True)
log_env_vars(LOG)

# === DECLARE GLOBAL CONSTANTS ===

TIMEOUT_SECONDS: Final[float] = float(os.getenv("CONSUMER_TIMEOUT_SECONDS", "10.0"))
MAX_MESSAGES: Final[int] = int(os.getenv("CONSUMER_MAX_MESSAGES", "1000"))

# === DECLARE CONSTANT PATHS ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
OUTPUT_DIR: Final[Path] = DATA_DIR / "output"

OUTPUT_CSV: Final[Path] = OUTPUT_DIR / "consumed_weather.csv"
OUTPUT_DB: Final[Path] = OUTPUT_DIR / "weather.duckdb"

STATIONS_CSV: Final[Path] = DATA_DIR / "stations.csv"
UNITS_CSV: Final[Path] = DATA_DIR / "units.csv"


# ==========================================================
# DEFINE SECTION A. ACQUIRE RESOURCES AND GET READY HELPERS
# ==========================================================


def log_paths() -> None:
    """Log run header and all paths."""
    log_header(LOG, "C-WEATHER")
    LOG.info("========================")
    LOG.info("START weather consumer main()")
    LOG.info("========================")
    log_path(LOG, "ROOT_DIR", ROOT_DIR)
    log_path(LOG, "DATA_DIR", DATA_DIR)
    log_path(LOG, "OUTPUT_CSV", OUTPUT_CSV)
    log_path(LOG, "OUTPUT_DB", OUTPUT_DB)
    log_path(LOG, "STATIONS_CSV", STATIONS_CSV)
    log_path(LOG, "UNITS_CSV", UNITS_CSV)


def load_settings() -> KafkaSettings:
    """Load settings from .env and log them.

    Returns:
        A KafkaSettings instance populated from environment variables.
    """
    LOG.info("Loading settings from .env...")
    settings = KafkaSettings.from_env()
    LOG.info(f"KAFKA_BOOTSTRAP_SERVERS  = {settings.bootstrap_servers}")
    LOG.info(f"KAFKA_TOPIC              = {settings.topic}")
    LOG.info(f"KAFKA_GROUP_ID           = {settings.group_id}")
    LOG.info(f"CONSUMER_TIMEOUT_SECONDS = {TIMEOUT_SECONDS}")
    LOG.info(f"CONSUMER_MAX_MESSAGES    = {MAX_MESSAGES}")
    return settings


def verify_connection(settings: KafkaSettings) -> None:
    """Verify Kafka is reachable before doing anything else.

    Raises:
        SystemExit: If Kafka is not reachable.
    """
    LOG.info("Verifying Kafka connection...")
    try:
        verify_kafka_connection(settings)
        LOG.info("Kafka port is reachable.")
    except ConnectionError as error:
        LOG.error(str(error))
        raise SystemExit(1) from error


def verify_topic(settings: KafkaSettings) -> None:
    """Verify the topic exists and has messages.

    Raises:
        SystemExit: If the topic does not exist or is empty.
    """
    LOG.info("Verifying Kafka topic...")
    admin = create_admin_client(settings)

    if not topic_exists(admin, settings.topic):
        LOG.error(f"Topic {settings.topic!r} does not exist.")
        LOG.error("Run the producer first.")
        raise SystemExit(1)

    message_count = get_topic_message_count(admin, settings.topic, settings)
    LOG.info(f"Topic {settings.topic!r} exists.")
    LOG.info(f"Found {message_count} message(s) available.")

    if message_count == 0:
        LOG.error("Topic is empty. Run the producer first.")
        raise SystemExit(1)


def get_kafka_consumer(settings: KafkaSettings) -> Any:
    """Create a Kafka consumer subscribed to the topic.

    Resets offsets to the beginning so all available messages are read.

    Returns:
        A confluent_kafka.Consumer instance subscribed to the topic.
    """
    LOG.info("Creating Kafka consumer...")
    consumer = create_consumer(settings)
    consumer.subscribe(
        [settings.topic],
        on_assign=lambda c, partitions: c.assign(
            [
                TopicPartition(
                    partition.topic,
                    partition.partition,
                    OFFSET_BEGINNING,
                )
                for partition in partitions
            ]
        ),
    )
    LOG.info(f"Subscribed to topic: {settings.topic!r} (reading from beginning)")
    return consumer


# ===========================================================================
# DEFINE SECTION C. CONSUME AND PROCESS MESSAGES HELPERS
# ===========================================================================


def initialize_output() -> RunningStats:
    """Initialize output resources.

    Returns:
        A RunningStats instance.
    """
    LOG.info("Initializing output...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if OUTPUT_CSV.exists():
        OUTPUT_CSV.unlink()
    LOG.info(f"Output CSV cleared: {OUTPUT_CSV.name}")

    init_db(OUTPUT_DB)
    LOG.info(f"Database initialized: {OUTPUT_DB.name}")

    return RunningStats()


def load_reference_data() -> tuple[set[str], set[str]]:
    """Load reference data used for message validation.

    Returns:
        A tuple of (valid_station_ids, valid_unit_systems).
    """
    LOG.info("Loading validation reference data...")
    station_records = read_csv_rows(STATIONS_CSV)
    unit_records = read_csv_rows(UNITS_CSV)

    valid_station_ids = make_lookup_set(station_records, "station_id")
    valid_unit_systems = make_lookup_set(unit_records, "unit_system")

    LOG.info(f"Found {len(valid_station_ids)} valid station(s).")
    LOG.info(f"Found {len(valid_unit_systems)} valid unit system(s).")
    return valid_station_ids, valid_unit_systems


def process_message(
    row: dict[str, Any],
    *,
    valid_station_ids: set[str],
    valid_unit_systems: set[str],
    stats: RunningStats,
) -> dict[str, Any] | None:
    """Process one consumed weather message.

    Steps:
      - Validate required fields and allowed values
      - Enrich with derived fields (heat_index, wind_chill)
      - Update running statistics on temperature

    Arguments:
        row: A raw consumed Kafka message row.
        valid_station_ids: Set of known station IDs for validation.
        valid_unit_systems: Set of known unit systems for validation.
        stats: Running statistics accumulator (tracks temperature).

    Returns:
        The enriched row, or None if validation failed.
    """
    result = validate_weather_record(
        record=row,
        valid_station_ids=valid_station_ids,
        valid_unit_systems=valid_unit_systems,
    )
    if not result.is_valid:
        LOG.warning(f"Validation failed for reading {row.get('reading_id', '?')}")
        LOG.warning(f"errors={result.errors}")
        return None

    enriched = enrich_message(row)
    LOG.info(f"heat_index={enriched['heat_index']}")
    LOG.info(f"wind_chill={enriched['wind_chill']}")

    temperature = float(row.get("temperature", 0.0))
    stats.update(temperature)
    LOG.info(f"running_avg_temp={stats.mean:.1f}F")

    return enriched


def consume_messages(
    consumer: Any,
    *,
    valid_station_ids: set[str],
    valid_unit_systems: set[str],
    stats: RunningStats,
) -> tuple[int, int]:
    """Consume and process messages from the Kafka topic.

    Runs until MAX_MESSAGES is reached or TIMEOUT_SECONDS elapses
    with no new message.

    All arguments after the asterisk must be passed as keyword arguments.

    Arguments:
        consumer: An open Kafka consumer subscribed to the topic.
        valid_station_ids: Set of known station IDs for validation.
        valid_unit_systems: Set of known unit systems for validation.
        stats: Running statistics accumulator.

    Returns:
        A tuple of (consumed_count, skipped_count).
    """
    LOG.info("Consuming messages...")
    LOG.info(f"Waiting for up to {MAX_MESSAGES} message(s).")
    LOG.info("Press CTRL+C to stop early.\n")

    consumed_count = 0
    skipped_count = 0

    while consumed_count + skipped_count < MAX_MESSAGES:
        row = consume_kafka_message(
            consumer=consumer,
            timeout_seconds=TIMEOUT_SECONDS,
        )

        if row is None:
            LOG.info(f"No message received within {TIMEOUT_SECONDS}s timeout.")
            LOG.info("Producer finished or paused. Stopping consumer.")
            break

        LOG.info(row)

        enriched = process_message(
            row,
            valid_station_ids=valid_station_ids,
            valid_unit_systems=valid_unit_systems,
            stats=stats,
        )

        if enriched is None:
            skipped_count += 1
            LOG.warning("MESSAGE REJECTED")
            LOG.warning(f"reading={row.get('reading_id', '?')}")
            LOG.warning(f"skipped={skipped_count}")
            continue

        write_valid_record(OUTPUT_DB, enriched)
        LOG.info(f"  reading={enriched['reading_id']}")

        append_csv_row(
            path=OUTPUT_CSV,
            row={field: enriched.get(field, "") for field in CONSUMED_FIELDNAMES},
            fieldnames=CONSUMED_FIELDNAMES,
        )

        consumed_count += 1
        LOG.info("MESSAGE ACCEPTED")
        LOG.info(f"reading={enriched['reading_id']}")
        LOG.info(f"station={enriched['station_id']}")
        LOG.info(f"temp={enriched['temperature']}F")
        LOG.info(f"heat_index={enriched['heat_index']}F")
        LOG.info(f"wind_chill={enriched['wind_chill']}F")
        LOG.info(f"consumed={consumed_count}")
        LOG.info("RUNNING STATS (temperature)")
        LOG.info(f"avg_temp={stats.mean:.1f}F")
        LOG.info(f"min_temp={stats.minimum:.1f}F")
        LOG.info(f"max_temp={stats.maximum:.1f}F")

    return consumed_count, skipped_count


def save_artifacts() -> None:
    """Save output artifacts or note their location."""
    LOG.info("Saving artifacts...")
    log_path(LOG, "WROTE OUTPUT_CSV", OUTPUT_CSV)
    log_path(LOG, "WROTE OUTPUT_DB", OUTPUT_DB)


# ===========================================================================
# DEFINE SECTION E. EXIT AND CLEANUP HELPERS
# ===========================================================================


def log_summary(
    consumed_count: int,
    skipped_count: int,
    stats: RunningStats,
    settings: KafkaSettings,
) -> None:
    """Log final summary statistics."""
    LOG.info("Summary:")
    LOG.info(f"Consumed {consumed_count} message(s) from topic {settings.topic!r}.")
    LOG.info(f"Skipped  {skipped_count} message(s).")
    log_path(LOG, "OUTPUT_CSV", OUTPUT_CSV)

    if stats.count > 0:
        LOG.info(f"  Avg temperature: {stats.mean:.1f}F")
        LOG.info(f"  Min temperature: {stats.minimum:.1f}F")
        LOG.info(f"  Max temperature: {stats.maximum:.1f}F")

    LOG.info("========================")
    LOG.info("Weather consumer executed successfully!")
    LOG.info("========================")


# ===========================================================================
# MAIN FUNCTION
# ===========================================================================


def main() -> None:
    """Main entry point for the weather Kafka consumer."""
    log_paths()

    LOG.info("========================")
    LOG.info("SECTION A. Acquire")
    LOG.info("========================")

    settings = load_settings()
    verify_connection(settings)
    verify_topic(settings)
    consumer = get_kafka_consumer(settings)

    LOG.info("========================")
    LOG.info("SECTION C. Consume and Process Messages")
    LOG.info("========================")

    stats = initialize_output()
    valid_station_ids, valid_unit_systems = load_reference_data()

    consumed_count = 0
    skipped_count = 0

    try:
        consumed_count, skipped_count = consume_messages(
            consumer,
            valid_station_ids=valid_station_ids,
            valid_unit_systems=valid_unit_systems,
            stats=stats,
        )
    finally:
        consumer.close()
        LOG.info("Kafka consumer closed.")

    save_artifacts()
    log_storage_summary(OUTPUT_DB)

    LOG.info("========================")
    LOG.info("SECTION E. Exit")
    LOG.info("========================")

    log_summary(consumed_count, skipped_count, stats, settings)


# === CONDITIONAL EXECUTION GUARD ===

if __name__ == "__main__":
    main()
