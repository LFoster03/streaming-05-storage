"""src/streaming/kafka_producer_weather_foster.py.

Kafka producer: weather readings pipeline.

Reads weather readings from data/weather_readings.csv,
validates them against the data contract,
writes rejected records to a local CSV file,
and sends valid records to a Kafka topic one message at a time.

Start with main() at the bottom.
Work up to see how it all fits together.

Author: Lindsay Foster
Date: 2026-05

Terminal command to run this file from the root project folder:

    uv run python -m streaming.kafka_producer_weather_foster

"""

# === DECLARE IMPORTS ===

from collections.abc import Generator
import os
from pathlib import Path
import time
from typing import Any, Final

from datafun_streaming.core.types import DataRecordDict
from datafun_streaming.io.errors import missing_csv_field_message
from datafun_streaming.io.io_utils import (
    append_csv_row,
    format_message_for_log,
    read_csv_rows,
)
from datafun_streaming.kafka.kafka_connection_utils import verify_kafka_connection
from datafun_streaming.kafka.kafka_producer_utils import (
    create_producer,
    prepare_producer_topic,
    produce_kafka_message,
)
from datafun_streaming.kafka.kafka_settings import KafkaSettings
from datafun_toolkit.logger import get_logger, log_header, log_path
from dotenv import load_dotenv

from streaming.core.utils import log_env_vars
from streaming.data_validation.data_contract_weather_foster import (
    REJECTED_WEATHER_FIELDNAMES,
    STATIONS_REQUIRED_FIELDS,
    UNITS_REQUIRED_FIELDS,
    validate_weather_record,
)
from streaming.data_validation.data_validation_foster import (
    add_validation_errors,
    make_lookup_set,
    validate_reference_records,
)

# === CONFIGURE LOGGER ===

LOG = get_logger("P-WEATHER", level="DEBUG")

# === LOAD ENVIRONMENT VARIABLES ===

load_dotenv(override=True)
log_env_vars(LOG)

# === DECLARE GLOBAL CONSTANTS ===

msg_count = os.getenv("PRODUCER_MESSAGE_COUNT", "10")
msg_interval_seconds = os.getenv("PRODUCER_MESSAGE_INTERVAL_SECONDS", "1.0")

MESSAGE_COUNT: Final[int] = int(msg_count)
MESSAGE_INTERVAL_SECONDS: Final[float] = float(msg_interval_seconds)

# === DECLARE CONSTANT PATHS ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
OUTPUT_DIR: Final[Path] = DATA_DIR / "output"

WEATHER_CSV: Final[Path] = DATA_DIR / "weather_readings.csv"
STATIONS_CSV: Final[Path] = DATA_DIR / "stations.csv"
UNITS_CSV: Final[Path] = DATA_DIR / "units.csv"
REJECTED_WEATHER_CSV: Final[Path] = OUTPUT_DIR / "producer_rejected_weather.csv"


# ==========================================================
# DEFINE SECTION A. ACQUIRE RESOURCES AND GET READY HELPERS
# ==========================================================


def log_paths() -> None:
    """Log run header and all paths."""
    log_header(LOG, "P-WEATHER")
    LOG.info("========================")
    LOG.info("START weather producer main()")
    LOG.info("========================")
    log_path(LOG, "ROOT_DIR", ROOT_DIR)
    log_path(LOG, "DATA_DIR", DATA_DIR)
    log_path(LOG, "WEATHER_CSV", WEATHER_CSV)
    log_path(LOG, "STATIONS_CSV", STATIONS_CSV)
    log_path(LOG, "UNITS_CSV", UNITS_CSV)
    log_path(LOG, "REJECTED_WEATHER_CSV", REJECTED_WEATHER_CSV)


def load_settings() -> KafkaSettings:
    """Load settings from .env and log them.

    Returns:
        A KafkaSettings instance populated from environment variables.
    """
    LOG.info("Loading settings from .env...")
    settings = KafkaSettings.from_env()
    LOG.info(f"KAFKA_BOOTSTRAP_SERVERS           = {settings.bootstrap_servers}")
    LOG.info(f"KAFKA_TOPIC                       = {settings.topic}")
    LOG.info(f"PRODUCER_MESSAGE_COUNT            = {MESSAGE_COUNT}")
    LOG.info(f"PRODUCER_MESSAGE_INTERVAL_SECONDS = {MESSAGE_INTERVAL_SECONDS}")
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


def load_reference_data() -> tuple[set[str], set[str]]:
    """Load and validate reference data.

    Returns:
        A tuple of (valid_station_ids, valid_unit_systems).

    Raises:
        SystemExit: If any reference file is missing or invalid.
    """
    LOG.info("Loading validation reference data...")
    station_records = read_csv_rows(STATIONS_CSV)
    unit_records = read_csv_rows(UNITS_CSV)

    errors: list[str] = []
    errors.extend(
        validate_reference_records(
            records=station_records,
            required_fields=STATIONS_REQUIRED_FIELDS,
            label="stations.csv",
        )
    )
    errors.extend(
        validate_reference_records(
            records=unit_records,
            required_fields=UNITS_REQUIRED_FIELDS,
            label="units.csv",
        )
    )

    if errors:
        for error in errors:
            LOG.error(error)
        LOG.error("Reference data failed validation. Fix reference files first.")
        raise SystemExit(1)

    valid_station_ids = make_lookup_set(station_records, "station_id")
    valid_unit_systems = make_lookup_set(unit_records, "unit_system")
    LOG.info(
        f"Found {len(valid_station_ids)} valid stations, "
        f"{len(valid_unit_systems)} valid unit systems."
    )
    return valid_station_ids, valid_unit_systems


# ===========================================================================
# DEFINE SECTION P. PRODUCE MESSAGES HELPERS
# ===========================================================================


def get_message_key(message: dict[str, Any]) -> str:
    """Return the Kafka message key for a weather reading.

    We use station_id as the key so all readings from the same station
    go to the same Kafka partition, keeping them in order.
    """
    try:
        return str(message["station_id"])
    except KeyError as error:
        msg = missing_csv_field_message(
            field="station_id",
            available_fields=list(message.keys()),
        )
        raise KeyError(msg) from error


def generate_messages(count: int) -> Generator[dict[str, str]]:
    """Generate a stream of weather readings from the input CSV file.

    Arguments:
        count: How many readings to generate.

    Yields:
        One weather reading row dictionary at a time.
    """
    weather_rows = read_csv_rows(WEATHER_CSV)
    yield from weather_rows[:count]


def write_rejected_record(record: DataRecordDict, errors: list[str]) -> None:
    """Write one rejected record to the rejected output CSV."""
    append_csv_row(
        path=REJECTED_WEATHER_CSV,
        row=add_validation_errors(record=record, errors=errors),
        fieldnames=REJECTED_WEATHER_FIELDNAMES,
    )


def initialize_output() -> None:
    """Initialize output directory and clear rejected CSV from prior runs."""
    LOG.info("Initializing output...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if REJECTED_WEATHER_CSV.exists():
        REJECTED_WEATHER_CSV.unlink()
    LOG.info(f"Output directory ready: {OUTPUT_DIR.name}")


def send_messages(
    producer: Any,
    settings: KafkaSettings,
    valid_station_ids: set[str],
    valid_unit_systems: set[str],
) -> tuple[int, int]:
    """Generate, validate, and send messages to the Kafka topic.

    Arguments:
        producer: An open Kafka producer.
        settings: Kafka settings including the topic name.
        valid_station_ids: Set of known station IDs for validation.
        valid_unit_systems: Set of known unit systems for validation.

    Returns:
        A tuple of (sent_count, rejected_count).
    """
    LOG.info("Sending messages...")
    LOG.info(f"Sending up to {MESSAGE_COUNT} message(s) to topic {settings.topic!r}.")
    LOG.info("Watch each reading arrive. Press CTRL+C to stop early.\n")

    sent_count = 0
    rejected_count = 0

    try:
        for message in generate_messages(MESSAGE_COUNT):
            LOG.info(format_message_for_log(message))

            result = validate_weather_record(
                record=message,
                valid_station_ids=valid_station_ids,
                valid_unit_systems=valid_unit_systems,
            )

            if not result.is_valid:
                rejected_count += 1
                LOG.warning("MESSAGE REJECTED")
                LOG.warning(f"  errors={result.errors}")
                write_rejected_record(message, result.errors)
                continue

            key = get_message_key(message)
            LOG.info(f"  Sending message with key={key}")

            produce_kafka_message(
                producer=producer,
                topic=settings.topic,
                key=key,
                message=message,
            )

            sent_count += 1
            LOG.info(f"  MESSAGE SENT  sent={sent_count}")
            time.sleep(MESSAGE_INTERVAL_SECONDS)

    except (FileNotFoundError, KeyError, RuntimeError, ValueError) as error:
        LOG.error(str(error))
        LOG.error("Producer stopped before completing all messages.")
        raise SystemExit(1) from error

    return sent_count, rejected_count


def log_rejected(rejected_count: int) -> None:
    """Log the rejected records CSV path if any records were rejected.

    Arguments:
        rejected_count: The number of rejected records.
    """
    LOG.info("Checking for rejected records...")
    if rejected_count > 0:
        log_path(LOG, "  WROTE REJECTED_WEATHER_CSV", REJECTED_WEATHER_CSV)
    else:
        LOG.info("  No records rejected.")


# ===========================================================================
# DEFINE SECTION E. EXIT AND CLEANUP HELPERS
# ===========================================================================


def log_summary(sent_count: int, rejected_count: int, settings: KafkaSettings) -> None:
    """Log final summary statistics."""
    LOG.info("Summary:")
    LOG.info(f"Sent {sent_count} message(s) to topic {settings.topic!r}.")
    LOG.info(f"Rejected {rejected_count} message(s).")
    LOG.info("========================")
    LOG.info("Weather producer executed successfully!")
    LOG.info("========================")


# ===========================================================================
# MAIN FUNCTION
# ===========================================================================


def main() -> None:
    """Main entry point for the weather Kafka producer."""
    log_paths()

    LOG.info("========================")
    LOG.info("SECTION A. Acquire")
    LOG.info("========================")

    settings = load_settings()
    verify_connection(settings)
    prepare_producer_topic(settings)
    valid_station_ids, valid_unit_systems = load_reference_data()
    producer = create_producer(settings)

    LOG.info("========================")
    LOG.info("SECTION P. Produce Messages")
    LOG.info("========================")

    initialize_output()
    sent_count, rejected_count = send_messages(
        producer, settings, valid_station_ids, valid_unit_systems
    )
    log_rejected(rejected_count)

    LOG.info("========================")
    LOG.info("SECTION E. Exit")
    LOG.info("========================")

    producer.flush()
    log_summary(sent_count, rejected_count, settings)


# === CONDITIONAL EXECUTION GUARD ===

if __name__ == "__main__":
    main()
