"""src/streaming/data_validation/data_contract_weather_foster.py.

Defines what a valid weather reading message looks like:
required fields, allowed values, reference table fields,
and output field order.

Use the data/*.csv files as the source of truth for the data contract.

The reusable validation helpers live in core/validation_utils.py.
The domain-specific field rules and validate_weather_record live here.

Author: Lindsay Foster
Date: 2026-05

"""

# === DECLARE IMPORTS ===

from typing import Any, Final

from datafun_streaming.core.types import DataRecordDict
from datafun_streaming.data_validation.types import ValidationResult
from datafun_streaming.data_validation.validation_utils import (
    validate_datetime,
    validate_positive_integer,
    validate_required_fields,
)

# === DECLARE REQUIRED FIELDS ===

# === EVENT TABLE FIELDS ===

WEATHER_REQUIRED_FIELDS: Final[list[str]] = [
    "reading_id",
    "datetime",
    "station_id",
    "unit_system",
    "temperature",
    "humidity_pct",
    "wind_speed",
    "condition",
    "device_id",
]

WEATHER_OPTIONAL_FIELDS: Final[list[str]] = []

VALID_WEATHER_FIELDNAMES: Final[list[str]] = [
    *WEATHER_REQUIRED_FIELDS,
    *WEATHER_OPTIONAL_FIELDS,
]

# === REFERENCE TABLE FIELDS ===

STATIONS_REQUIRED_FIELDS: Final[list[str]] = [
    "station_id",
    "station_name",
    "city",
    "state_code",
    "country",
    "latitude",
    "longitude",
    "elevation_ft",
    "timezone",
]

UNITS_REQUIRED_FIELDS: Final[list[str]] = [
    "unit_system",
    "temp_unit",
    "speed_unit",
    "temp_label",
    "speed_label",
]

# === ALLOWED VALUES ===

ALLOWED_CONDITIONS: Final[set[str]] = {
    "sunny",
    "cloudy",
    "rainy",
    "windy",
    "humid",
    "snowy",
    "foggy",
}

ALLOWED_UNIT_SYSTEMS: Final[set[str]] = {
    "imperial",
    "metric",
}

# === OUTPUT FIELD ORDER ===

CONSUMED_FIELDNAMES: Final[list[str]] = [
    *WEATHER_REQUIRED_FIELDS,
    "heat_index",
    "wind_chill",
    "_kafka_key",
    "_kafka_partition",
    "_kafka_offset",
]

REJECTED_WEATHER_FIELDNAMES: Final[list[str]] = [
    *WEATHER_REQUIRED_FIELDS,
    "validation_errors",
]


# === DOMAIN-SPECIFIC VALIDATION ===


def validate_weather_record(
    *,
    record: DataRecordDict,
    valid_station_ids: set[str],
    valid_unit_systems: set[str],
) -> ValidationResult:
    """Validate one weather reading against this project's data contract.

    All arguments after the asterisk must be passed as keyword arguments.

    Arguments:
        record: The message to validate.
        valid_station_ids: The set of valid station_id values
            from the stations reference table.
        valid_unit_systems: The set of valid unit_system values
            from the units reference table.

    Returns:
        A ValidationResult indicating whether the record is valid
        and any errors found.
    """
    errors: list[str] = []

    errors.extend(
        validate_required_fields(
            record=record,
            required_fields=WEATHER_REQUIRED_FIELDS,
        )
    )

    if errors:
        return ValidationResult(is_valid=False, errors=errors)

    if record["station_id"] not in valid_station_ids:
        errors.append(f"Unknown station_id: {record['station_id']!r}")

    if record["unit_system"] not in valid_unit_systems:
        errors.append(f"Unknown unit_system: {record['unit_system']!r}")

    if record["condition"] not in ALLOWED_CONDITIONS:
        errors.append(f"Invalid condition: {record['condition']!r}")

    errors.extend(validate_datetime(record["datetime"]))

    errors.extend(validate_positive_integer(record["humidity_pct"]))

    has_errors = bool(errors)
    is_result_valid = not has_errors

    return ValidationResult(is_valid=is_result_valid, errors=errors)


# === OUTPUT HELPERS ===


def keep_weather_fields(row: dict[str, Any]) -> dict[str, Any]:
    """Return only required weather fields in standard order.

    Arguments:
        row: The original message as a dict.

    Returns:
        A new dict with only the required fields in the standard order.
    """
    return {field: row.get(field, "") for field in WEATHER_REQUIRED_FIELDS}
