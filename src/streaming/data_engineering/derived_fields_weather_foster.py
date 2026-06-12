"""src/streaming/data_engineering/derived_fields_weather_foster.py.

Derived field calculations for weather reading messages.

Contains functions that compute fields not present in the raw Kafka message.
These fields are calculated by the consumer from the raw message fields.

Derived fields computed here:
  - heat_index: feels-like temperature accounting for heat and humidity
  - wind_chill: feels-like temperature accounting for cold and wind speed

The producer sends raw measurements only.
The consumer is responsible for all derived calculations.

Author: Lindsay Foster
Date: 2026-05

"""

# === DECLARE IMPORTS ===

import logging
from typing import Any, Final

# === DECLARE EXPORTS ===

__all__ = [
    "HEAT_INDEX_TEMP_THRESHOLD",
    "WIND_CHILL_TEMP_THRESHOLD",
    "compute_heat_index",
    "compute_wind_chill",
    "enrich_message",
]

# === DECLARE CONSTANTS ===

# Heat index is only meaningful when temperature is at or above this value (F).
HEAT_INDEX_TEMP_THRESHOLD: Final[float] = 80.0

# Wind chill is only meaningful when temperature is at or below this value (F).
WIND_CHILL_TEMP_THRESHOLD: Final[float] = 50.0

# === CONFIGURE LOGGER ===

LOG = logging.getLogger(__name__)

# === DEFINE DERIVED FIELD FUNCTIONS ===


def compute_heat_index(temperature: float, humidity: float) -> float:
    """Compute the heat index (feels-like temperature) using the Rothfusz regression formula.

    Used by the National Weather Service.
    Only meaningful when temperature >= 80F and humidity >= 40%.
    Returns the raw temperature if conditions are not met.

    Arguments:
        temperature: Air temperature in Fahrenheit.
        humidity: Relative humidity as a percentage (e.g. 65 for 65%).

    Returns:
        Heat index in Fahrenheit, rounded to 1 decimal place.
    """
    if temperature < HEAT_INDEX_TEMP_THRESHOLD or humidity < 40:
        return round(temperature, 1)

    hi = (
        -42.379
        + 2.04901523 * temperature
        + 10.14333127 * humidity
        - 0.22475541 * temperature * humidity
        - 0.00683783 * temperature**2
        - 0.05481717 * humidity**2
        + 0.00122874 * temperature**2 * humidity
        + 0.00085282 * temperature * humidity**2
        - 0.00000199 * temperature**2 * humidity**2
    )
    return round(hi, 1)


def compute_wind_chill(temperature: float, wind_speed: float) -> float:
    """Compute the wind chill (feels-like temperature) using the formula.

    Used by the National Weather Service.
    Only meaningful when temperature <= 50F and wind speed > 3 mph.
    Returns the raw temperature if conditions are not met.

    Arguments:
        temperature: Air temperature in Fahrenheit.
        wind_speed: Wind speed in miles per hour.

    Returns:
        Wind chill in Fahrenheit, rounded to 1 decimal place.
    """
    if temperature > WIND_CHILL_TEMP_THRESHOLD or wind_speed <= 3:
        return round(temperature, 1)

    wc = (
        35.74
        + 0.6215 * temperature
        - 35.75 * wind_speed**0.16
        + 0.4275 * temperature * wind_speed**0.16
    )
    return round(wc, 1)


def enrich_message(
    row: dict[str, Any],
) -> dict[str, Any]:
    """Add all derived fields to a raw weather message row.

    Computes heat_index and wind_chill from the raw message fields.

    Arguments:
        row: A validated raw message row.

    Returns:
        A new dict containing all original fields plus derived fields.
    """
    temperature = float(row.get("temperature", 0.0))
    humidity = float(row.get("humidity_pct", 0.0))
    wind_speed = float(row.get("wind_speed", 0.0))

    heat_index = compute_heat_index(temperature, humidity)
    wind_chill = compute_wind_chill(temperature, wind_speed)

    return {
        **row,
        "heat_index": heat_index,
        "wind_chill": wind_chill,
    }
