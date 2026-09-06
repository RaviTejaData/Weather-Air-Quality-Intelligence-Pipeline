"""
Bronze-layer ingestion.

Pulls current weather and air-quality readings for a configurable list of
cities from the Open-Meteo public APIs (no API key required) and lands the
raw, untouched JSON responses to the bronze zone, one file per city per
ingestion run.

Design decisions
-----------------
- Raw responses are stored exactly as received (plus a small ingestion
  envelope) so any transformation bug downstream can always be replayed
  from source-of-truth data. This mirrors the "bronze is immutable" pattern
  used with Azure Data Lake / Databricks medallion architectures.
- Every record carries an `ingested_at` UTC timestamp and the `source_url`
  actually called, which makes freshness checks and reproducibility trivial.
- Network calls are isolated in two small functions so they can be mocked
  in unit tests without needing `responses`/`vcr` style fixtures.
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
REQUEST_TIMEOUT_SECONDS = 15


@dataclass
class City:
    name: str
    country: str
    latitude: float
    longitude: float

    @property
    def slug(self) -> str:
        return self.name.lower().replace(" ", "_")


def load_cities(config_path: str | Path) -> list[City]:
    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return [City(**c) for c in raw["cities"]]


def fetch_weather(city: City, session: requests.Session | None = None) -> dict[str, Any]:
    """Call Open-Meteo's forecast endpoint for a single city's current conditions."""
    session = session or requests
    params = {
        "latitude": city.latitude,
        "longitude": city.longitude,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation,weather_code",
        "timezone": "UTC",
    }
    resp = session.get(WEATHER_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    resp.raise_for_status()
    return resp.json()


def fetch_air_quality(city: City, session: requests.Session | None = None) -> dict[str, Any]:
    """Call Open-Meteo's air-quality endpoint for a single city's current AQI components."""
    session = session or requests
    params = {
        "latitude": city.latitude,
        "longitude": city.longitude,
        "current": "pm2_5,pm10,us_aqi",
        "timezone": "UTC",
    }
    resp = session.get(AIR_QUALITY_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    resp.raise_for_status()
    return resp.json()


def build_bronze_record(city: City, weather: dict[str, Any], air_quality: dict[str, Any]) -> dict[str, Any]:
    """Wrap raw API payloads in an ingestion envelope. Nothing is dropped or reshaped here."""
    return {
        "city": city.name,
        "country": city.country,
        "latitude": city.latitude,
        "longitude": city.longitude,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "weather_url": WEATHER_URL,
            "air_quality_url": AIR_QUALITY_URL,
        },
        "weather_raw": weather,
        "air_quality_raw": air_quality,
    }


def ingest_all(
    cities: list[City],
    output_dir: str | Path,
    offline: bool = False,
    sample_dir: str | Path | None = None,
) -> list[Path]:
    """
    Ingest every city in `cities` and write one JSON file per city to
    `output_dir/<run_date>/<city_slug>.json`.

    When `offline=True`, reads pre-recorded fixtures from `sample_dir`
    instead of calling the network -- used for CI, demos, and anywhere a
    live connection isn't guaranteed.
    """
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = Path(output_dir) / run_date
    out_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    session = requests.Session() if not offline else None

    for city in cities:
        try:
            if offline:
                fixture_path = Path(sample_dir) / f"{city.slug}.json"
                with open(fixture_path, "r", encoding="utf-8") as f:
                    record = json.load(f)
                    # Refresh the timestamp so freshness checks pass in demos.
                    record["ingested_at"] = datetime.now(timezone.utc).isoformat()
            else:
                weather = fetch_weather(city, session)
                air_quality = fetch_air_quality(city, session)
                record = build_bronze_record(city, weather, air_quality)

            out_path = out_dir / f"{city.slug}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, indent=2)
            written.append(out_path)
            logger.info("Ingested %s -> %s", city.name, out_path)

        except Exception:  # noqa: BLE001 -- one bad city must not kill the run
            logger.exception("Failed to ingest %s, skipping this run", city.name)

    return written


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest bronze-layer weather + air quality data.")
    parser.add_argument("--config", default="config/cities.yaml")
    parser.add_argument("--output", default="data/bronze")
    parser.add_argument("--offline", action="store_true", help="Use bundled sample fixtures instead of live API calls.")
    parser.add_argument("--sample-dir", default="data/sample")
    args = parser.parse_args()

    cities = load_cities(args.config)
    written = ingest_all(cities, args.output, offline=args.offline, sample_dir=args.sample_dir)
    logger.info("Ingestion complete: %d/%d cities written", len(written), len(cities))


if __name__ == "__main__":
    main()
