import json
from pathlib import Path
from unittest.mock import MagicMock

from src.ingest import City, build_bronze_record, ingest_all, load_cities


def test_load_cities_parses_yaml(tmp_path: Path):
    config = tmp_path / "cities.yaml"
    config.write_text(
        "cities:\n  - name: Testville\n    country: XX\n    latitude: 1.0\n    longitude: 2.0\n"
    )
    cities = load_cities(config)
    assert len(cities) == 1
    assert cities[0].name == "Testville"
    assert cities[0].slug == "testville"


def test_build_bronze_record_wraps_payloads_without_mutating():
    city = City(name="London", country="GB", latitude=51.5, longitude=-0.1)
    weather = {"current": {"temperature_2m": 18.0}}
    air_quality = {"current": {"us_aqi": 40.0}}

    record = build_bronze_record(city, weather, air_quality)

    assert record["city"] == "London"
    assert record["weather_raw"] == weather
    assert record["air_quality_raw"] == air_quality
    assert "ingested_at" in record


def test_ingest_all_offline_mode_reads_fixtures(tmp_path: Path):
    sample_dir = tmp_path / "sample"
    sample_dir.mkdir()
    fixture = {
        "city": "London",
        "country": "GB",
        "latitude": 51.5,
        "longitude": -0.1,
        "ingested_at": "2020-01-01T00:00:00+00:00",
        "weather_raw": {"current": {"temperature_2m": 18.0}},
        "air_quality_raw": {"current": {"us_aqi": 40.0}},
    }
    (sample_dir / "london.json").write_text(json.dumps(fixture))

    cities = [City(name="London", country="GB", latitude=51.5, longitude=-0.1)]
    output_dir = tmp_path / "bronze"

    written = ingest_all(cities, output_dir, offline=True, sample_dir=sample_dir)

    assert len(written) == 1
    result = json.loads(written[0].read_text())
    assert result["city"] == "London"
    # Timestamp should be refreshed, not the stale fixture value.
    assert result["ingested_at"] != "2020-01-01T00:00:00+00:00"


def test_ingest_all_skips_a_failing_city_without_stopping_the_run(tmp_path: Path):
    good_city = City(name="London", country="GB", latitude=51.5, longitude=-0.1)
    bad_city = City(name="Nowhere", country="ZZ", latitude=0.0, longitude=0.0)

    sample_dir = tmp_path / "sample"
    sample_dir.mkdir()
    (sample_dir / "london.json").write_text(
        json.dumps(
            {
                "city": "London",
                "country": "GB",
                "latitude": 51.5,
                "longitude": -0.1,
                "ingested_at": "2020-01-01T00:00:00+00:00",
                "weather_raw": {},
                "air_quality_raw": {},
            }
        )
    )
    # No fixture written for "Nowhere" -> should raise internally and be skipped.

    written = ingest_all(
        [good_city, bad_city], tmp_path / "bronze", offline=True, sample_dir=sample_dir
    )
    assert len(written) == 1
    assert written[0].stem == "london"
