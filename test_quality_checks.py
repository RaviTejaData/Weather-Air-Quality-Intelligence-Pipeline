from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from src import quality_checks as qc


def make_row(**overrides):
    row = {
        "city": "London",
        "country": "GB",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "temperature_c": 18.0,
        "humidity_pct": 55.0,
        "us_aqi": 40.0,
    }
    row.update(overrides)
    return row


def test_run_all_checks_passes_on_clean_data():
    df = pd.DataFrame([make_row(), make_row(city="Tokyo", temperature_c=24.0)])
    report = qc.run_all_checks(df, qc.SILVER_QUALITY_CONFIG)
    assert report.passed


def test_completeness_fails_on_nulls():
    df = pd.DataFrame([make_row(temperature_c=None)])
    result = qc.check_completeness(df, ["city", "temperature_c"])
    assert not result.passed


def test_value_ranges_flags_impossible_temperature():
    df = pd.DataFrame([make_row(temperature_c=250.0)])
    result = qc.check_value_ranges(df, {"temperature_c": (-90.0, 60.0)})
    assert not result.passed
    assert "temperature_c" in result.details


def test_value_ranges_passes_within_bounds():
    df = pd.DataFrame([make_row(temperature_c=-15.0)])
    result = qc.check_value_ranges(df, {"temperature_c": (-90.0, 60.0)})
    assert result.passed


def test_freshness_fails_on_stale_data():
    stale_time = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
    df = pd.DataFrame([make_row(ingested_at=stale_time)])
    result = qc.check_freshness(df, "ingested_at", max_age_minutes=180)
    assert not result.passed


def test_freshness_passes_on_recent_data():
    df = pd.DataFrame([make_row()])
    result = qc.check_freshness(df, "ingested_at", max_age_minutes=180)
    assert result.passed


def test_row_count_fails_when_below_minimum():
    df = pd.DataFrame([])
    result = qc.check_row_count(df, expected_min=1)
    assert not result.passed


def test_run_all_checks_raises_on_blocking_failure():
    df = pd.DataFrame([make_row(temperature_c=999.0)])
    with pytest.raises(qc.DataQualityError):
        qc.run_all_checks(df, qc.SILVER_QUALITY_CONFIG)


def test_schema_check_detects_wrong_type():
    df = pd.DataFrame([make_row(temperature_c="not-a-number")])
    result = qc.check_schema(df, {"temperature_c": "floating"})
    assert not result.passed
