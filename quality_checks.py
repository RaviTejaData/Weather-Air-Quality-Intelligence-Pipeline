"""
Data quality gate, run between Silver and Gold.

This module intentionally has zero Spark dependency: checks operate on a
pandas DataFrame (Spark DataFrames can be sampled/collected via
`.toPandas()` for the row counts this pipeline deals with). That keeps the
quality logic unit-testable in milliseconds, independent of a Spark
session, and reusable if a future source is pandas-native.

The check set mirrors a standard source-to-target validation pass:
  - completeness   (are required fields populated?)
  - schema         (are the right columns/types present?)
  - range          (are values physically/business-plausible?)
  - freshness      (is this data recent enough to trust?)
  - volume         (did we get roughly the amount of data we expected?)

Each check returns a `CheckResult`. `run_all_checks` aggregates them into a
`DataQualityReport` and raises `DataQualityError` if any check marked
`blocking=True` fails, so a bad run can halt the pipeline before Gold is
published rather than silently poisoning a dashboard.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd


class DataQualityError(Exception):
    """Raised when one or more blocking data quality checks fail."""


@dataclass
class CheckResult:
    name: str
    passed: bool
    blocking: bool
    details: str = ""


@dataclass
class DataQualityReport:
    results: list[CheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def blocking_failures(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed and r.blocking]

    def summary(self) -> str:
        lines = [f"Data Quality Report — {'PASS' if self.passed else 'FAIL'}"]
        for r in self.results:
            status = "PASS" if r.passed else ("FAIL (blocking)" if r.blocking else "WARN")
            lines.append(f"  [{status}] {r.name}: {r.details}")
        return "\n".join(lines)


def check_completeness(df: pd.DataFrame, required_columns: list[str]) -> CheckResult:
    null_counts = {col: int(df[col].isna().sum()) for col in required_columns if col in df.columns}
    missing_columns = [col for col in required_columns if col not in df.columns]
    total_nulls = sum(null_counts.values())

    passed = total_nulls == 0 and not missing_columns
    details = f"missing_columns={missing_columns}, null_counts={null_counts}" if not passed else "no missing columns or nulls"
    return CheckResult("completeness", passed, blocking=True, details=details)


def check_schema(df: pd.DataFrame, expected_types: dict[str, str]) -> CheckResult:
    """expected_types maps column name -> pandas dtype kind, e.g. {'temperature_c': 'float'}."""
    mismatches = []
    for col, expected_kind in expected_types.items():
        if col not in df.columns:
            mismatches.append(f"{col} missing")
            continue
        actual_kind = pd.api.types.infer_dtype(df[col], skipna=True)
        if expected_kind not in actual_kind:
            mismatches.append(f"{col}: expected~{expected_kind}, got {actual_kind}")

    passed = not mismatches
    details = "; ".join(mismatches) if mismatches else "schema matches expectations"
    return CheckResult("schema", passed, blocking=True, details=details)


def check_value_ranges(df: pd.DataFrame, rules: dict[str, tuple[float, float]]) -> CheckResult:
    violations = {}
    for col, (low, high) in rules.items():
        if col not in df.columns:
            continue
        out_of_range = df[(df[col] < low) | (df[col] > high)]
        if len(out_of_range) > 0:
            violations[col] = {
                "count": len(out_of_range),
                "example_rows": out_of_range["city"].tolist()[:5] if "city" in df.columns else [],
            }

    passed = not violations
    details = f"violations={violations}" if violations else "all values within expected ranges"
    return CheckResult("value_ranges", passed, blocking=True, details=details)


def check_freshness(df: pd.DataFrame, timestamp_col: str, max_age_minutes: int) -> CheckResult:
    if timestamp_col not in df.columns or df.empty:
        return CheckResult("freshness", False, blocking=True, details=f"no data or missing column '{timestamp_col}'")

    timestamps = pd.to_datetime(df[timestamp_col], utc=True)
    latest = timestamps.max()
    now = pd.Timestamp.now(tz=timezone.utc)
    age = now - latest
    passed = age <= timedelta(minutes=max_age_minutes)
    details = f"latest record is {age} old (limit {max_age_minutes} min)"
    return CheckResult("freshness", passed, blocking=False, details=details)


def check_row_count(df: pd.DataFrame, expected_min: int) -> CheckResult:
    passed = len(df) >= expected_min
    details = f"got {len(df)} rows, expected at least {expected_min}"
    return CheckResult("row_count", passed, blocking=True, details=details)


def run_all_checks(df: pd.DataFrame, config: dict[str, Any]) -> DataQualityReport:
    """
    `config` shape:
    {
        "required_columns": [...],
        "expected_types": {...},
        "value_ranges": {...},
        "freshness": {"column": "...", "max_age_minutes": 120},
        "expected_min_rows": 1,
    }
    """
    report = DataQualityReport()
    report.results.append(check_completeness(df, config["required_columns"]))
    report.results.append(check_schema(df, config["expected_types"]))
    report.results.append(check_value_ranges(df, config["value_ranges"]))
    report.results.append(
        check_freshness(df, config["freshness"]["column"], config["freshness"]["max_age_minutes"])
    )
    report.results.append(check_row_count(df, config["expected_min_rows"]))

    if report.blocking_failures:
        raise DataQualityError(report.summary())

    return report


# Default rule set for the Silver weather+air-quality table used by this pipeline.
SILVER_QUALITY_CONFIG: dict[str, Any] = {
    "required_columns": ["city", "country", "ingested_at", "temperature_c"],
    "expected_types": {
        "temperature_c": "floating",
        "humidity_pct": "floating",
        "city": "string",
    },
    "value_ranges": {
        "temperature_c": (-90.0, 60.0),        # physical bounds on Earth's surface
        "humidity_pct": (0.0, 100.0),
        "us_aqi": (0.0, 500.0),
    },
    "freshness": {"column": "ingested_at", "max_age_minutes": 180},
    "expected_min_rows": 1,
}
