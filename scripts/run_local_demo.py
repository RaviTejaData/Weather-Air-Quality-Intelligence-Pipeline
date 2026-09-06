"""
Quick local demo: runs the full Bronze -> Silver -> Gold -> Quality Gate
flow using pandas instead of PySpark, so it works with nothing but
`pip install -r requirements-demo.txt` -- no Spark, no Docker, no Airflow.

This is deliberately a second, simplified implementation path rather than a
wrapper around transform.py: it exists purely to let anyone evaluate the
project's logic in under a minute. The "real" pipeline (src/transform.py)
is PySpark + Delta Lake, orchestrated by Airflow -- see dags/weather_pipeline_dag.py
and the README for how the two relate.

Usage:
    python scripts/run_local_demo.py                 # live API calls
    python scripts/run_local_demo.py --offline        # bundled sample data
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.ingest import ingest_all, load_cities  # noqa: E402
from src.quality_checks import SILVER_QUALITY_CONFIG, run_all_checks  # noqa: E402


def bronze_files_to_silver_df(bronze_files: list[Path]) -> pd.DataFrame:
    import json

    rows = []
    for path in bronze_files:
        record = json.loads(path.read_text())
        weather = record.get("weather_raw", {}).get("current", {})
        aq = record.get("air_quality_raw", {}).get("current", {})
        rows.append(
            {
                "city": record["city"],
                "country": record["country"],
                "ingested_at": record["ingested_at"],
                "temperature_c": weather.get("temperature_2m"),
                "humidity_pct": weather.get("relative_humidity_2m"),
                "wind_speed_kmh": weather.get("wind_speed_10m"),
                "precipitation_mm": weather.get("precipitation"),
                "pm2_5": aq.get("pm2_5"),
                "pm10": aq.get("pm10"),
                "us_aqi": aq.get("us_aqi"),
            }
        )
    df = pd.DataFrame(rows)
    df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True)
    return df


def silver_to_gold(silver_df: pd.DataFrame) -> pd.DataFrame:
    def categorize_aqi(aqi: float) -> str:
        if aqi <= 50:
            return "Good"
        if aqi <= 100:
            return "Moderate"
        if aqi <= 150:
            return "Unhealthy for Sensitive Groups"
        if aqi <= 200:
            return "Unhealthy"
        return "Very Unhealthy"

    gold = silver_df.copy()
    gold["aqi_category"] = gold["us_aqi"].apply(categorize_aqi)
    gold["extreme_weather_flag"] = (gold["temperature_c"] >= 40) | (gold["temperature_c"] <= -20) | (
        gold["wind_speed_kmh"] >= 60
    )
    return gold[
        [
            "city",
            "country",
            "temperature_c",
            "humidity_pct",
            "wind_speed_kmh",
            "precipitation_mm",
            "us_aqi",
            "aqi_category",
            "extreme_weather_flag",
        ]
    ].sort_values("temperature_c", ascending=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--offline", action="store_true", help="Use bundled sample fixtures instead of live API calls.")
    parser.add_argument("--config", default=str(Path(__file__).resolve().parent.parent / "config" / "cities.yaml"))
    parser.add_argument("--sample-dir", default=str(Path(__file__).resolve().parent.parent / "data" / "sample"))
    parser.add_argument("--chart", action="store_true", help="Also save a PNG chart of results (requires matplotlib).")
    args = parser.parse_args()

    demo_dir = Path(__file__).resolve().parent.parent / "data" / "demo_run"
    demo_dir.mkdir(parents=True, exist_ok=True)

    cities = load_cities(args.config)

    if args.offline:
        # Demo mode ships with fixtures for a handful of cities; filter down to those.
        available = {p.stem for p in Path(args.sample_dir).glob("*.json")}
        cities = [c for c in cities if c.slug in available]

    print(f"Ingesting {len(cities)} cities ({'offline fixtures' if args.offline else 'live Open-Meteo API'})...")
    bronze_files = ingest_all(cities, demo_dir / "bronze", offline=args.offline, sample_dir=args.sample_dir)

    print("Building Silver layer...")
    silver_df = bronze_files_to_silver_df(bronze_files)

    print("Running data quality gate...")
    report = run_all_checks(silver_df, SILVER_QUALITY_CONFIG)
    print(report.summary())
    print()

    print("Building Gold layer...")
    gold_df = silver_to_gold(silver_df)

    gold_path = demo_dir / "gold_summary.csv"
    gold_df.to_csv(gold_path, index=False)

    print()
    print("=== Gold summary ===")
    print(gold_df.to_string(index=False))
    print()
    print(f"Saved to {gold_path}")

    if args.chart:
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(8, 4.5))
        ax.barh(gold_df["city"], gold_df["temperature_c"], color="#38d9c4")
        ax.set_xlabel("Temperature (°C)")
        ax.set_title("Current temperature by city")
        fig.tight_layout()
        chart_path = demo_dir / "temperature_chart.png"
        fig.savefig(chart_path, dpi=150)
        print(f"Saved chart to {chart_path}")


if __name__ == "__main__":
    main()
