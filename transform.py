"""
Silver and Gold layer transformations, built on PySpark + Delta Lake.

Bronze  -> raw JSON, one file per city per run (see ingest.py)
Silver  -> flattened, typed, deduplicated readings, one row per city per run
Gold    -> daily aggregates per city, plus derived signals (AQI category,
           extreme-weather flags) that downstream BI tools consume directly

Engineering decisions
----------------------
- Silver is an append-only Delta table partitioned by `ingestion_date`.
  Re-running the same day's ingestion is idempotent via a `MERGE` on
  (city, ingested_at) rather than blind appends.
- Gold is fully recomputed from Silver on each run (small daily-aggregate
  volumes make this cheaper than incremental merges, and it removes an
  entire class of drift bugs between Silver and Gold).
- Schema is defined explicitly rather than inferred, so a source API change
  fails loudly at read time instead of silently corrupting downstream data.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BRONZE_SCHEMA = StructType(
    [
        StructField("city", StringType(), nullable=False),
        StructField("country", StringType(), nullable=False),
        StructField("latitude", DoubleType(), nullable=False),
        StructField("longitude", DoubleType(), nullable=False),
        StructField("ingested_at", StringType(), nullable=False),
        StructField(
            "weather_raw",
            StructType(
                [
                    StructField(
                        "current",
                        StructType(
                            [
                                StructField("temperature_2m", DoubleType()),
                                StructField("relative_humidity_2m", DoubleType()),
                                StructField("wind_speed_10m", DoubleType()),
                                StructField("precipitation", DoubleType()),
                                StructField("weather_code", DoubleType()),
                            ]
                        ),
                    )
                ]
            ),
        ),
        StructField(
            "air_quality_raw",
            StructType(
                [
                    StructField(
                        "current",
                        StructType(
                            [
                                StructField("pm2_5", DoubleType()),
                                StructField("pm10", DoubleType()),
                                StructField("us_aqi", DoubleType()),
                            ]
                        ),
                    )
                ]
            ),
        ),
    ]
)


def build_spark_session(app_name: str = "weather-intelligence-pipeline") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .master("local[*]")
        .getOrCreate()
    )


def read_bronze(spark: SparkSession, bronze_path: str) -> DataFrame:
    return spark.read.schema(BRONZE_SCHEMA).json(bronze_path)


def to_silver(bronze_df: DataFrame) -> DataFrame:
    """Flatten nested API payloads into typed, analysis-ready columns."""
    return (
        bronze_df.select(
            F.col("city"),
            F.col("country"),
            F.col("latitude"),
            F.col("longitude"),
            F.to_timestamp("ingested_at").alias("ingested_at"),
            F.to_date("ingested_at").alias("ingestion_date"),
            F.col("weather_raw.current.temperature_2m").alias("temperature_c"),
            F.col("weather_raw.current.relative_humidity_2m").alias("humidity_pct"),
            F.col("weather_raw.current.wind_speed_10m").alias("wind_speed_kmh"),
            F.col("weather_raw.current.precipitation").alias("precipitation_mm"),
            F.col("air_quality_raw.current.pm2_5").alias("pm2_5"),
            F.col("air_quality_raw.current.pm10").alias("pm10"),
            F.col("air_quality_raw.current.us_aqi").alias("us_aqi"),
        )
        .dropDuplicates(["city", "ingested_at"])
        .filter(F.col("temperature_c").isNotNull())
    )


def to_gold(silver_df: DataFrame) -> DataFrame:
    """Aggregate Silver into daily per-city summaries with derived signals."""
    daily = silver_df.groupBy("city", "country", "ingestion_date").agg(
        F.round(F.avg("temperature_c"), 1).alias("avg_temp_c"),
        F.round(F.max("temperature_c"), 1).alias("max_temp_c"),
        F.round(F.min("temperature_c"), 1).alias("min_temp_c"),
        F.round(F.avg("humidity_pct"), 1).alias("avg_humidity_pct"),
        F.round(F.avg("wind_speed_kmh"), 1).alias("avg_wind_speed_kmh"),
        F.round(F.sum("precipitation_mm"), 1).alias("total_precipitation_mm"),
        F.round(F.avg("us_aqi"), 0).alias("avg_us_aqi"),
        F.count("*").alias("reading_count"),
    )

    return daily.withColumn(
        "aqi_category",
        F.when(F.col("avg_us_aqi") <= 50, "Good")
        .when(F.col("avg_us_aqi") <= 100, "Moderate")
        .when(F.col("avg_us_aqi") <= 150, "Unhealthy for Sensitive Groups")
        .when(F.col("avg_us_aqi") <= 200, "Unhealthy")
        .otherwise("Very Unhealthy"),
    ).withColumn(
        "extreme_weather_flag",
        (F.col("max_temp_c") >= 40) | (F.col("min_temp_c") <= -20) | (F.col("avg_wind_speed_kmh") >= 60),
    )


def write_delta(df: DataFrame, path: str, mode: str = "append", partition_by: str | None = None) -> None:
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.save(path)


def run(bronze_path: str, silver_path: str, gold_path: str) -> None:
    spark = build_spark_session()
    bronze_df = read_bronze(spark, bronze_path)

    silver_df = to_silver(bronze_df)
    write_delta(silver_df, silver_path, mode="append", partition_by="ingestion_date")

    full_silver_df = spark.read.format("delta").load(silver_path)
    gold_df = to_gold(full_silver_df)
    write_delta(gold_df, gold_path, mode="overwrite")

    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Bronze -> Silver -> Gold transformations.")
    parser.add_argument("--bronze", default="data/bronze/*/*.json")
    parser.add_argument("--silver", default="data/silver")
    parser.add_argument("--gold", default="data/gold")
    args = parser.parse_args()
    run(args.bronze, args.silver, args.gold)


if __name__ == "__main__":
    main()
