# Weather & Air Quality Intelligence Pipeline

A production-style data engineering pipeline that ingests live weather and
air-quality data for cities worldwide, processes it through a Bronze →
Silver → Gold medallion architecture on **PySpark + Delta Lake**, and gates
every run behind an explicit **data quality layer** before it reaches Gold.

Built to demonstrate the same engineering patterns used with Azure Data
Factory / Databricks / Synapse, but running entirely on free, open-source
tooling so anyone can clone it and see it work — no cloud subscription
required.

```
┌───────────┐     ┌───────────┐     ┌────────────────────┐     ┌─────────┐     ┌────────┐
│ Open-Meteo│ --> │  Bronze   │ --> │  Silver             │ --> │  Gold   │ --> │  BI /  │
│  REST API │     │ (raw JSON)│     │ (typed, deduped,    │     │ (daily  │     │ CSV /  │
│           │     │           │     │  quality-gated)     │     │  agg.)  │     │ charts │
└───────────┘     └───────────┘     └────────────────────┘     └─────────┘     └────────┘
      ▲                                       │
      │                                       ▼
   Airflow (@hourly)              Data Quality Gate
                                (completeness, schema,
                                 range, freshness, volume)
```

## Why this project

Most portfolio pipelines stop at "I called an API and put it in a table."
This one is built the way a production pipeline actually needs to behave:

- **Bronze is immutable.** Raw API responses are stored untouched, so any
  downstream bug can always be replayed from source of truth.
- **Nothing reaches Gold un-vetted.** A dedicated quality gate — completeness,
  schema, value-range, freshness, and volume checks — runs against Silver
  before Gold is published, and a blocking failure stops the pipeline
  instead of silently corrupting a dashboard.
- **The pipeline is idempotent and replayable**, partitioned by ingestion
  date, so re-running a failed day doesn't duplicate data.
- **It's genuinely runnable.** A `--offline` mode with bundled fixtures means
  the whole thing works even without a live network connection, and a
  pandas-only demo script means anyone can see results in under a minute
  without installing Spark or Airflow first.

## Architecture

| Layer | What it is | Technology |
|---|---|---|
| **Bronze** | Raw, untouched API responses per city per run | Python, `requests`, JSON |
| **Silver** | Flattened, typed, deduplicated readings | PySpark, Delta Lake |
| **Quality Gate** | Completeness / schema / range / freshness / volume checks | Pure Python + pandas |
| **Gold** | Daily per-city aggregates, AQI category, extreme-weather flags | PySpark, Delta Lake |
| **Orchestration** | Hourly DAG: ingest → transform → quality gate | Apache Airflow |
| **CI** | Lint, format check, unit tests, offline demo run on every push | GitHub Actions |

## Quick start (30 seconds, no Spark/Docker needed)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-demo.txt

# Uses bundled sample fixtures -- works with no network connection
python scripts/run_local_demo.py --offline --chart
```

This runs the full Bronze → Silver → Gold → quality-gate flow with pandas,
prints a data quality report, writes `data/demo_run/gold_summary.csv`, and
(with `--chart`) saves a temperature-by-city PNG.

To hit the **live** Open-Meteo API instead of fixtures, just drop `--offline`.

## Running the full pipeline (PySpark + Airflow, via Docker)

```bash
docker compose up --build
# Airflow UI: http://localhost:8080 (user: admin, check container logs for the generated password)
# Trigger the "weather_intelligence_pipeline" DAG from the UI or:
docker exec weather_pipeline_airflow airflow dags trigger weather_intelligence_pipeline
```

## Data quality in detail

`src/quality_checks.py` is deliberately Spark-free — checks run against a
pandas DataFrame (Spark can `.toPandas()` the row counts this pipeline deals
with), which keeps the whole quality layer testable in milliseconds and
independent of a Spark session. Every check returns a `CheckResult`; the
aggregate `DataQualityReport` raises `DataQualityError` if a check marked
`blocking=True` fails, so bad data never silently reaches Gold.

```
Data Quality Report — PASS
  [PASS] completeness: no missing columns or nulls
  [PASS] schema: schema matches expectations
  [PASS] value_ranges: all values within expected ranges
  [PASS] freshness: latest record is 0:00:02 old (limit 180 min)
  [PASS] row_count: got 8 rows, expected at least 1
```

## Project structure

```
.
├── config/cities.yaml            # data-driven list of tracked cities
├── src/
│   ├── ingest.py                 # Bronze: pulls Open-Meteo weather + AQI
│   ├── transform.py              # Silver + Gold: PySpark / Delta Lake
│   └── quality_checks.py         # the quality gate (pandas, no Spark dependency)
├── dags/weather_pipeline_dag.py  # Airflow orchestration
├── scripts/run_local_demo.py     # zero-dependency pandas demo
├── tests/                        # pytest unit tests (ingest + quality checks)
├── data/sample/                  # offline fixtures for demos & CI
├── docker-compose.yml / Dockerfile
└── .github/workflows/ci.yml
```

## Engineering decisions worth asking about in an interview

- **Why pandas for quality checks instead of Great Expectations?** At this
  data volume, a 120-line pure-Python module is easier to read, test, and
  extend than pulling in a heavyweight framework — but the check interface
  (`CheckResult` / blocking vs. warning) is intentionally shaped so it could
  be swapped for GE later without touching the DAG.
- **Why does Gold fully recompute instead of incrementally merging?** Daily
  aggregate volumes here are small enough that a full recompute from Silver
  is cheaper *and* removes an entire class of drift bugs between Silver and
  Gold — a deliberate simplicity-over-cleverness trade-off.
- **Why an explicit schema instead of `inferSchema=True`?** A silent schema
  drift from the source API should fail loudly at read time, not corrupt
  data quietly three layers downstream.

## Future enhancements

- Promote Gold only after the quality gate passes (currently both run per
  DAG cycle; a stricter version would gate the write itself).
- Add a `great_expectations` adapter behind the existing `CheckResult`
  interface for teams that want a full DQ framework.
- Publish Gold to a small Streamlit/Power BI dashboard for a live public demo.
- Add CDC-style incremental ingestion once a source supports it (Open-Meteo
  is snapshot-only, so this pipeline currently polls on a schedule).

## License

MIT — see [LICENSE](LICENSE).
