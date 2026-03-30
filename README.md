# Big Data Engineering Final Project - DEAI 2026

End-to-end big data pipeline built on NYC TLC High Volume For-Hire Vehicle (FHVHV) trip records.  
The project ingests raw Parquet data, processes it through a Spark medallion architecture, stores  
every layer in a MinIO S3-compatible data lake, and surfaces analytical results through a FastAPI REST API.

---

## Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         Data Sources                                         │
│         NYC TLC FHVHV Parquet Feed  (cloudfront CDN)                        │
└───────────────────────────┬──────────────────────────────────────────────────┘
                            │  HTTP download
                            ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                     Ingestion Layer  (data-ingestion.py)                     │
│   • Downloads monthly Parquet file to /tmp/                                  │
│   • Uploads raw file to MinIO  →  s3a://nyc-tlc/raw/                        │
│   • Validates schema via a local PySpark read                                │
└───────────────────────────┬──────────────────────────────────────────────────┘
                            │  S3A write
                            ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                     Data Lake  –  MinIO  (S3-compatible)                     │
│                                                                              │
│   Bucket: nyc-tlc                                                            │
│   ├── raw/              ← original Parquet files as-is                       │
│   ├── silver/trips/     ← cleaned, typed, deduplicated records               │
│   └── gold/                                                                  │
│       ├── trips_featured/   ← feature-engineered trip-level table            │
│       ├── hourly_agg/       ← hourly aggregations by zone & time             │
│       └── driver_daily_agg/ ← per-driver daily earnings summary             │
└──────────┬─────────────────────────────────┬────────────────────────────────┘
           │  S3A read / write               │  S3A read (boto3)
           ▼                                 ▼
┌──────────────────────┐         ┌───────────────────────────────────────────┐
│  Processing Layer    │         │           Query / API Layer               │
│  (data-process.py)   │         │                                           │
│                      │         │  DuckDB  (in-process, via pandas)         │
│  Apache Spark        │         │  ┌─────────────────────────────────────┐  │
│  Medallion Pipeline  │         │  │ reads gold/ Parquet files from MinIO│  │
│                      │         │  │ runs analytical SQL aggregations     │  │
│  RAW → SILVER → GOLD │         │  └──────────────┬──────────────────────┘  │
│                      │         │                 │                          │
│  • null removal       │         │                 ▼                          │
│  • bounds filtering  │         │  FastAPI  (main.py)                        │
│  • deduplication     │         │  ┌─────────────────────────────────────┐  │
│  • feature eng.      │         │  │ GET /api/v1/trips/hourly            │  │
│  • aggregation       │         │  │ GET /api/v1/trips/summary           │  │
└──────────────────────┘         │  │ GET /api/v1/drivers/leaderboard     │  │
                                 │  │ GET /health                         │  │
                                 │  └─────────────────────────────────────┘  │
                                 └───────────────────────────────────────────┘
                                                   │
                                                   ▼
                                 ┌─────────────────────────────────────────┐
                                 │   Analytical Questions  (analytical-     │
                                 │   questions.py)                          │
                                 │   Q1 · Time-of-day demand & revenue      │
                                 │   Q2 · Weekday vs weekend comparison     │
                                 │   Q3 · Driver earnings & airport trips   │
                                 └─────────────────────────────────────────┘
```

### Technology stack

| Layer | Technology |
|---|---|
| Ingestion | Python (`requests`, `boto3`), PySpark |
| Processing | Apache Spark (PySpark) — local mode |
| Data Lake | MinIO (S3-compatible object storage) |
| Query Engine | DuckDB / pandas (reads gold Parquet in-process) |
| API | FastAPI + Uvicorn |
| Containerisation | Docker Compose |

---

## Pipeline Explanation

The pipeline follows the **medallion architecture** (RAW → SILVER → GOLD), a standard data lakehouse pattern where data quality and richness increase at each layer.

### Stage 1 — Data Ingestion (`data-ingestion.py`)

1. **Download**: Pulls the monthly FHVHV Parquet file from the NYC TLC CDN (e.g. `fhvhv_tripdata_2025-11.parquet`) to a local `/tmp/` path.
2. **Upload to MinIO**: Uses `boto3` to PUT the file into the `nyc-tlc` bucket under the `raw/` prefix via the S3-compatible MinIO endpoint.
3. **Validate**: Opens a local PySpark session with the S3A connector and reads the raw file back, printing schema, row count, and sample rows to confirm a clean upload.

### Stage 2 — Data Processing (`data-process.py`)

**RAW layer** — reads all Parquet files from `s3a://nyc-tlc/raw/` with schema merging enabled so monthly files with minor schema differences are handled automatically.

**SILVER layer** — quality-focused cleaning:
- Drop rows missing critical fields (`pickup_datetime`, `dropoff_datetime`, `trip_miles`, `base_passenger_fare`).
- Cast timestamp columns to the correct type.
- Apply hard business-rule bounds (e.g. trip distance 0.1–500 mi, fare $1–$5000, duration 1–300 min).
- Deduplicate on `(pickup_datetime, dropoff_datetime, PULocationID, DOLocationID)`.
- Rename raw column names to a canonical silver schema.
- Persist to `s3a://nyc-tlc/silver/trips/` partitioned by `(trip_year, trip_month)`.

**GOLD layer** — analytics-ready tables:

| Table | Description |
|---|---|
| `gold/trips_featured` | Trip-level table with derived features: `total_fare`, `speed_mph`, `fare_per_mile`, `tip_pct`, `time_of_day`, `day_of_week`, `is_weekend`, `is_airport_trip`, `duration_bin` |
| `gold/hourly_agg` | Aggregated by `(trip_date, pickup_hour, time_of_day, is_weekend, pu_location_id)` — trip counts, revenue, avg fare, avg speed, airport & shared-ride counts |
| `gold/driver_daily_agg` | Aggregated by `(license_num, dispatch_base, trip_date)` — earnings, miles, avg speed, tip rate, airport trips |

### Stage 3 — Query Layer (DuckDB / pandas)

The FastAPI server uses `boto3` to download gold-layer Parquet files into memory and processes them with **pandas** (acting as a DuckDB-equivalent in-process query engine). This avoids the overhead of a running Spark cluster at query time while still querying the same gold Parquet tables.

### Stage 4 — API Layer (`main.py`)

FastAPI exposes three analytical endpoints backed by the gold data:

| Endpoint | Gold table | Description |
|---|---|---|
| `GET /api/v1/trips/hourly` | `hourly_agg` | Filterable hourly trip aggregations (year, month, hour, time-of-day, weekend, zone) |
| `GET /api/v1/trips/summary` | `hourly_agg` | Single-object dataset summary: totals, averages, busiest hour, airport & shared-ride shares |
| `GET /api/v1/drivers/leaderboard` | `driver_daily_agg` | Driver earnings sorted by total pay; filterable by year, month, license |

### Stage 5 — Data Analysis (`analytical-questions.py`)

Answers three analytical questions by calling the API endpoints and cross-referencing the gold-layer data:

- **Q1** — Which time-of-day period has the highest trip demand and revenue?
- **Q2** — Do weekend trips differ from weekday trips in fare, tip rate, speed, and volume?
- **Q3** — Who are the top-earning drivers, and how do airport trips contribute to their efficiency?

---

## Files

| File | Purpose |
|---|---|
| `data-ingestion.py` | Download raw Parquet → upload to MinIO → validate via Spark |
| `data-process.py` | Spark medallion pipeline (RAW → SILVER → GOLD) |
| `main.py` | FastAPI analytics API server |
| `analytical-questions.py` | Three analytical questions answered via the API |
| `dockerfile.api` | Docker image for the FastAPI service |
| `docker-compose.yml` | Local stack: MinIO + Spark master/worker + FastAPI |
| `requirements.txt` | Python dependencies |

---

## Quick Start

1. Install dependencies:
```bash
pip install -r requirements.txt
```
2. Start Docker stack:
```bash
docker compose up -d
```
3. Run ingestion script:
```bash
python3 data-ingestion.py
```
4. Run data processing pipeline:
```bash
python3 data-process.py
```
5. Run FastAPI Analytics API:
```bash
python3 -m uvicorn main:app --reload --host 0.0.0.0 --port 8000
```
6. Run analytical questions:
```bash
python3 analytical-questions.py
```

The Docker stack creates the `nyc-tlc` bucket. The `raw`, `silver`, and `gold` paths appear only after the ingestion and processing scripts upload objects into those prefixes.

---

## What it does

1. Downloads an FHVHV monthly file such as `fhvhv_tripdata_2025-11.parquet` to `/tmp/fhvhv_tripdata_2025-11.parquet`.
2. Uploads it to MinIO bucket `nyc-tlc` at key `raw/fhvhv_tripdata_2025-11.parquet`.
3. Starts a local Spark session and reads from `s3a://nyc-tlc/raw/fhvhv_tripdata_2025-11.parquet`.
4. Prints schema, row count, and sample rows.

## Changing the source month
The ingestion script defaults to the NYC TLC High Volume For-Hire Vehicle Trip Records Parquet feed. To switch months, set `DATASET_MONTH` when you run it:

```bash
DATASET_MONTH=2024-01 python3 data-ingestion.py
```

Optional overrides:
- `DATASET_PREFIX` defaults to `fhvhv_tripdata`
- `DATASET_BASE_URL` defaults to `https://d37ci6vzurychx.cloudfront.net/trip-data`
- `PARQUET_KEY` defaults to `raw/<dataset filename>`
- `LOCAL_PARQUET_PATH` defaults to `/tmp/<dataset filename>`

## MinIO access
- UI: http://localhost:9001
- Console credentials: `minioadmin` / `minioadmin`
- S3 endpoint: http://localhost:9000

## Running from Spark image (optional)
If you want to run Spark using the Docker Spark image instead of local Spark from Python environment, use Spark submit with dependencies for S3A:
```bash
/opt/spark/bin/spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  data-ingestion.py
```

## Validate output
After ingestion completes, confirm file exists in MinIO:
```bash
docker compose exec minio-init sh -c 'mc alias set local http://minio:9000 minioadmin minioadmin && mc ls local/nyc-tlc/raw'
```

## MinIO persistence
MinIO data is now stored in a named Docker volume, so `docker compose down` followed by `docker compose up -d` keeps your bucket contents.

If you run:
```bash
docker compose down -v
```
Docker removes the MinIO volume and all stored objects, so you must rerun ingestion and processing.

## Analytics API Endpoints
Available at `http://localhost:8000` when running:
- `GET /api/v1/trips/hourly`: Hourly trip aggregations
- `GET /api/v1/trips/summary`: Trip summary statistics
- `GET /api/v1/drivers/leaderboard`: Top driver statistics
- `GET /health`: Health check endpoint

API documentation (Swagger): `http://localhost:8000/docs`

## Data Processing Pipeline
The `data-process.py` implements a medallion architecture:
- **RAW**: Original Parquet files from MinIO
- **SILVER**: Cleaned, typed, and deduplicated records
- **GOLD**: Feature-engineered, aggregated analytical tables

## Notes
- The scripts use `pandas` and `pyspark`, so ensure your Python environment has dependencies installed.
- Docker Compose brings up required services; keep them running while ingesting and processing.
- The FastAPI server reads from processed GOLD-layer Parquet data in MinIO.

---

## Grading Overview

| Area | Points |
|---|---|
| Data Ingestion | 20 |
| Data Processing with Spark | 20 |
| Data Lake Structure | 15 |
| API Layer | 20 |
| Data Analysis | 15 |
| Documentation & Architecture | 10 |
| **Total** | **100** |
