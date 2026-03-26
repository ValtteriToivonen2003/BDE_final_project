# Big Data Engineering Final Project - DEAI 2026

This repository contains a complete NYC taxi data ingestion pipeline:
- Download Yellow Taxi Parquet data
- Upload to MinIO S3
- Read from MinIO using Spark with S3A

## Files
- `data-ingestion.py`: end-to-end ingestion script (download, upload, Spark read)
- `data-process.py`: Spark medallion pipeline (RAW → SILVER → GOLD data transformation)
- `main.py`: FastAPI Analytics API server with analytical endpoints
- `dockerfile.api`: Docker image for running FastAPI server
- `docker-compose.yml`: local stack with MinIO, Spark master/worker, and FastAPI API service
- `requirements.txt`: Python dependencies

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

## What it does
1. Downloads `yellow_tripdata_2024-01.parquet` to `/tmp/yellow_tripdata_2024-01.parquet`.
2. Uploads to MinIO bucket `nyc-tlc` at key `raw/yellow_tripdata.parquet`.
3. Starts a local Spark session and reads from `s3a://nyc-tlc/raw/yellow_tripdata.parquet`.
4. Prints schema, row count, and sample rows.

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
