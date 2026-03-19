# Big Data Engineering Final Project - DEAI 2026

This repository contains a complete NYC taxi data ingestion pipeline:
- Download Yellow Taxi Parquet data
- Upload to MinIO S3
- Read from MinIO using Spark with S3A

## Files
- `data-ingestion.py`: end-to-end ingestion script (download, upload, Spark read)
- `docker-compose.yml`: local stack with MinIO, Spark master/worker, and FastAPI API service

## Quick Start
1. Start Docker stack:
```bash
docker compose up -d
```
2. Run ingestion script:
```bash
python3 data-ingestion.py
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

## Notes
- The script uses `pandas` and `pyspark`, so ensure your Python environment has dependencies installed.
- Docker Compose brings up required services; keep them running while ingesting.
