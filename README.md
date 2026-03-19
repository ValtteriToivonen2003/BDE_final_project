# Big Data Engineering Final Project - DEAI 2026

This project implements data ingestion of NYC taxi trip data into MinIO and a Spark read-from-S3 ingestion pipeline.

## Goals implemented

1. Download raw dataset in Parquet format
2. Upload raw parquet data to MinIO bucket `taxi/raw`
3. Read raw data from S3 using Spark (`s3a://`) and write processed parquet to `taxi/processed`

## Setup

1. Start containers:

```bash
docker compose up -d
```

2. Install Python (from host) dependencies:

```bash
python3 -m pip install -r ingest/requirements.txt
```

## Run raw ingestion (download + upload)

```bash
python3 ingest/download_and_upload.py \
  --url https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet \
  --local-dir ./data/raw \
  --bucket taxi \
  --key raw/yellow_tripdata.parquet \
  --endpoint http://localhost:9000 \
  --access-key minioadmin \
  --secret-key minioadmin
```

Confirm object exists by listing with MinIO mc (optional):

```bash
docker compose exec minio-init sh -c 'mc alias set local http://minio:9000 minioadmin minioadmin && mc ls local/taxi/raw'
```

## Run Spark ingestion reading from S3

Spark reads directly from MinIO via s3a:

```bash
python3 ingest/spark_read_s3.py
```

If you want to run with Spark submit and explicit packages:

```bash
/opt/spark/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.5 ingest/spark_read_s3.py
```

## Verify

After Spark job, verify processed parquet in MinIO:

```bash
docker compose exec minio-init sh -c 'mc alias set local http://minio:9000 minioadmin minioadmin && mc ls local/taxi/processed'
```

## Notes
- This pipeline stores the raw dataset in MinIO as parquet and reads it directly via Spark S3A.
- If your MinIO is running on another host (docker or remote), update endpoint and credentials.
