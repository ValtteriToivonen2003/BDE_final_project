"""
Data Ingestion Pipeline - NYC TLC Trip Data
============================================
Goal: Download raw dataset, convert to Parquet, upload to MinIO S3, and read with Spark.
"""

import os
import requests
import pandas as pd
from io import BytesIO
import boto3
from botocore.client import Config
from pyspark.sql import SparkSession


# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

# MinIO connection settings (adjust to your environment)
MINIO_ENDPOINT    = "http://localhost:9000"          # MinIO server URL
MINIO_ACCESS_KEY  = "minioadmin"                     # MinIO access key
MINIO_SECRET_KEY  = "minioadmin"                     # MinIO secret key
BUCKET_NAME       = "nyc-tlc"                        # Target S3 bucket
PARQUET_KEY       = "raw/yellow_tripdata.parquet"    # Object key inside bucket

# NYC TLC dataset URL (Yellow Taxi - January 2024)
DATA_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_2024-01.parquet"
)

LOCAL_PARQUET_PATH = "/tmp/yellow_tripdata_2024-01.parquet"


# ─────────────────────────────────────────────
# STEP 1 – Download & store as Parquet
# ─────────────────────────────────────────────

def download_and_save_parquet(url: str, local_path: str) -> None:
    """
    Download the NYC TLC dataset. If it is already in Parquet format (as
    provided by the TLC CDN) it is saved directly; otherwise it is converted.
    """
    print(f"[1/3] Downloading dataset from:\n      {url}")
    response = requests.get(url, stream=True, timeout=120)
    response.raise_for_status()

    # The TLC CDN now serves native Parquet files – write directly to disk.
    with open(local_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
            f.write(chunk)

    size_mb = os.path.getsize(local_path) / (1024 ** 2)
    print(f"    ✓ Saved to {local_path}  ({size_mb:.1f} MB)")

    # Quick sanity-check: load with pandas and print schema
    df = pd.read_parquet(local_path)
    print(f"    ✓ Rows: {len(df):,}  |  Columns: {list(df.columns)}\n")


# ─────────────────────────────────────────────
# STEP 2 – Upload Parquet to MinIO (S3)
# ─────────────────────────────────────────────

def upload_to_minio(local_path: str, bucket: str, key: str) -> None:
    """Create the target bucket (if absent) and upload the Parquet file."""
    print(f"[2/3] Connecting to MinIO at {MINIO_ENDPOINT} …")

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    # Create bucket if it doesn't exist
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)
        print(f"    ✓ Bucket '{bucket}' created.")
    else:
        print(f"    ✓ Bucket '{bucket}' already exists.")

    # Upload
    print(f"    Uploading  {local_path}  →  s3://{bucket}/{key}")
    s3.upload_file(local_path, bucket, key)
    print(f"    ✓ Upload complete.\n")


# ─────────────────────────────────────────────
# STEP 3 – Read from MinIO with Apache Spark
# ─────────────────────────────────────────────

def read_with_spark(bucket: str, key: str) -> None:
    """
    Initialise a local Spark session configured with the S3A connector
    pointing at MinIO, then read the Parquet file and print basic stats.
    """
    print("[3/3] Starting Spark session …")

    spark = (
        SparkSession.builder
        .appName("NYC-TLC-Ingestion")
        .master("local[*]")
        # ── S3A / MinIO settings ──────────────────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # ── Required JARs (adjust versions to match your Hadoop/Spark build) ─
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    s3_path = f"s3a://{bucket}/{key}"
    print(f"    Reading from  {s3_path}")

    df = spark.read.parquet(s3_path)

    print(f"    ✓ Schema:")
    df.printSchema()
    print(f"    ✓ Total rows : {df.count():,}")
    print(f"    ✓ Sample rows:")
    df.show(5, truncate=False)

    spark.stop()
    print("    ✓ Spark session closed.\n")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    download_and_save_parquet(DATA_URL, LOCAL_PARQUET_PATH)
    upload_to_minio(LOCAL_PARQUET_PATH, BUCKET_NAME, PARQUET_KEY)
    read_with_spark(BUCKET_NAME, PARQUET_KEY)
    print("═" * 55)
    print("  Data ingestion pipeline completed successfully.")
    print("═" * 55)