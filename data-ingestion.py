"""
Data Ingestion Pipeline - NYC TLC Trip Data
============================================
Goal: Download raw dataset, convert to Parquet, upload to MinIO S3, and read with Spark.
"""

import os
import time
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

# NYC TLC dataset URL (high volume taxi data - November 2025)
DATA_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2025-11.parquet"
)

LOCAL_PARQUET_PATH = "/tmp/fhvhv_tripdata_2025-11.parquet"


# ─────────────────────────────────────────────
# STEP 1 – Download & store as Parquet
# ─────────────────────────────────────────────

def download_and_save_parquet(url: str, local_path: str, retries: int = 3) -> None:
    """
    Download the dataset with a retry loop and chunked writing to prevent corruption.
    """
    print(f"[1/3] Downloading dataset from:\n      {url}")
    
    for attempt in range(retries):
        try:
            # Added a longer timeout and stream=True for large files
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()

            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0

            with open(local_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024): # 1MB chunks
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        # Print progress every 50MB so you know it hasn't frozen
                        if downloaded % (50 * 1024 * 1024) < (1024 * 1024):
                            print(f"    Progress: {downloaded / (1024**2):.1f} MB / {total_size / (1024**2):.1f} MB")

            size_mb = os.path.getsize(local_path) / (1024 ** 2)
            print(f"    ✓ Saved to {local_path} ({size_mb:.1f} MB)")
            
            # The Critical Sanity Check
            df = pd.read_parquet(local_path)
            print(f"    ✓ Data Verified. Rows: {len(df):,}\n")
            return # Exit function if successful

        except Exception as e:
            print(f"    ⚠ Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                print("    Retrying in 5 seconds...")
                time.sleep(5)
            else:
                print("    Error: All download attempts failed.")
                raise


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
.config("spark.hadoop.fs.s3a.connection.timeout",           "60000")
.config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
.config("spark.hadoop.fs.s3a.socket.timeout",               "60000")
.config("spark.hadoop.fs.s3a.input.fadvise",               "sequential")
.config("spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.4.1,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.367")
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