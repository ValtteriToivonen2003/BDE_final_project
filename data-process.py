"""
Spark Data Processing Pipeline — NYC TLC Trip Data

Stages:
  RAW    : Original Parquet files as-is from MinIO
  SILVER : Cleaned, typed, and deduplicated records
  GOLD   : Feature-engineered, aggregated analytical tables
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType
from pyspark.sql.window import Window


# Configuration

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

BUCKET           = "nyc-tlc"
RAW_PREFIX       = "raw"
SILVER_PREFIX    = "silver"
GOLD_PREFIX      = "gold"

S3A = f"s3a://{BUCKET}"


# Spark session

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("NYC-TLC-Medallion-Pipeline")
        .config("spark.driver.memory", "8g") 
        .config("spark.executor.memory", "8g")
        .master("local[*]")
        # S3A / MinIO 
        .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.timeout",            "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout",  "60000")
        .config("spark.hadoop.fs.s3a.socket.timeout",                "60000")
        .config("spark.hadoop.fs.s3a.input.fadvise",                 "sequential")
        # Packages 
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.4.1,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.367")
        # Performance / memory 
        .config("spark.sql.shuffle.partitions",        "16")
        .config("spark.sql.adaptive.enabled",          "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Give execution memory more room; storage fraction is cut so
        # cache doesn't crowd out sort/shuffle buffer on a single node
        .config("spark.memory.fraction",               "0.8")
        .config("spark.memory.storageFraction",        "0.2")
        # Spill to disk instead of OOM-ing during sort/dedup
        .config("spark.shuffle.spill.compress",        "true")
        .config("spark.shuffle.file.buffer",           "1m")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .getOrCreate()
    )


# Layer 1: raw

def read_raw(spark: SparkSession) -> DataFrame:
    """
    Read ALL Parquet files under the raw/ prefix (glob).
    Spark merges schemas automatically so multiple monthly files
    can differ slightly in nullable flags.
    """
    path = f"{S3A}/{RAW_PREFIX}/*.parquet"
    print(f"\n[RAW] Reading from  {path}")

    df = (
        spark.read
        .option("mergeSchema", "true")
        .parquet(path)
    )

    print(f"      Partitions : {df.rdd.getNumPartitions()}")
    print(f"      Columns    : {len(df.columns)}")
    df.printSchema()
    return df


# Layer 2: silver

# Columns that must be non-null for a row to be useful
_REQUIRED_COLS = [
    "pickup_datetime",
    "dropoff_datetime",
    "trip_miles",
    "base_passenger_fare",
]

# Hard business-rule bounds
_BOUNDS = {
    "trip_miles":          (0.1,  500.0),   # miles
    "base_passenger_fare": (1.0,  5000.0),  # USD
    "trip_time":           (60,   18_000),  # seconds  (1 min – 5 hrs)
    "tolls":               (0.0,  200.0),   # USD
    "tips":                (0.0,  500.0),   # USD
}

# Canonical column names we produce from the raw schema
_RENAME = {
    # raw name               silver name
    "hvfhs_license_num":    "license_num",
    "dispatching_base_num": "dispatch_base",
    "originating_base_num": "origin_base",
    "on_scene_datetime":    "on_scene_at",
    "pickup_datetime":      "pickup_at",
    "dropoff_datetime":     "dropoff_at",
    "PULocationID":         "pu_location_id",
    "DOLocationID":         "do_location_id",
    "trip_time":            "trip_time_secs",
    "base_passenger_fare":  "fare_base",
    "tolls":                "fare_tolls",
    "bcf":                  "fare_bcf",
    "sales_tax":            "fare_tax",
    "congestion_surcharge": "fare_congestion",
    "airport_fee":          "fare_airport",
    "tips":                 "tip_amount",
    "shared_request_flag":  "shared_requested",
    "shared_match_flag":    "shared_matched",
    "access_a_ride_flag":   "access_a_ride",
    "wav_request_flag":     "wav_requested",
    "wav_match_flag":       "wav_matched",
}


def _cast_timestamps(df: DataFrame) -> DataFrame:
    for col in ("pickup_datetime", "dropoff_datetime", "on_scene_datetime",
                "request_datetime"):
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(TimestampType()))
    return df


def _filter_bounds(df: DataFrame) -> DataFrame:
    for col_name, (lo, hi) in _BOUNDS.items():
        if col_name in df.columns:
            df = df.filter(F.col(col_name).between(lo, hi))
    return df


def build_silver(raw: DataFrame) -> DataFrame:
    print("\n[Silver] Cleaning …")

    # 1. Drop nulls in required columns
    present_required = [c for c in _REQUIRED_COLS if c in raw.columns]
    df = raw.dropna(subset=present_required)
    print(f"         After null-drop   : {df.count():>12,} rows")

    # 2. Timestamps
    df = _cast_timestamps(df)

    # 3. Bounds
    df = _filter_bounds(df)
    print(f"         After bounds-filter: {df.count():>12,} rows")

    # 4. Deduplicate
    dedup_keys = [c for c in
                  ["pickup_datetime", "dropoff_datetime",
                   "PULocationID", "DOLocationID"]
                  if c in df.columns]
    df = df.dropDuplicates(dedup_keys)
    print(f"         After dedup        : {df.count():>12,} rows")

    # 5. Rename to silver schema
    for raw_col, silver_col in _RENAME.items():
        if raw_col in df.columns:
            df = df.withColumnRenamed(raw_col, silver_col)

    # 6. Partition columns 
    pickup_col = "pickup_at" if "pickup_at" in df.columns else "pickup_datetime"
    df = (
        df
        .withColumn("trip_year",  F.year(pickup_col))
        .withColumn("trip_month", F.month(pickup_col))
    )

    # 7. Metadata
    df = df.withColumn("_loaded_at", F.current_timestamp())

    return df


# Layer 3: gold

def build_gold_trips(silver: DataFrame) -> DataFrame:
    """
    Feature-engineered trip-level table.

    New columns
    -----------
    total_fare          – sum of all fare components
    speed_mph           – average speed in miles per hour
    fare_per_mile       – base fare divided by trip miles
    tip_pct             – tip as a percentage of base fare
    time_of_day         – Morning / Afternoon / Evening / Night
    day_of_week         – Monday … Sunday
    is_weekend          – boolean
    is_airport_trip     – boolean (any airport surcharge > 0)
    is_shared           – boolean (requested and matched shared ride)
    duration_bin        – Short (<10 min) / Medium / Long / Extra-long
    """
    print("\n[Gold trips] Feature engineering …")

    # Derived fare
    fare_cols = ["fare_base", "fare_tolls", "fare_bcf",
                 "fare_tax", "fare_congestion", "fare_airport", "tip_amount"]
    present_fares = [c for c in fare_cols if c in silver.columns]
    total_fare_expr = sum(F.coalesce(F.col(c), F.lit(0.0)) for c in present_fares)

    df = silver.withColumn("total_fare", total_fare_expr)

    # Speed & efficiency 
    df = (
        df
        .withColumn("speed_mph",
                    F.when(F.col("trip_time_secs") > 0,
                           F.col("trip_miles") / (F.col("trip_time_secs") / 3600.0)
                    ).otherwise(None))
        .withColumn("fare_per_mile",
                    F.when(F.col("trip_miles") > 0,
                           F.col("fare_base") / F.col("trip_miles")
                    ).otherwise(None))
        .withColumn("tip_pct",
                    F.when(F.col("fare_base") > 0,
                           (F.col("tip_amount") / F.col("fare_base")) * 100
                    ).otherwise(0.0))
    )

    # Filter out physically impossible speeds (> 120 mph = data error)
    df = df.filter(F.col("speed_mph").isNull() | (F.col("speed_mph") <= 120))

    # Temporal features
    df = (
        df
        .withColumn("pickup_hour",   F.hour("pickup_at"))
        .withColumn("day_of_week",   F.date_format("pickup_at", "EEEE"))
        .withColumn("is_weekend",
                    F.dayofweek("pickup_at").isin([1, 7]))   # Sun=1, Sat=7
        .withColumn("trip_date",     F.to_date("pickup_at"))
        .withColumn("time_of_day",
                    F.when(F.col("pickup_hour").between(5, 11),  "Morning")
                     .when(F.col("pickup_hour").between(12, 16), "Afternoon")
                     .when(F.col("pickup_hour").between(17, 21), "Evening")
                     .otherwise("Night"))
    )

    # Categorical features
    df = df.withColumn(
        "is_airport_trip",
        F.coalesce(F.col("fare_airport"), F.lit(0.0)) > 0
    )
    if "shared_requested" in df.columns:
        df = df.withColumn(
            "is_shared",
            (F.col("shared_requested") == "Y") & (F.col("shared_matched") == "Y")
        )
    else:
        df = df.withColumn("is_shared", F.lit(False))

    # Duration bins
    df = df.withColumn(
        "duration_bin",
        F.when(F.col("trip_time_secs") <  600, "Short (<10 min)")
         .when(F.col("trip_time_secs") < 1800, "Medium (10–30 min)")
         .when(F.col("trip_time_secs") < 3600, "Long (30–60 min)")
         .otherwise("Extra-long (>1 hr)")
    )

    return df


def build_gold_hourly_agg(trips: DataFrame) -> DataFrame:
    """
    Hourly aggregation — one row per (trip_date, pickup_hour, pu_location_id).
    Metrics: trip_count, avg_fare, avg_speed_mph, avg_tip_pct, total_revenue.
    """
    print("[GOLD – hourly_agg] Aggregating …")

    group_keys = ["trip_date", "trip_year", "trip_month",
                  "pickup_hour", "time_of_day", "is_weekend"]
    if "pu_location_id" in trips.columns:
        group_keys.append("pu_location_id")

    agg = (
        trips
        .groupBy(*group_keys)
        .agg(
            F.count("*")                       .alias("trip_count"),
            F.sum("total_fare")                .alias("total_revenue"),
            F.avg("total_fare")                .alias("avg_fare"),
            F.avg("fare_base")                 .alias("avg_base_fare"),
            F.avg("tip_amount")                .alias("avg_tip"),
            F.avg("tip_pct")                   .alias("avg_tip_pct"),
            F.avg("trip_miles")                .alias("avg_trip_miles"),
            F.avg("trip_time_secs")            .alias("avg_trip_time_secs"),
            F.avg("speed_mph")                 .alias("avg_speed_mph"),
            F.sum(F.col("is_airport_trip").cast(IntegerType()))
                                               .alias("airport_trip_count"),
            F.sum(F.col("is_shared").cast(IntegerType()))
                                               .alias("shared_trip_count"),
        )
        .withColumn("revenue_per_trip", F.col("total_revenue") / F.col("trip_count"))
        .withColumn("_loaded_at", F.current_timestamp())
    )

    return agg


def build_gold_driver_agg(trips: DataFrame) -> DataFrame:
    """
    Per-driver daily summary — only when driver_pay column is present.
    Metrics: trips, total earnings, avg per-trip pay, avg speed, tip rate.
    """
    if "driver_pay" not in trips.columns or "license_num" not in trips.columns:
        print("[GOLD – driver_agg] Skipped (columns unavailable).")
        return None

    print("[GOLD – driver_agg] Aggregating …")

    return (
        trips
        .groupBy("license_num", "dispatch_base", "trip_date",
                 "trip_year", "trip_month")
        .agg(
            F.count("*")                  .alias("trip_count"),
            F.sum("driver_pay")           .alias("total_driver_pay"),
            F.avg("driver_pay")           .alias("avg_driver_pay_per_trip"),
            F.sum("trip_miles")           .alias("total_miles"),
            F.avg("speed_mph")            .alias("avg_speed_mph"),
            F.avg("tip_pct")              .alias("avg_tip_pct"),
            F.sum(F.col("is_airport_trip").cast(IntegerType()))
                                          .alias("airport_trips"),
        )
        .withColumn("pay_per_mile",
                    F.col("total_driver_pay") / F.col("total_miles"))
        .withColumn("_loaded_at", F.current_timestamp())
    )


# Write helpers

def write_layer(df: DataFrame, layer: str, table: str,
                partition_by: list[str] | None = None) -> None:
    """
    Write a DataFrame to MinIO as partitioned Parquet.
    Overwrites the table path so the pipeline is idempotent.
    """
    path = f"{S3A}/{layer}/{table}"
    print(f"    Writing  →  {path}")

    writer = (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.parquet(path)
    print(f"    ✓ {table} written.\n")


# Entry point

def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Raw
    raw_df = read_raw(spark)

    # Silver
    silver_df = build_silver(raw_df)
    # Repartition to 16 balanced tasks; avoids giant per-task sort buffers
    silver_df = silver_df.repartition(16)
    write_layer(silver_df, SILVER_PREFIX, "trips",
                partition_by=["trip_year", "trip_month"])

    # Drop the lazy plan — read back from S3 so the JVM heap is clean
    # before we start the heavier gold feature-engineering pass
    silver_df = spark.read.parquet(f"{S3A}/{SILVER_PREFIX}/trips")
    print(f"    Silver re-read: {silver_df.count():,} rows")

    # Gold trip-level features
    gold_trips = build_gold_trips(silver_df)
    gold_trips = gold_trips.repartition(16)
    write_layer(gold_trips, GOLD_PREFIX, "trips_featured",
                partition_by=["trip_year", "trip_month"])

    # Read back again for aggregations (cheap columnar scans, low memory)
    gold_trips = spark.read.parquet(f"{S3A}/{GOLD_PREFIX}/trips_featured")

    # Gold hourly-aggregation
    gold_hourly = build_gold_hourly_agg(gold_trips)
    write_layer(gold_hourly, GOLD_PREFIX, "hourly_agg",
                partition_by=["trip_year", "trip_month"])

    # Gold driver-aggregation
    gold_drivers = build_gold_driver_agg(gold_trips)
    if gold_drivers is not None:
        write_layer(gold_drivers, GOLD_PREFIX, "driver_daily_agg",
                    partition_by=["trip_year", "trip_month"])

    # Sanity-check read-back
    print("\n[VERIFY] Spot-checking gold/hourly_agg …")
    check = spark.read.parquet(f"{S3A}/{GOLD_PREFIX}/hourly_agg")
    print(f"         Rows    : {check.count():,}")
    print(f"         Columns : {check.columns}")
    check.orderBy(F.desc("total_revenue")).show(5, truncate=False)

    spark.stop()

    print("\n" + "═" * 60)
    print("  Medallion pipeline completed:  raw → silver → gold")
    print("═" * 60)


if __name__ == "__main__":
    main()