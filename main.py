"""
NYC TLC Analytics API
FastAPI server exposing three analytical endpoints backed by Spark gold-layer
Parquet data stored in MinIO.

Endpoints:
  GET /api/v1/trips/hourly          
  GET /api/v1/trips/summary         
  GET /api/v1/drivers/leaderboard   
  GET /health                       
"""

from __future__ import annotations

import os
from datetime import date
from functools import lru_cache
from typing import Optional

import boto3
import pandas as pd
from botocore.client import Config
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


# SETTINGS

class Settings(BaseSettings):
    minio_endpoint: str   = "http://localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    bucket: str           = "nyc-tlc"

    class Config:
        env_prefix = ""       


@lru_cache
def get_settings() -> Settings:
    return Settings()


# DATA LAYER 

def _s3_client():
    s = get_settings()
    return boto3.client(
        "s3",
        endpoint_url=s.minio_endpoint,
        aws_access_key_id=s.minio_access_key,
        aws_secret_access_key=s.minio_secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _list_parquet_keys(prefix: str) -> list[str]:
    """Return every object key under *prefix* that ends in .parquet."""
    s3  = _s3_client()
    cfg = get_settings()
    paginator = s3.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=cfg.bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    return keys


def _read_parquet_from_minio(prefix: str) -> pd.DataFrame:
    """
    Download all Parquet files under *prefix* into a single DataFrame.
    Raises HTTPException 503 if MinIO is unreachable, 404 if no data found.
    """
    s3  = _s3_client()
    cfg = get_settings()

    try:
        keys = _list_parquet_keys(prefix)
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Cannot reach MinIO at {cfg.minio_endpoint}: {exc}",
        )

    if not keys:
        raise HTTPException(
            status_code=404,
            detail=f"No Parquet files found at s3://{cfg.bucket}/{prefix}",
        )

    frames: list[pd.DataFrame] = []
    for key in keys:
        import io
        buf = io.BytesIO()
        s3.download_fileobj(cfg.bucket, key, buf)
        buf.seek(0)
        frames.append(pd.read_parquet(buf))

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ─────────────────────────────────────────────
# APP
# ─────────────────────────────────────────────

app = FastAPI(
    title="NYC TLC Analytics API",
    description=(
        "REST API for the NYC For-Hire Vehicle (FHVHV) trip dataset.\n\n"
        "Data is sourced from Spark gold-layer Parquet tables stored in MinIO:\n"
        "- `gold/hourly_agg`        — hourly trip metrics\n"
        "- `gold/trips_featured`    — individual enriched trips\n"
        "- `gold/driver_daily_agg`  — per-driver daily summaries\n\n"
        "All monetary values are in **USD**. Distances in **miles**. "
        "Times in **seconds** unless noted otherwise."
    ),
    version="1.0.0",
    contact={"name": "Data Engineering", "email": "data@example.com"},
    license_info={"name": "MIT"},
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# RESPONSE MODELS

class HourlyAggRow(BaseModel):
    trip_date: Optional[date]         = Field(None,  description="Calendar date of trips")
    trip_year: Optional[int]          = Field(None,  description="Year")
    trip_month: Optional[int]         = Field(None,  description="Month (1–12)")
    pickup_hour: Optional[int]        = Field(None,  description="Hour of day (0–23)")
    time_of_day: Optional[str]        = Field(None,  description="Morning / Afternoon / Evening / Night")
    is_weekend: Optional[bool]        = Field(None,  description="True if Saturday or Sunday")
    pu_location_id: Optional[int]     = Field(None,  description="TLC pick-up zone ID")
    trip_count: Optional[int]         = Field(None,  description="Number of trips")
    total_revenue: Optional[float]    = Field(None,  description="Total fare revenue (USD)")
    avg_fare: Optional[float]         = Field(None,  description="Mean total fare (USD)")
    avg_base_fare: Optional[float]    = Field(None,  description="Mean base fare (USD)")
    avg_tip: Optional[float]          = Field(None,  description="Mean tip amount (USD)")
    avg_tip_pct: Optional[float]      = Field(None,  description="Mean tip as % of base fare")
    avg_trip_miles: Optional[float]   = Field(None,  description="Mean trip distance (miles)")
    avg_trip_time_secs: Optional[float] = Field(None,description="Mean trip duration (seconds)")
    avg_speed_mph: Optional[float]    = Field(None,  description="Mean speed (mph)")
    airport_trip_count: Optional[int] = Field(None,  description="Trips with an airport fee")
    shared_trip_count: Optional[int]  = Field(None,  description="Shared-ride matched trips")
    revenue_per_trip: Optional[float] = Field(None,  description="Total revenue ÷ trip count")


class TripSummary(BaseModel):
    total_trips: int           = Field(..., description="All trips in the gold layer")
    total_revenue_usd: float   = Field(..., description="Sum of all fares")
    avg_fare_usd: float        = Field(..., description="Overall average fare")
    avg_speed_mph: float       = Field(..., description="Overall average speed")
    avg_trip_miles: float      = Field(..., description="Overall average trip distance")
    busiest_hour: int          = Field(..., description="Hour-of-day with most trips (0–23)")
    busiest_time_of_day: str   = Field(..., description="Time-of-day bucket with most trips")
    airport_trip_share_pct: float = Field(..., description="% of trips with an airport fee")
    shared_trip_share_pct: float  = Field(..., description="% of trips that were shared rides")


class DriverRow(BaseModel):
    license_num: Optional[str]              = Field(None, description="HVFHS license number")
    dispatch_base: Optional[str]            = Field(None, description="Dispatching base number")
    trip_date: Optional[date]               = Field(None, description="Date of the trips")
    trip_year: Optional[int]                = Field(None)
    trip_month: Optional[int]               = Field(None)
    trip_count: Optional[int]               = Field(None, description="Trips driven that day")
    total_driver_pay: Optional[float]       = Field(None, description="Total pay (USD)")
    avg_driver_pay_per_trip: Optional[float]= Field(None, description="Average pay per trip (USD)")
    total_miles: Optional[float]            = Field(None, description="Total miles driven")
    avg_speed_mph: Optional[float]          = Field(None, description="Average speed (mph)")
    avg_tip_pct: Optional[float]            = Field(None, description="Average tip %")
    airport_trips: Optional[int]            = Field(None, description="Airport trips count")
    pay_per_mile: Optional[float]           = Field(None, description="Pay per mile (USD/mile)")


class PaginatedHourly(BaseModel):
    total: int
    page: int
    page_size: int
    results: list[HourlyAggRow]


class PaginatedDrivers(BaseModel):
    total: int
    page: int
    page_size: int
    results: list[DriverRow]


# ENDPOINTS

@app.get("/health", tags=["System"], summary="Liveness probe")
def health():
    return {"status": "ok"}


# 1. Hourly aggregation

@app.get(
    "/api/v1/trips/hourly",
    response_model=PaginatedHourly,
    tags=["Trips"],
    summary="Hourly trip aggregations",
    description=(
        "Returns rows from the `gold/hourly_agg` table."
        "Filter by year, month, hour, or time-of-day bucket. "
        "Results are ordered by `trip_date` and `pickup_hour` ascending."
    ),
)
def get_hourly_agg(
    year:         Optional[int] = Query(None, ge=2019, le=2030, description="Filter by year"),
    month:        Optional[int] = Query(None, ge=1, le=12,      description="Filter by month (1–12)"),
    hour:         Optional[int] = Query(None, ge=0, le=23,      description="Filter by hour of day (0–23)"),
    time_of_day:  Optional[str] = Query(None, description="Morning | Afternoon | Evening | Night"),
    is_weekend:   Optional[bool]= Query(None, description="True = weekend trips only"),
    pu_location_id: Optional[int] = Query(None, description="TLC pick-up zone ID"),
    page:         int           = Query(1, ge=1,               description="Page number (1-based)"),
    page_size:    int           = Query(50, ge=1, le=500,      description="Rows per page"),
):
    df = _read_parquet_from_minio("gold/hourly_agg")

    # Filters
    if year          is not None and "trip_year"    in df.columns: df = df[df["trip_year"]    == year]
    if month         is not None and "trip_month"   in df.columns: df = df[df["trip_month"]   == month]
    if hour          is not None and "pickup_hour"  in df.columns: df = df[df["pickup_hour"]  == hour]
    if time_of_day   is not None and "time_of_day"  in df.columns: df = df[df["time_of_day"]  == time_of_day]
    if is_weekend    is not None and "is_weekend"   in df.columns: df = df[df["is_weekend"]   == is_weekend]
    if pu_location_id is not None and "pu_location_id" in df.columns:
        df = df[df["pu_location_id"] == pu_location_id]

    # Sort
    sort_cols = [c for c in ("trip_date", "pickup_hour") if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols)

    total = len(df)
    start = (page - 1) * page_size
    df    = df.iloc[start : start + page_size]

    # Convert date columns to Python date so Pydantic can serialise them
    for col in ("trip_date", "_loaded_at"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            if col == "trip_date":
                df[col] = df[col].dt.date

    records = df.where(pd.notna(df), None).to_dict(orient="records")
    return PaginatedHourly(
        total=total, page=page, page_size=page_size,
        results=[HourlyAggRow(**r) for r in records],
    )


# 2. High level summary

@app.get(
    "/api/v1/trips/summary",
    response_model=TripSummary,
    tags=["Trips"],
    summary="High-level dataset summary",
    description=(
        "Aggregates the entire `gold/hourly_agg` table into a single summary "
        "object — total trips, revenue, averages, and share metrics."
    ),
)
def get_trip_summary():
    df = _read_parquet_from_minio("gold/hourly_agg")

    total_trips   = int(df["trip_count"].sum())               if "trip_count"        in df.columns else 0
    total_revenue = float(df["total_revenue"].sum())          if "total_revenue"     in df.columns else 0.0
    avg_fare      = float(df["avg_fare"].mean())              if "avg_fare"          in df.columns else 0.0
    avg_speed     = float(df["avg_speed_mph"].mean())         if "avg_speed_mph"     in df.columns else 0.0
    avg_miles     = float(df["avg_trip_miles"].mean())        if "avg_trip_miles"    in df.columns else 0.0

    if "pickup_hour" in df.columns and "trip_count" in df.columns:
        hour_totals   = df.groupby("pickup_hour")["trip_count"].sum()
        busiest_hour  = int(hour_totals.idxmax())
    else:
        busiest_hour  = 0

    if "time_of_day" in df.columns and "trip_count" in df.columns:
        tod_totals         = df.groupby("time_of_day")["trip_count"].sum()
        busiest_tod        = str(tod_totals.idxmax())
    else:
        busiest_tod        = "Unknown"

    airport_share = 0.0
    shared_share  = 0.0
    if total_trips > 0:
        if "airport_trip_count" in df.columns:
            airport_share = float(df["airport_trip_count"].sum()) / total_trips * 100
        if "shared_trip_count" in df.columns:
            shared_share  = float(df["shared_trip_count"].sum())  / total_trips * 100

    return TripSummary(
        total_trips=total_trips,
        total_revenue_usd=round(total_revenue, 2),
        avg_fare_usd=round(avg_fare, 2),
        avg_speed_mph=round(avg_speed, 2),
        avg_trip_miles=round(avg_miles, 2),
        busiest_hour=busiest_hour,
        busiest_time_of_day=busiest_tod,
        airport_trip_share_pct=round(airport_share, 2),
        shared_trip_share_pct=round(shared_share, 2),
    )


# 3. Driver leaderboard

@app.get(
    "/api/v1/drivers/leaderboard",
    response_model=PaginatedDrivers,
    tags=["Drivers"],
    summary="Driver earnings leaderboard",
    description=(
        "Returns rows from `gold/driver_daily_agg`, sorted by `total_driver_pay` "
        "descending (highest earners first).\n\n"
        "Filter by year, month, or a specific license number."
    ),
)
def get_driver_leaderboard(
    year:        Optional[int] = Query(None, ge=2019, le=2030),
    month:       Optional[int] = Query(None, ge=1, le=12),
    license_num: Optional[str] = Query(None, description="Exact HVFHS license number"),
    page:        int           = Query(1, ge=1),
    page_size:   int           = Query(50, ge=1, le=500),
):
    df = _read_parquet_from_minio("gold/driver_daily_agg")

    if year        is not None and "trip_year"   in df.columns: df = df[df["trip_year"]   == year]
    if month       is not None and "trip_month"  in df.columns: df = df[df["trip_month"]  == month]
    if license_num is not None and "license_num" in df.columns: df = df[df["license_num"] == license_num]

    if "total_driver_pay" in df.columns:
        df = df.sort_values("total_driver_pay", ascending=False)

    total = len(df)
    start = (page - 1) * page_size
    df    = df.iloc[start : start + page_size]

    for col in ("trip_date", "_loaded_at"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            if col == "trip_date":
                df[col] = df[col].dt.date

    records = df.where(pd.notna(df), None).to_dict(orient="records")
    return PaginatedDrivers(
        total=total, page=page, page_size=page_size,
        results=[DriverRow(**r) for r in records],
    )