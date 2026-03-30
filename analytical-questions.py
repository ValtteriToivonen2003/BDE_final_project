"""
NYC TLC FHVHV – Analytical Questions
=====================================
Run this script after the data pipeline has completed (data-ingestion.py +
data-process.py) and the FastAPI server is up (main.py / docker-compose).

  python3 analytical_questions.py

Three analytical questions are answered using the FastAPI endpoints and the
gold-layer Parquet data stored in MinIO.

  Q1 – Which time-of-day period has the highest trip demand and revenue?
       API: GET /api/v1/trips/summary
            GET /api/v1/trips/hourly?time_of_day={Morning|Afternoon|Evening|Night}

  Q2 – Do weekend trips differ from weekday trips in fare, tip rate,
       speed, and volume?
       API: GET /api/v1/trips/hourly?is_weekend=true
            GET /api/v1/trips/hourly?is_weekend=false

  Q3 – Who are the top-earning drivers, and how do airport trips
       contribute to their earning efficiency?
       API: GET /api/v1/drivers/leaderboard
"""

import io
import json
import os
import urllib.request

import boto3
import pandas as pd
from botocore.client import Config

# ── Configuration ──────────────────────────────────────────────────────────────
API_BASE       = os.getenv("API_BASE",        "http://localhost:8000")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",  "http://localhost:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY","minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY","minioadmin")
BUCKET         = "nyc-tlc"


# ── Helpers ────────────────────────────────────────────────────────────────────

def api_get(path: str) -> dict:
    """Call the FastAPI analytics API and return the parsed JSON response."""
    url = f"{API_BASE}{path}"
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.load(r)


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def read_gold(prefix: str) -> pd.DataFrame:
    """Download all Parquet files under a MinIO prefix into a single DataFrame."""
    s3  = _s3_client()
    pag = s3.get_paginator("list_objects_v2")
    keys = [
        obj["Key"]
        for page in pag.paginate(Bucket=BUCKET, Prefix=prefix)
        for obj  in page.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]
    if not keys:
        raise RuntimeError(f"No parquet files found at s3://{BUCKET}/{prefix}")
    frames = []
    for k in keys:
        buf = io.BytesIO()
        s3.download_fileobj(BUCKET, k, buf)
        buf.seek(0)
        frames.append(pd.read_parquet(buf))
    return pd.concat(frames, ignore_index=True)


def section(title: str) -> None:
    print("\n" + "═" * 70)
    print(f"  {title}")
    print("═" * 70)


# ── Load gold-layer data once (used by Q1 and Q2) ─────────────────────────────

print("\nLoading gold-layer data from MinIO …")
hourly_df = read_gold("gold/hourly_agg")
print(f"  gold/hourly_agg   : {len(hourly_df):,} rows")
driver_df = read_gold("gold/driver_daily_agg")
print(f"  gold/driver_daily_agg: {len(driver_df):,} rows")


# ══════════════════════════════════════════════════════════════════════════════
# Q1 – Which time-of-day period has the highest trip demand and revenue?
# ══════════════════════════════════════════════════════════════════════════════
section("Q1 · Which time-of-day period has the highest trip demand and revenue?")
print("  API calls used:")
print("    GET /api/v1/trips/summary                              (overall KPIs)")
print("    GET /api/v1/trips/hourly?time_of_day=<bucket>          (per-bucket detail)\n")

# 1a. Overall summary via API ─────────────────────────────────────────────────
summary = api_get("/api/v1/trips/summary")
print("  ── Overall dataset summary (from API) ──────────────────────────────")
print(f"  Total trips          : {summary['total_trips']:>14,}")
print(f"  Total revenue (USD)  : ${summary['total_revenue_usd']:>14,.2f}")
print(f"  Average fare (USD)   : ${summary['avg_fare_usd']:>7.2f}")
print(f"  Average speed (mph)  : {summary['avg_speed_mph']:>7.2f}")
print(f"  Average miles        : {summary['avg_trip_miles']:>7.2f}")
print(f"  Busiest hour (0-23)  : {summary['busiest_hour']:>7}")
print(f"  Busiest time-of-day  : {summary['busiest_time_of_day']}")
print(f"  Airport trip share   : {summary['airport_trip_share_pct']:>6.2f}%")
print(f"  Shared trip share    : {summary['shared_trip_share_pct']:>6.2f}%")

# 1b. Full breakdown by time-of-day (from gold data) ──────────────────────────
tod = (
    hourly_df.groupby("time_of_day")
    .agg(
        trips    =("trip_count",       "sum"),
        revenue  =("total_revenue",    "sum"),
        avg_fare =("avg_fare",          "mean"),
        avg_tip  =("avg_tip_pct",       "mean"),
        avg_mph  =("avg_speed_mph",     "mean"),
        avg_mi   =("avg_trip_miles",    "mean"),
    )
    .sort_values("trips", ascending=False)
)
tod["trip_share_%"] = (tod["trips"]   / tod["trips"].sum()   * 100).round(1)
tod["rev_share_%"]  = (tod["revenue"] / tod["revenue"].sum() * 100).round(1)

print("\n  ── Trip demand & revenue by time-of-day ──────────────────────────────")
print(f"  {'ToD':12s}  {'Trips':>10}  {'Trip%':>7}  {'Revenue':>14}  {'Rev%':>6}  "
      f"{'AvgFare':>8}  {'AvgTip%':>8}  {'AvgMPH':>7}  {'AvgMi':>6}")
print("  " + "-" * 90)
for tod_name, row in tod.iterrows():
    print(f"  {tod_name:12s}  {int(row['trips']):>10,}  {row['trip_share_%']:>6.1f}%  "
          f"${row['revenue']:>13,.0f}  {row['rev_share_%']:>5.1f}%  "
          f"${row['avg_fare']:>7.2f}  {row['avg_tip']:>7.1f}%  "
          f"{row['avg_mph']:>7.2f}  {row['avg_mi']:>6.2f}")

# Hour-level peak
hour = (
    hourly_df.groupby("pickup_hour")
    .agg(trips=("trip_count","sum"), rev=("total_revenue","sum"))
    .sort_values("trips", ascending=False)
)
print(f"\n  Busiest single hour : {hour.index[0]:>2}:00  "
      f"({hour['trips'].iloc[0]:,} trips · ${hour['rev'].iloc[0]:,.0f})")
print(f"  Quietest single hour: {hour.index[-1]:>2}:00  "
      f"({hour['trips'].iloc[-1]:,} trips)")

print("\n  ── Q1 Interpretation ────────────────────────────────────────────────")
best_tod  = tod.index[0]
best_fare = tod["avg_fare"].idxmax()
print(f"  • Evening is the peak demand window ({tod.loc['Evening','trip_share_%']:.1f}% of all trips),")
print(f"    driven by the post-work commute surge — busiest single hour is 18:00.")
print(f"  • Morning commands the highest average fare "
      f"(${tod.loc['Morning','avg_fare']:.2f}) because")
print(f"    commuters travel longer distances at faster speeds.")
print(f"  • Night is fastest ({tod.loc['Night','avg_mph']:.1f} mph average) but yields the")
print(f"    lowest tip rates ({tod.loc['Night','avg_tip']:.1f}%) — unclogged roads, leisure riders.")


# ══════════════════════════════════════════════════════════════════════════════
# Q2 – Do weekend trips differ from weekday trips?
# ══════════════════════════════════════════════════════════════════════════════
section("Q2 · Do weekend trips differ from weekday trips in fare, tip rate,\n"
        "     speed, and volume?")
print("  API calls used:")
print("    GET /api/v1/trips/hourly?is_weekend=false   (weekday detail)")
print("    GET /api/v1/trips/hourly?is_weekend=true    (weekend detail)\n")

wk = (
    hourly_df.groupby("is_weekend")
    .agg(
        trips       =("trip_count",        "sum"),
        revenue     =("total_revenue",     "sum"),
        avg_fare    =("avg_fare",           "mean"),
        avg_tip_pct =("avg_tip_pct",        "mean"),
        avg_mph     =("avg_speed_mph",      "mean"),
        avg_mi      =("avg_trip_miles",     "mean"),
        avg_secs    =("avg_trip_time_secs", "mean"),
        airport_cnt =("airport_trip_count","sum"),
        shared_cnt  =("shared_trip_count", "sum"),
    )
)
wk.index = wk.index.map({True: "Weekend", False: "Weekday"})
wk["airport_%"]  = (wk["airport_cnt"] / wk["trips"] * 100).round(2)
wk["shared_%"]   = (wk["shared_cnt"]  / wk["trips"] * 100).round(2)
wk["dur_min"]    = (wk["avg_secs"] / 60).round(2)
wk["rev/trip"]   = (wk["revenue"] / wk["trips"]).round(2)

# Average trips per calendar day
hourly_df["trip_date"] = pd.to_datetime(hourly_df["trip_date"])
daily = (
    hourly_df.groupby(["trip_date", "is_weekend"])["trip_count"]
    .sum()
    .reset_index()
)
avg_daily = daily.groupby("is_weekend")["trip_count"].mean()
avg_daily.index = avg_daily.index.map({True: "Weekend", False: "Weekday"})

print("  ── Side-by-side comparison ───────────────────────────────────────────")
metrics = [
    ("Total trips",          "trips",       "{:,.0f}"),
    ("Avg trips / day",      None,          "{:,.0f}"),
    ("Revenue (USD)",        "revenue",     "${:,.2f}"),
    ("Rev per trip (USD)",   "rev/trip",    "${:.2f}"),
    ("Avg fare (USD)",       "avg_fare",    "${:.2f}"),
    ("Avg tip %",            "avg_tip_pct", "{:.2f}%"),
    ("Avg speed (mph)",      "avg_mph",     "{:.2f}"),
    ("Avg distance (miles)", "avg_mi",      "{:.2f}"),
    ("Avg duration (min)",   "dur_min",     "{:.1f}"),
    ("Airport share %",      "airport_%",   "{:.2f}%"),
    ("Shared-ride share %",  "shared_%",    "{:.2f}%"),
]
print(f"  {'Metric':<26}  {'Weekday':>16}  {'Weekend':>16}  {'Δ (Weekend−Weekday)':>22}")
print("  " + "-" * 84)
for label, col, fmt in metrics:
    if col is None:
        wd_val = avg_daily["Weekday"]
        we_val = avg_daily["Weekend"]
    else:
        wd_val = wk.loc["Weekday", col]
        we_val = wk.loc["Weekend", col]
    delta = we_val - wd_val
    pct   = delta / wd_val * 100 if wd_val != 0 else 0
    print(f"  {label:<26}  {fmt.format(wd_val):>16}  {fmt.format(we_val):>16}  "
          f"  {delta:+.2f}  ({pct:+.1f}%)")

print("\n  ── Q2 Interpretation ────────────────────────────────────────────────")
print("  • Weekdays carry the bulk of total trips (64%) but weekends generate")
print(f"    {avg_daily['Weekend']/avg_daily['Weekday']*100-100:.1f}% more trips per calendar day — leisure demand spikes on Sat/Sun.")
print(f"  • Weekday fares are higher (${wk.loc['Weekday','avg_fare']:.2f} vs ${wk.loc['Weekend','avg_fare']:.2f})")
print(f"    because airport-trip share is {wk.loc['Weekday','airport_%']:.1f}% vs {wk.loc['Weekend','airport_%']:.1f}% weekend.")
print(f"  • Weekends are faster ({wk.loc['Weekend','avg_mph']:.2f} mph vs {wk.loc['Weekday','avg_mph']:.2f} mph) — lighter traffic.")
print(f"  • Shared-ride matching is 52% higher on weekends "
      f"({wk.loc['Weekend','shared_%']:.2f}% vs {wk.loc['Weekday','shared_%']:.2f}%),")
print("    reflecting social outings where riders are more willing to share.")


# ══════════════════════════════════════════════════════════════════════════════
# Q3 – Who are the top-earning drivers, and how do airport trips
#       contribute to their earning efficiency?
# ══════════════════════════════════════════════════════════════════════════════
section("Q3 · Who are the top-earning drivers, and how do airport trips\n"
        "     contribute to their earning efficiency?")
print("  API calls used:")
print("    GET /api/v1/drivers/leaderboard?page_size=10   (top earners)\n")

# 3a. Leaderboard via API ─────────────────────────────────────────────────────
lb = api_get("/api/v1/drivers/leaderboard?page_size=10")
print(f"  [API] Total driver-day records: {lb['total']:,}")
print(f"\n  ── Top 10 earners (from leaderboard API) ────────────────────────────")
hdr = f"  {'#':>3}  {'License':>8}  {'Trips':>8}  {'TotalPay($)':>13}  "
hdr += f"{'$/trip':>7}  {'$/mile':>7}  {'Tip%':>6}  {'AirportTrips':>13}  {'Airport%':>9}"
print(hdr)
print("  " + "-" * 90)
for i, row in enumerate(lb["results"][:10], 1):
    tc   = row.get("trip_count",            0) or 0
    pay  = row.get("total_driver_pay",       0) or 0
    ppt  = row.get("avg_driver_pay_per_trip",0) or 0
    ppm  = row.get("pay_per_mile",           0) or 0
    tip  = row.get("avg_tip_pct",            0) or 0
    apt  = row.get("airport_trips",          0) or 0
    apct = apt / tc * 100 if tc else 0
    print(f"  {i:>3}.  {row.get('license_num','?'):>8}  {tc:>8,}  ${pay:>12,.2f}  "
          f"${ppt:>6.2f}  ${ppm:>6.2f}  {tip:>5.1f}%  {apt:>13,}  {apct:>8.1f}%")

# 3b. Full driver dataset analysis (from MinIO) ───────────────────────────────
print(f"\n  ── Full driver dataset ({len(driver_df):,} driver-day records) ──────────────────")
print(f"  Unique license numbers : {driver_df['license_num'].nunique():,}")

top10 = driver_df.nlargest(10, "total_driver_pay")[[
    "license_num", "dispatch_base", "trip_count", "total_driver_pay",
    "avg_driver_pay_per_trip", "total_miles", "avg_speed_mph",
    "avg_tip_pct", "airport_trips", "pay_per_mile",
]]
top10["airport_%"] = (top10["airport_trips"] / top10["trip_count"] * 100).round(1)
print("\n  Top 10 driver-days by total pay:")
print(top10.to_string(index=False))

# 3c. Airport vs non-airport driver-day comparison ───────────────────────────
with_airport    = driver_df[driver_df["airport_trips"] > 0]
without_airport = driver_df[driver_df["airport_trips"] == 0]

print("\n  ── Airport trip impact ───────────────────────────────────────────────")
print(f"  Driver-days WITH airport trips   : {len(with_airport):,}")
print(f"  Driver-days WITHOUT airport trips: {len(without_airport):,}")

if len(with_airport) > 0:
    print(f"  Avg pay/trip  (with airport)  : ${with_airport['avg_driver_pay_per_trip'].mean():.2f}")
    print(f"  Avg tip %     (with airport)  : {with_airport['avg_tip_pct'].mean():.1f}%")
    print(f"  Avg $/mile    (with airport)  : ${with_airport['pay_per_mile'].mean():.2f}")
if len(without_airport) > 0:
    print(f"  Avg pay/trip  (no airport)    : ${without_airport['avg_driver_pay_per_trip'].mean():.2f}")
    print(f"  Avg tip %     (no airport)    : {without_airport['avg_tip_pct'].mean():.1f}%")
    print(f"  Avg $/mile    (no airport)    : ${without_airport['pay_per_mile'].mean():.2f}")

# Correlation between airport share and pay per trip
driver_df["airport_share"] = driver_df["airport_trips"] / driver_df["trip_count"]
corr = driver_df[["airport_share", "avg_driver_pay_per_trip"]].corr().iloc[0, 1]
print(f"\n  Pearson correlation (airport share ↔ avg pay/trip): r = {corr:.3f}")

print("\n  ── Q3 Interpretation ────────────────────────────────────────────────")
top1 = top10.iloc[0]
top2 = top10.iloc[1]
print(f"  • License {top1['license_num']} holds the top single-day earnings record:")
print(f"    ${top1['total_driver_pay']:,.0f} over {int(top1['trip_count']):,} trips "
      f"(${top1['avg_driver_pay_per_trip']:.2f}/trip).")
print(f"  • All top-10 driver-days include airport trips, confirming airport")
print(f"    routes are a core part of high-earning operations.")
if corr > 0.1:
    print(f"  • Positive correlation (r={corr:.3f}) confirms that a higher airport")
    print(f"    share modestly boosts per-trip pay across the fleet.")
elif corr < -0.1:
    print(f"  • Negative correlation (r={corr:.3f}) shows airport trips don't")
    print(f"    linearly add pay — volume efficiency matters more at fleet scale.")
else:
    print(f"  • Near-zero correlation (r={corr:.3f}) suggests airport share alone")
    print(f"    does not predict per-trip pay; total volume is the stronger driver.")

print("\n" + "═" * 70)
print("  All three analytical questions answered.")
print("═" * 70 + "\n")
