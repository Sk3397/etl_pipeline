"""
transform.py - Data transformation and enrichment using Pandas
"""
import pandas as pd
import numpy as np
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

COLUMN_RENAMES = {
    "time": "recorded_at", "temperature_2m": "temperature_c",
    "apparent_temperature": "feels_like_c", "relative_humidity_2m": "humidity_pct",
    "precipitation": "precipitation_mm", "wind_speed_10m": "wind_speed_kmh",
    "wind_direction_10m": "wind_direction_deg", "surface_pressure": "pressure_hpa",
    "cloud_cover": "cloud_cover_pct", "visibility": "visibility_m",
}

VALID_RANGES = {
    "temperature_c": (-80, 60), "feels_like_c": (-90, 70),
    "humidity_pct": (0, 100), "precipitation_mm": (0, 500),
    "wind_speed_kmh": (0, 500), "wind_direction_deg": (0, 360),
    "pressure_hpa": (870, 1084), "cloud_cover_pct": (0, 100),
    "visibility_m": (0, 100000), "uv_index": (0, 20),
}

def rename_columns(df):
    return df.rename(columns=COLUMN_RENAMES)

def cast_dtypes(df):
    df = df.copy()
    df["recorded_at"] = pd.to_datetime(df["recorded_at"], errors="coerce")
    df["extracted_at"] = pd.to_datetime(df["extracted_at"], errors="coerce")
    for col in VALID_RANGES:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["city"] = df["city"].astype(str).str.strip()
    df["country"] = df["country"].astype(str).str.strip().str.upper()
    return df

def clip_outliers(df):
    df = df.copy()
    df["quality_flags"] = ""
    for col, (lo, hi) in VALID_RANGES.items():
        if col not in df.columns:
            continue
        mask = (df[col] < lo) | (df[col] > hi)
        if mask.sum() > 0:
            df.loc[mask, "quality_flags"] += f"{col}_clipped;"
            df[col] = df[col].clip(lower=lo, upper=hi)
    return df

def handle_nulls(df):
    df = df.copy()
    df = df.dropna(subset=["recorded_at", "city"])
    numeric_cols = [c for c in VALID_RANGES if c in df.columns]
    df = df.sort_values(["city", "recorded_at"]).reset_index(drop=True)
    null_mask = df[numeric_cols].isnull().any(axis=1)
    df.loc[null_mask, "quality_flags"] = df.loc[null_mask, "quality_flags"] + "null_filled;"
    df[numeric_cols] = df.groupby("city")[numeric_cols].transform(lambda x: x.ffill().bfill())
    return df

def degrees_to_compass(deg):
    if pd.isna(deg):
        return "Unknown"
    dirs = ["N","NNE","NE","ENE","E","ESE","SE","SSE","S","SSW","SW","WSW","W","WNW","NW","NNW"]
    return dirs[int((float(deg) + 11.25) / 22.5) % 16]

def categorize_weather(df):
    c = pd.Series("Clear", index=df.index)
    c[df["cloud_cover_pct"] > 80] = "Overcast"
    c[df["cloud_cover_pct"].between(40, 80)] = "Partly Cloudy"
    c[df["precipitation_mm"] > 0.1] = "Light Rain"
    c[df["precipitation_mm"] > 2.5] = "Moderate Rain"
    c[df["precipitation_mm"] > 10]  = "Heavy Rain"
    c[(df["precipitation_mm"] > 0.1) & (df["temperature_c"] < 0)] = "Snow"
    c[df["wind_speed_kmh"] > 60] = "Strong Winds"
    return c

def add_derived_features(df):
    df = df.copy()
    df["temperature_f"] = (df["temperature_c"] * 9 / 5 + 32).round(2)
    df["weather_condition"] = categorize_weather(df)
    df["wind_compass"] = df["wind_direction_deg"].apply(degrees_to_compass)
    df["hour"]        = df["recorded_at"].dt.hour
    df["day_of_week"] = df["recorded_at"].dt.day_name()
    df["date"]        = df["recorded_at"].dt.date
    df["month"]       = df["recorded_at"].dt.month
    df["year"]        = df["recorded_at"].dt.year
    df["is_daytime"]  = df["hour"].between(6, 20)
    return df

def remove_duplicates(df):
    return df.drop_duplicates(subset=["city", "recorded_at"], keep="last")

def transform(raw_df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Transform: {len(raw_df):,} rows in")
    df = (raw_df
          .pipe(rename_columns)
          .pipe(cast_dtypes)
          .pipe(clip_outliers)
          .pipe(handle_nulls)
          .pipe(add_derived_features)
          .pipe(remove_duplicates))
    logger.info(f"Transform: {len(df):,} rows out, {df.shape[1]} columns")
    return df
