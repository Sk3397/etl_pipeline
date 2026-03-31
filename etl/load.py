"""
load.py - Load transformed data into PostgreSQL
"""
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging
import os

logger = logging.getLogger(__name__)

CREATE_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS weather;"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS weather.hourly_observations (
    id                  BIGSERIAL PRIMARY KEY,
    city                VARCHAR(100)  NOT NULL,
    country             CHAR(2)       NOT NULL,
    latitude            NUMERIC(9,6),
    longitude           NUMERIC(9,6),
    recorded_at         TIMESTAMPTZ   NOT NULL,
    date                DATE,
    year                SMALLINT,
    month               SMALLINT,
    hour                SMALLINT,
    day_of_week         VARCHAR(10),
    is_daytime          BOOLEAN,
    temperature_c       NUMERIC(6,2),
    temperature_f       NUMERIC(6,2),
    feels_like_c        NUMERIC(6,2),
    humidity_pct        NUMERIC(5,2),
    precipitation_mm    NUMERIC(7,3),
    wind_speed_kmh      NUMERIC(7,2),
    wind_direction_deg  NUMERIC(6,2),
    wind_compass        VARCHAR(4),
    pressure_hpa        NUMERIC(8,2),
    cloud_cover_pct     NUMERIC(5,2),
    visibility_m        NUMERIC(10,2),
    uv_index            NUMERIC(5,2),
    weather_condition   VARCHAR(30),
    quality_flags       TEXT DEFAULT '',
    extracted_at        TIMESTAMPTZ,
    loaded_at           TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_city_time UNIQUE (city, recorded_at)
);
CREATE INDEX IF NOT EXISTS idx_city        ON weather.hourly_observations (city);
CREATE INDEX IF NOT EXISTS idx_recorded_at ON weather.hourly_observations (recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_country     ON weather.hourly_observations (country);
CREATE INDEX IF NOT EXISTS idx_date        ON weather.hourly_observations (date DESC);
"""

def get_engine(db_url=None):
    url = db_url or os.getenv("DATABASE_URL", "postgresql://etl_user:etl_pass@localhost:5432/weather_db")
    return create_engine(url, pool_size=5, max_overflow=10)

def setup_schema(engine):
    with engine.begin() as conn:
        conn.execute(text(CREATE_SCHEMA_SQL))
        for stmt in CREATE_TABLE_SQL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(text(stmt))
    logger.info("Schema ready")

def load_dataframe(df: pd.DataFrame, engine, batch_size=5000) -> int:
    if df.empty:
        return 0
    db_cols = [
        "city","country","latitude","longitude","recorded_at","date","year","month",
        "hour","day_of_week","is_daytime","temperature_c","temperature_f","feels_like_c",
        "humidity_pct","precipitation_mm","wind_speed_kmh","wind_direction_deg",
        "wind_compass","pressure_hpa","cloud_cover_pct","visibility_m","uv_index",
        "weather_condition","quality_flags","extracted_at",
    ]
    load_cols = [c for c in db_cols if c in df.columns]
    df_load = df[load_cols].where(pd.notnull(df[load_cols]), None)
    total = len(df_load)
    loaded = 0
    cols = ", ".join(load_cols)
    ph   = ", ".join([f":{c}" for c in load_cols])
    upsert_sql = f"""
        INSERT INTO weather.hourly_observations ({cols}) VALUES ({ph})
        ON CONFLICT (city, recorded_at) DO UPDATE SET
            temperature_c=EXCLUDED.temperature_c, humidity_pct=EXCLUDED.humidity_pct,
            precipitation_mm=EXCLUDED.precipitation_mm, wind_speed_kmh=EXCLUDED.wind_speed_kmh,
            weather_condition=EXCLUDED.weather_condition, quality_flags=EXCLUDED.quality_flags,
            loaded_at=NOW();
    """
    try:
        with engine.begin() as conn:
            for start in range(0, total, batch_size):
                batch = df_load.iloc[start:start+batch_size].to_dict(orient="records")
                conn.execute(text(upsert_sql), batch)
                loaded += len(batch)
                logger.info(f"  Loaded {loaded:,}/{total:,}")
    except SQLAlchemyError as e:
        logger.error(f"Load error: {e}")
        raise
    return loaded

def load(df: pd.DataFrame, db_url=None) -> int:
    engine = get_engine(db_url)
    setup_schema(engine)
    rows = load_dataframe(df, engine)
    logger.info(f"Load complete: {rows:,} rows")
    return rows
