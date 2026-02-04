#!/usr/bin/env python3
"""
NBM (National Blend of Models) Data Logger

Downloads NBM model data from NOAA's AWS bucket and stores extracted
weather parameters for Colorado Springs in the weather database.

NBM blends multiple models (GFS, HRRR, RAP, NAM, ECMWF, etc.) into a
single statistically post-processed forecast. It's considered one of
the most accurate operational forecasts available.

NBM runs hourly with forecasts out to 264 hours. This logger runs 4x daily
to capture key forecast hours without excessive downloads.

Designed to run 4x daily:
  - 06:00 UTC (05Z run available)
  - 12:00 UTC (11Z run available)
  - 18:00 UTC (17Z run available)
  - 00:00 UTC (23Z run available)

Requires:
  - pygrib library (conda: conda install -c conda-forge pygrib)
  - numpy
  - requests

Data Source: https://noaa-nbm-grib2-pds.s3.amazonaws.com/
Resolution: 2.5 km (CONUS)
"""

import logging
import sqlite3
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

import db_utils
import tz_utils
from script_metrics import ScriptMetrics

# Optional imports (checked at runtime)
try:
    import pygrib
    import numpy as np
    HAS_PYGRIB = True
except ImportError:
    HAS_PYGRIB = False

# Configuration - use paths from db_utils for consistency
SCRIPT_DIR = db_utils.SCRIPTS_DIR
DATA_DIR = db_utils.DATA_DIR
DB_PATH = db_utils.DB_PATH
LOG_PATH = SCRIPT_DIR / "nbm_logger.log"

# S3 bucket (accessible via HTTP)
BUCKET = "noaa-nbm-grib2-pds"
BASE_URL = f"https://{BUCKET}.s3.amazonaws.com"

# Colorado Springs coordinates
COLORADO_SPRINGS = {
    "lat": 38.9194,
    "lon": -104.7509,
    "name": "Colorado Springs, CO"
}

# Forecast hours to extract (NBM has hourly out to 36h, then 3-hourly)
# Keep it reasonable due to file sizes (100-170 MB each)
FORECAST_HOURS = [1, 6, 12, 24, 36, 48, 72, 96, 120, 168]

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def init_nbm_table():
    """Create NBM forecast table if it doesn't exist."""
    conn = db_utils.get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS nbm_forecasts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetch_time TEXT NOT NULL,
            model_run_date TEXT NOT NULL,
            model_run_cycle TEXT NOT NULL,
            forecast_hour INTEGER NOT NULL,
            valid_time TEXT NOT NULL,
            temp_2m_k REAL,
            dewpoint_2m_k REAL,
            rh_2m REAL,
            apparent_temp_k REAL,
            max_temp_k REAL,
            min_temp_k REAL,
            total_precip_mm REAL,
            snow_amt_mm REAL,
            prob_precip_pct REAL,
            prob_snow_pct REAL,
            u_wind_10m_ms REAL,
            v_wind_10m_ms REAL,
            wind_speed_ms REAL,
            wind_direction_deg REAL,
            wind_gust_ms REAL,
            sky_cover_pct REAL,
            visibility_m REAL,
            ceiling_m REAL,
            UNIQUE(model_run_date, model_run_cycle, forecast_hour)
        )
    """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_nbm_valid ON nbm_forecasts(valid_time)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_nbm_fetch ON nbm_forecasts(fetch_time)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_nbm_model ON nbm_forecasts(model_run_date, model_run_cycle)")

    db_utils.commit_with_retry(conn, "init nbm table")
    conn.close()
    logger.info("NBM table initialized")


def check_file_exists(url: str) -> bool:
    """Check if a file exists at the given URL using HEAD request."""
    try:
        response = requests.head(url, timeout=10)
        return response.status_code == 200
    except requests.RequestException:
        return False


def get_latest_run():
    """Get the most recent complete NBM run."""
    today = datetime.now(timezone.utc)

    # NBM runs hourly, check last 6 hours
    for hours_back in range(6):
        check_time = today - timedelta(hours=hours_back)
        date_str = check_time.strftime("%Y%m%d")
        cycle = check_time.strftime("%H")

        # Check if f024 forecast file exists (indicates complete run)
        url = f"{BASE_URL}/blend.{date_str}/{cycle}/core/blend.t{cycle}z.core.f024.co.grib2"
        if check_file_exists(url):
            logger.info("Found latest run: %s %sZ", date_str, cycle)
            return date_str, cycle

    return None, None


def download_grib(date, cycle, forecast_hour):
    """Download an NBM GRIB2 file to temp directory."""
    filename = f"blend.t{cycle}z.core.f{forecast_hour:03d}.co.grib2"
    url = f"{BASE_URL}/blend.{date}/{cycle}/core/{filename}"

    local_path = Path(tempfile.gettempdir()) / filename

    logger.info("Downloading %s...", filename)
    try:
        response = requests.get(url, stream=True, timeout=600)  # 10 min timeout for large files
        response.raise_for_status()

        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=65536):  # 64KB chunks
                f.write(chunk)

        size_mb = local_path.stat().st_size / (1024 * 1024)
        logger.info("  Downloaded: %.1f MB", size_mb)
        return local_path

    except requests.RequestException as e:
        logger.error("  Download failed for f%03d: %s", forecast_hour, e)
        return None


def extract_data(grib_path, lat, lon):
    """Extract weather data for a specific location from GRIB2 file."""
    if not HAS_PYGRIB:
        logger.error("pygrib not installed")
        return None

    grbs = pygrib.open(str(grib_path))
    results = {}

    # NBM uses standard longitude (-180 to 180)
    target_lon = lon

    # Parameters to extract (by name since message numbers vary)
    param_map = {
        "temp_2m": {"name": "2 metre temperature", "typeOfLevel": "heightAboveGround", "level": 2},
        "dewpoint_2m": {"name": "2 metre dewpoint temperature", "typeOfLevel": "heightAboveGround", "level": 2},
        "rh_2m": {"name": "2 metre relative humidity", "typeOfLevel": "heightAboveGround", "level": 2},
        "apparent_temp": {"name": "Apparent temperature", "typeOfLevel": "heightAboveGround", "level": 2},
        "max_temp": {"name": "Maximum temperature", "typeOfLevel": "heightAboveGround", "level": 2},
        "min_temp": {"name": "Minimum temperature", "typeOfLevel": "heightAboveGround", "level": 2},
        "total_precip": {"name": "Total Precipitation", "typeOfLevel": "surface"},
        "snow_amt": {"name": "Total snowfall", "typeOfLevel": "surface"},
        "prob_precip": {"name": "Probability of precipitation", "typeOfLevel": "surface"},
        "u_wind_10m": {"name": "10 metre U wind component", "typeOfLevel": "heightAboveGround", "level": 10},
        "v_wind_10m": {"name": "10 metre V wind component", "typeOfLevel": "heightAboveGround", "level": 10},
        "wind_gust": {"name": "Wind speed (gust)", "typeOfLevel": "surface"},
        "sky_cover": {"name": "Total Cloud Cover", "typeOfLevel": "surface"},
        "visibility": {"name": "Visibility", "typeOfLevel": "surface"},
        "ceiling": {"name": "Ceiling", "typeOfLevel": "cloudCeiling"},
    }

    for key, params in param_map.items():
        try:
            # Try to find the message by name
            msgs = grbs.select(**params)
            if msgs:
                grb = msgs[0]
                lats, lons = grb.latlons()
                data = grb.values

                # Find nearest grid point
                lat_idx = np.argmin(np.abs(lats[:, 0] - lat))
                lon_idx = np.argmin(np.abs(lons[0, :] - target_lon))

                raw_value = data[lat_idx, lon_idx]
                if not np.ma.is_masked(raw_value):
                    results[key] = float(raw_value)
                else:
                    results[key] = None
            else:
                results[key] = None
        except (ValueError, KeyError, RuntimeError) as e:
            logger.debug("Could not extract %s: %s", key, e)
            results[key] = None

    grbs.close()

    # Calculate derived wind values
    if results.get("u_wind_10m") is not None and results.get("v_wind_10m") is not None:
        u = results["u_wind_10m"]
        v = results["v_wind_10m"]
        results["wind_speed"] = np.sqrt(u**2 + v**2)
        results["wind_direction"] = (270 - np.degrees(np.arctan2(v, u))) % 360
    else:
        results["wind_speed"] = None
        results["wind_direction"] = None

    return results


def store_nbm_data(conn, fetch_time, date, cycle, forecast_hour, valid_time, data):
    """Store extracted NBM data in database."""
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR REPLACE INTO nbm_forecasts (
            fetch_time, model_run_date, model_run_cycle, forecast_hour, valid_time,
            temp_2m_k, dewpoint_2m_k, rh_2m, apparent_temp_k, max_temp_k, min_temp_k,
            total_precip_mm, snow_amt_mm, prob_precip_pct, prob_snow_pct,
            u_wind_10m_ms, v_wind_10m_ms, wind_speed_ms, wind_direction_deg, wind_gust_ms,
            sky_cover_pct, visibility_m, ceiling_m
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        fetch_time, date, cycle, forecast_hour, valid_time,
        data.get("temp_2m"),
        data.get("dewpoint_2m"),
        data.get("rh_2m"),
        data.get("apparent_temp"),
        data.get("max_temp"),
        data.get("min_temp"),
        data.get("total_precip"),
        data.get("snow_amt"),
        data.get("prob_precip"),
        data.get("prob_snow"),
        data.get("u_wind_10m"),
        data.get("v_wind_10m"),
        data.get("wind_speed"),
        data.get("wind_direction"),
        data.get("wind_gust"),
        data.get("sky_cover"),
        data.get("visibility"),
        data.get("ceiling")
    ))


def main():
    """Main entry point."""
    logger.info("=" * 50)
    logger.info("NBM Logger starting")

    if not HAS_PYGRIB:
        logger.error("pygrib not available - run from conda environment")
        logger.error("  conda activate gfs-logger")
        return 1

    # Ensure data directory exists
    DATA_DIR.mkdir(exist_ok=True)

    # Initialize database table
    init_nbm_table()

    # Get latest model run
    date, cycle = get_latest_run()
    if not date:
        logger.error("Could not find available NBM model run")
        return 1

    logger.info("Processing NBM run: %s %sZ", date, cycle)
    model_run = f"{date} {cycle}Z"

    # Calculate model run time
    model_time = datetime.strptime(f"{date}{cycle}", "%Y%m%d%H")

    # Current fetch time
    fetch_time = tz_utils.now_utc()

    with ScriptMetrics('nbm_logger', expected_items=len(FORECAST_HOURS),
                       model_run=model_run) as metrics:
        # Connect to database
        conn = db_utils.get_connection()

        try:
            for forecast_hour in FORECAST_HOURS:
                item_name = f"f{forecast_hour:03d}"

                # Download GRIB file
                grib_path = download_grib(date, cycle, forecast_hour)
                if not grib_path:
                    metrics.item_failed(item_name, "Download failed", item_type='forecast_hour')
                    continue

                try:
                    # Extract data
                    data = extract_data(grib_path, COLORADO_SPRINGS["lat"],
                                        COLORADO_SPRINGS["lon"])
                    if not data:
                        metrics.item_failed(item_name, "Data extraction failed",
                                            item_type='forecast_hour')
                        continue

                    # Calculate valid time
                    valid_time = model_time + timedelta(hours=forecast_hour)

                    # Store in database with retry logic
                    def do_store(c):
                        store_nbm_data(c, fetch_time, date, cycle, forecast_hour,
                                       valid_time.isoformat(), data)
                    db_utils.execute_with_retry(do_store, conn, f"storing f{forecast_hour:03d}")
                    conn.commit()  # Release lock before metrics operation
                    metrics.item_succeeded(item_name, records_inserted=1,
                                           item_type='forecast_hour')

                    logger.info("  Stored f%03d (valid %s UTC)",
                                forecast_hour, valid_time.strftime('%Y-%m-%d %H:%M'))

                finally:
                    # Clean up downloaded file
                    if grib_path.exists():
                        grib_path.unlink()
                        logger.info("  Cleaned up %s", grib_path.name)

            db_utils.commit_with_retry(conn, "final commit")
            stored_count = sum(1 for i in metrics.items.values() if i.status == "success")
            logger.info("Stored %d NBM forecast hours", stored_count)

        except (ValueError, sqlite3.Error, OSError) as e:
            logger.error("Error: %s", e)
            conn.rollback()
            raise

        finally:
            conn.close()

        logger.info("NBM Logger complete")

    return 0 if metrics.status == "success" else 1


if __name__ == "__main__":
    sys.exit(main() or 0)
