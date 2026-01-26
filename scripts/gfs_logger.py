#!/usr/bin/env python3
"""
GFS (Global Forecast System) Data Logger

Downloads GFS model data from NOAA's AWS bucket and stores extracted
weather parameters for Colorado Springs in the weather database.

Designed to run 4x daily aligned with GFS model cycles:
  - 04:00 UTC (00Z run available)
  - 10:00 UTC (06Z run available)
  - 16:00 UTC (12Z run available)
  - 22:00 UTC (18Z run available)

Requires:
  - pygrib library (conda: conda install -c conda-forge pygrib)
  - numpy
  - requests

Data Source: https://noaa-gfs-bdp-pds.s3.amazonaws.com/
"""

import logging
import sqlite3
import sys
import tempfile
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

import db_utils
# Note: 'time' module not needed here - retry logic is in db_utils
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
LOG_PATH = SCRIPT_DIR / "gfs_logger.log"

# S3 bucket (accessible via HTTP)
BUCKET = "noaa-gfs-bdp-pds"
BASE_URL = f"https://{BUCKET}.s3.amazonaws.com"

# Colorado Springs coordinates
COLORADO_SPRINGS = {
    "lat": 38.9194,
    "lon": -104.7509,
    "name": "Colorado Springs, CO"
}

# Forecast hours to extract (skip f000 analysis - different GRIB structure)
FORECAST_HOURS = [6, 12, 24, 48, 72, 120, 168]  # 6h, 12h, 1d, 2d, 3d, 5d, 7d

# Key weather parameters and their GRIB message numbers
# Based on gfs.t00z.pgrb2.0p25.f024 structure
PARAMETERS = {
    "temp_2m": {"msg": 581, "desc": "2m Temperature", "unit": "K"},
    "dewpoint_2m": {"msg": 583, "desc": "2m Dewpoint", "unit": "K"},
    "rh_2m": {"msg": 584, "desc": "2m Relative Humidity", "unit": "%"},
    "apparent_temp": {"msg": 585, "desc": "Apparent Temperature", "unit": "K"},
    "total_precip": {"msg": 596, "desc": "Total Precipitation", "unit": "kg/m2"},
    "snow_depth": {"msg": 578, "desc": "Snow Depth", "unit": "m"},
    "cat_snow": {"msg": 601, "desc": "Categorical Snow", "unit": "bool"},
    "cat_rain": {"msg": 604, "desc": "Categorical Rain", "unit": "bool"},
    "cat_freezing_rain": {"msg": 603, "desc": "Categorical Freezing Rain", "unit": "bool"},
    "pct_frozen": {"msg": 591, "desc": "Percent Frozen Precip", "unit": "%"},
    "u_wind_10m": {"msg": 588, "desc": "10m U Wind", "unit": "m/s"},
    "v_wind_10m": {"msg": 589, "desc": "10m V Wind", "unit": "m/s"},
    "wind_gust": {"msg": 14, "desc": "Wind Gust", "unit": "m/s"},
    "cloud_cover": {"msg": 636, "desc": "Total Cloud Cover", "unit": "%"},
    "visibility": {"msg": 10, "desc": "Visibility", "unit": "m"},
    "mslp": {"msg": 1, "desc": "Mean Sea Level Pressure", "unit": "Pa"},
    "cape": {"msg": 624, "desc": "CAPE", "unit": "J/kg"},
}

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


def init_gfs_table():
    """Create GFS forecast table if it doesn't exist."""
    conn = db_utils.get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gfs_forecasts (
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
            total_precip_mm REAL,
            snow_depth_m REAL,
            cat_snow INTEGER,
            cat_rain INTEGER,
            cat_freezing_rain INTEGER,
            pct_frozen REAL,
            u_wind_10m_ms REAL,
            v_wind_10m_ms REAL,
            wind_speed_ms REAL,
            wind_direction_deg REAL,
            wind_gust_ms REAL,
            cloud_cover_pct REAL,
            visibility_m REAL,
            mslp_pa REAL,
            cape_jkg REAL,
            UNIQUE(model_run_date, model_run_cycle, forecast_hour)
        )
    """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_gfs_valid ON gfs_forecasts(valid_time)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_gfs_fetch ON gfs_forecasts(fetch_time)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_gfs_model ON gfs_forecasts(model_run_date, model_run_cycle)")

    conn.commit()
    conn.close()
    logger.info("GFS table initialized")


def list_s3_prefix(prefix: str, delimiter: str = "") -> list:
    """List objects in S3 bucket with given prefix using HTTP."""
    params = {"prefix": prefix}
    if delimiter:
        params["delimiter"] = delimiter

    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()

        # Parse XML response
        root = ET.fromstring(response.text)
        ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

        # Get common prefixes (directories) or contents (files)
        items = []
        for prefix_elem in root.findall(".//s3:CommonPrefixes/s3:Prefix", ns):
            items.append(prefix_elem.text)
        for key_elem in root.findall(".//s3:Contents/s3:Key", ns):
            items.append(key_elem.text)

        return items
    except requests.RequestException as e:
        logger.error("HTTP request error: %s", e)
        return []


def check_file_exists(url: str) -> bool:
    """Check if a file exists at the given URL using HEAD request."""
    try:
        response = requests.head(url, timeout=10)
        return response.status_code == 200
    except requests.RequestException:
        return False


def get_latest_run():
    """Get the most recent complete GFS run by checking recent dates directly."""
    # Check last 3 days, starting from today
    today = datetime.now(timezone.utc)

    for days_back in range(3):
        check_date = today - timedelta(days=days_back)
        date_str = check_date.strftime("%Y%m%d")

        # Check each cycle, most recent first
        for cycle in ["18", "12", "06", "00"]:
            # Check if f024 forecast file exists (indicates complete run)
            url = f"{BASE_URL}/gfs.{date_str}/{cycle}/atmos/gfs.t{cycle}z.pgrb2.0p25.f024"
            if check_file_exists(url):
                logger.info("Found latest run: %s %sZ", date_str, cycle)
                return date_str, cycle

    return None, None


def download_grib(date, cycle, forecast_hour):
    """Download a GFS GRIB2 file to temp directory."""
    filename = f"gfs.t{cycle}z.pgrb2.0p25.f{forecast_hour:03d}"
    url = f"{BASE_URL}/gfs.{date}/{cycle}/atmos/{filename}"

    local_path = Path(tempfile.gettempdir()) / filename

    logger.info("Downloading %s...", filename)
    try:
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()

        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
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

    # Convert longitude to 0-360 format for GFS grid
    gfs_lon = lon + 360 if lon < 0 else lon

    grbs = pygrib.open(str(grib_path))
    results = {}

    for key, param in PARAMETERS.items():
        try:
            grb = grbs.message(param["msg"])
            lats, lons = grb.latlons()
            data = grb.values

            # Find nearest grid point
            lat_idx = np.argmin(np.abs(lats[:, 0] - lat))
            lon_idx = np.argmin(np.abs(lons[0, :] - gfs_lon))

            raw_value = data[lat_idx, lon_idx]
            results[key] = float(raw_value)

        except (ValueError, KeyError, RuntimeError) as e:
            logger.warning("Could not extract %s: %s", key, e)
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


def store_gfs_data(conn, fetch_time, date, cycle, forecast_hour, valid_time, data):
    """Store extracted GFS data in database."""
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR REPLACE INTO gfs_forecasts (
            fetch_time, model_run_date, model_run_cycle, forecast_hour, valid_time,
            temp_2m_k, dewpoint_2m_k, rh_2m, apparent_temp_k,
            total_precip_mm, snow_depth_m, cat_snow, cat_rain, cat_freezing_rain, pct_frozen,
            u_wind_10m_ms, v_wind_10m_ms, wind_speed_ms, wind_direction_deg, wind_gust_ms,
            cloud_cover_pct, visibility_m, mslp_pa, cape_jkg
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        fetch_time, date, cycle, forecast_hour, valid_time,
        data.get("temp_2m"),
        data.get("dewpoint_2m"),
        data.get("rh_2m"),
        data.get("apparent_temp"),
        data.get("total_precip"),
        data.get("snow_depth"),
        1 if data.get("cat_snow") and data["cat_snow"] > 0.5 else 0,
        1 if data.get("cat_rain") and data["cat_rain"] > 0.5 else 0,
        1 if data.get("cat_freezing_rain") and data["cat_freezing_rain"] > 0.5 else 0,
        data.get("pct_frozen"),
        data.get("u_wind_10m"),
        data.get("v_wind_10m"),
        data.get("wind_speed"),
        data.get("wind_direction"),
        data.get("wind_gust"),
        data.get("cloud_cover"),
        data.get("visibility"),
        data.get("mslp"),
        data.get("cape")
    ))


def main():
    """Main entry point."""
    logger.info("=" * 50)
    logger.info("GFS Logger starting")

    if not HAS_PYGRIB:
        logger.error("pygrib not available - run from conda environment")
        logger.error("  conda activate gfs-logger")
        return 1

    # Ensure data directory exists
    DATA_DIR.mkdir(exist_ok=True)

    # Initialize database table
    init_gfs_table()

    # Get latest model run
    date, cycle = get_latest_run()
    if not date:
        logger.error("Could not find available GFS model run")
        return 1

    logger.info("Processing GFS run: %s %sZ", date, cycle)
    model_run = f"{date} {cycle}Z"

    # Calculate model run time
    model_time = datetime.strptime(f"{date}{cycle}", "%Y%m%d%H")

    # Current fetch time
    fetch_time = datetime.now().isoformat()

    with ScriptMetrics('gfs_logger', expected_items=len(FORECAST_HOURS),
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
                        store_gfs_data(c, fetch_time, date, cycle, forecast_hour,
                                       valid_time.isoformat(), data)
                    db_utils.execute_with_retry(conn, do_store, f"storing f{forecast_hour:03d}")
                    metrics.item_succeeded(item_name, records_inserted=1,
                                           item_type='forecast_hour')

                    logger.info("  Stored f%03d (valid %s UTC)",
                                forecast_hour, valid_time.strftime('%Y-%m-%d %H:%M'))

                finally:
                    # Clean up downloaded file
                    if grib_path.exists():
                        grib_path.unlink()
                        logger.info("  Cleaned up %s", grib_path.name)

            db_utils.execute_with_retry(conn, lambda c: c.commit(), "final commit")
            stored_count = sum(1 for i in metrics.items.values() if i.status == "success")
            logger.info("Stored %d GFS forecast hours", stored_count)

        except (ValueError, sqlite3.Error, OSError) as e:
            logger.error("Error: %s", e)
            conn.rollback()
            raise

        finally:
            conn.close()

        logger.info("GFS Logger complete")

    return 0 if metrics.status == "success" else 1


if __name__ == "__main__":
    sys.exit(main() or 0)
