#!/usr/bin/env python3
"""
Weather Underground PWS API Logger

Fetches personal weather station data via the WU API:
- Own station (KCOCOLOR3411) current conditions
- Nearby PWS stations (~10) current conditions

Replaces screenshot-scraping approach (capture_pws_data.py).

API: https://api.weather.com (IBM/Weather Company)
Rate limits: 1,500 calls/day, 30/min
Runs hourly at :15 via Windows Task Scheduler.
"""

import argparse
import logging
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

import db_utils
import tz_utils
from script_metrics import ScriptMetrics

# ============================================================================
# Configuration
# ============================================================================

WU_API_KEY = "d8940c3a288e44ff940c3a288e74ff72"
WU_BASE_URL = "https://api.weather.com"

OWN_STATION_ID = "KCOCOLOR3411"
OWN_STATION_LAT = 38.92
OWN_STATION_LON = -104.749
OWN_STATION_ELEVATION_FT = 6725

# API rate limiting
API_CALL_DELAY_SEC = 0.5  # 500ms between calls (max 30/min = 2s/call)

# Refresh nearby station list once per day
STATION_DISCOVERY_INTERVAL_HOURS = 24

# Paths
SCRIPT_DIR = db_utils.SCRIPTS_DIR
DATA_DIR = db_utils.DATA_DIR
LOG_PATH = SCRIPT_DIR / "wu_pws_logger.log"

# ============================================================================
# Logging
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# Database
# ============================================================================


def init_tables():
    """Create required tables if they don't exist."""
    conn = db_utils.get_connection()
    cursor = conn.cursor()

    # Own station readings
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pws_station_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            timestamp_local TEXT,
            fetch_time TEXT NOT NULL,
            temp_f REAL,
            humidity INTEGER,
            dewpoint_f REAL,
            heat_index_f REAL,
            wind_chill_f REAL,
            pressure_station_in REAL,
            pressure_sealevel_in REAL,
            wind_speed_mph REAL,
            wind_gust_mph REAL,
            wind_dir INTEGER,
            precip_rate_in REAL,
            precip_total_in REAL,
            uv_index REAL,
            solar_radiation REAL,
            UNIQUE(timestamp)
        )
    """)
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_pws_station_ts "
        "ON pws_station_data(timestamp)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_pws_station_fetch "
        "ON pws_station_data(fetch_time)"
    )

    # Nearby station registry
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pws_nearby_stations (
            station_id TEXT PRIMARY KEY,
            neighborhood TEXT,
            country TEXT,
            latitude REAL,
            longitude REAL,
            elevation_ft REAL,
            distance_km REAL,
            first_seen TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            is_active INTEGER DEFAULT 1
        )
    """)

    # Nearby station observations
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pws_nearby_observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            fetch_time TEXT NOT NULL,
            temp_f REAL,
            humidity INTEGER,
            dewpoint_f REAL,
            heat_index_f REAL,
            wind_chill_f REAL,
            pressure_station_in REAL,
            pressure_sealevel_in REAL,
            wind_speed_mph REAL,
            wind_gust_mph REAL,
            wind_dir INTEGER,
            precip_rate_in REAL,
            precip_total_in REAL,
            uv_index REAL,
            solar_radiation REAL,
            qc_status INTEGER,
            UNIQUE(station_id, timestamp),
            FOREIGN KEY (station_id)
                REFERENCES pws_nearby_stations(station_id)
        )
    """)
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_pws_nearby_ts "
        "ON pws_nearby_observations(timestamp)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_pws_nearby_station "
        "ON pws_nearby_observations(station_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_pws_nearby_fetch "
        "ON pws_nearby_observations(fetch_time)"
    )

    db_utils.commit_with_retry(conn, "init pws tables")
    conn.close()


# ============================================================================
# API Helpers
# ============================================================================


def wu_api_get(url, params):
    """Make a WU API GET request with rate limiting and error handling.

    Returns parsed JSON dict on success, None on failure or no data.
    """
    params['apiKey'] = WU_API_KEY
    params['format'] = 'json'

    try:
        response = requests.get(url, params=params, timeout=30)

        if response.status_code == 204:
            logger.info("API returned 204 (no data) for %s", url)
            return None

        response.raise_for_status()
        time.sleep(API_CALL_DELAY_SEC)
        return response.json()

    except requests.exceptions.HTTPError as exc:
        if response.status_code == 401:
            logger.error("API key invalid or expired")
        elif response.status_code == 429:
            logger.error("Rate limit exceeded")
        else:
            logger.error("HTTP error %d: %s", response.status_code, exc)
        return None
    except requests.exceptions.RequestException as exc:
        logger.error("Request failed: %s", exc)
        return None


def compute_sealevel_pressure(station_pressure_in, elevation_ft):
    """Compute sea-level-corrected pressure from station pressure.

    Uses the barometric formula:
    SLP = P_station * (1 - 0.0000225577 * elevation_m)^(-5.25588)

    Args:
        station_pressure_in: Station pressure in inches of mercury.
        elevation_ft: Station elevation in feet.

    Returns:
        Sea-level pressure in inHg, or None if inputs invalid.
    """
    if station_pressure_in is None or elevation_ft is None:
        return None
    elevation_m = elevation_ft * 0.3048
    correction = (1 - 0.0000225577 * elevation_m) ** (-5.25588)
    return round(station_pressure_in * correction, 2)


# ============================================================================
# Data Parsing
# ============================================================================


def parse_observation(obs_data):
    """Parse a WU API current observation response.

    Args:
        obs_data: JSON response from /v2/pws/observations/current

    Returns:
        Dict with parsed fields, or None if no data.
    """
    if not obs_data or 'observations' not in obs_data:
        return None

    observations = obs_data['observations']
    if not observations:
        return None

    obs = observations[0]
    imperial = obs.get('imperial', {})

    return {
        'station_id': obs.get('stationID'),
        'timestamp': obs.get('obsTimeUtc'),
        'timestamp_local': obs.get('obsTimeLocal'),
        'neighborhood': obs.get('neighborhood'),
        'country': obs.get('country'),
        'latitude': obs.get('lat'),
        'longitude': obs.get('lon'),
        'uv_index': obs.get('uv'),
        'solar_radiation': obs.get('solarRadiation'),
        'wind_dir': obs.get('winddir'),
        'humidity': obs.get('humidity'),
        'qc_status': obs.get('qcStatus'),
        'temp_f': imperial.get('temp'),
        'heat_index_f': imperial.get('heatIndex'),
        'dewpoint_f': imperial.get('dewpt'),
        'wind_chill_f': imperial.get('windChill'),
        'wind_speed_mph': imperial.get('windSpeed'),
        'wind_gust_mph': imperial.get('windGust'),
        'pressure_station_in': imperial.get('pressure'),
        'precip_rate_in': imperial.get('precipRate'),
        'precip_total_in': imperial.get('precipTotal'),
        'elevation_ft': imperial.get('elev'),
    }


def parse_nearby_stations(data):
    """Parse the columnar nearby stations response into a list of dicts.

    Args:
        data: JSON response from /v3/location/near

    Returns:
        List of station dicts with id, lat, lon, distance_km, neighborhood.
    """
    if not data or 'location' not in data:
        return []

    loc = data['location']
    station_ids = loc.get('stationId', [])
    stations = []

    for i, station_id in enumerate(station_ids):
        stations.append({
            'station_id': station_id,
            'neighborhood': _safe_index(loc.get('stationName', []), i),
            'latitude': _safe_index(loc.get('latitude', []), i),
            'longitude': _safe_index(loc.get('longitude', []), i),
            'distance_km': _safe_index(loc.get('distanceKm', []), i),
            'qc_status': _safe_index(loc.get('qcStatus', []), i),
        })

    return stations


def _safe_index(lst, idx):
    """Safely index a list, returning None if out of bounds."""
    if idx < len(lst):
        return lst[idx]
    return None


# ============================================================================
# Fetch Functions
# ============================================================================


def fetch_station_current(station_id):
    """Fetch current observations for a single PWS station.

    Returns parsed observation dict or None.
    """
    data = wu_api_get(
        f"{WU_BASE_URL}/v2/pws/observations/current",
        {'stationId': station_id, 'units': 'e'}
    )
    return parse_observation(data)


def fetch_nearby_station_list():
    """Fetch list of nearby PWS stations from WU API.

    Returns list of station metadata dicts.
    """
    data = wu_api_get(
        f"{WU_BASE_URL}/v3/location/near",
        {
            'geocode': f"{OWN_STATION_LAT},{OWN_STATION_LON}",
            'product': 'pws'
        }
    )
    return parse_nearby_stations(data)


# ============================================================================
# Store Functions
# ============================================================================


def store_own_observation(conn, fetch_time, obs):
    """Store own station observation in pws_station_data."""
    slp = compute_sealevel_pressure(
        obs['pressure_station_in'], OWN_STATION_ELEVATION_FT
    )

    def do_insert(database):
        cursor = database.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO pws_station_data (
                timestamp, timestamp_local, fetch_time,
                temp_f, humidity, dewpoint_f,
                heat_index_f, wind_chill_f,
                pressure_station_in, pressure_sealevel_in,
                wind_speed_mph, wind_gust_mph, wind_dir,
                precip_rate_in, precip_total_in,
                uv_index, solar_radiation
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            obs['timestamp'], obs['timestamp_local'], fetch_time,
            obs['temp_f'], obs['humidity'], obs['dewpoint_f'],
            obs['heat_index_f'], obs['wind_chill_f'],
            obs['pressure_station_in'], slp,
            obs['wind_speed_mph'], obs['wind_gust_mph'], obs['wind_dir'],
            obs['precip_rate_in'], obs['precip_total_in'],
            obs['uv_index'], obs['solar_radiation']
        ))
        return cursor.rowcount

    try:
        rowcount = db_utils.execute_with_retry(
            do_insert, conn, f"storing own station {OWN_STATION_ID}"
        )
        if rowcount > 0:
            logger.info("Stored own station data at %s", obs['timestamp'])
            return True
        logger.info("Own station data already exists for %s", obs['timestamp'])
        return False
    except sqlite3.Error as exc:
        logger.error("Failed to store own station data: %s", exc)
        return False


def store_nearby_station_meta(conn, fetch_time, station):
    """Insert or update nearby station metadata."""

    def do_upsert(database):
        cursor = database.cursor()
        cursor.execute("""
            INSERT INTO pws_nearby_stations (
                station_id, neighborhood, latitude, longitude,
                distance_km, first_seen, last_seen, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(station_id) DO UPDATE SET
                neighborhood = excluded.neighborhood,
                latitude = excluded.latitude,
                longitude = excluded.longitude,
                distance_km = excluded.distance_km,
                last_seen = excluded.last_seen,
                is_active = 1
        """, (
            station['station_id'], station['neighborhood'],
            station['latitude'], station['longitude'],
            station['distance_km'], fetch_time, fetch_time
        ))
        return cursor.rowcount

    try:
        db_utils.execute_with_retry(
            do_upsert, conn, f"storing station meta {station['station_id']}"
        )
    except sqlite3.Error as exc:
        logger.error("Failed to store station meta: %s", exc)


def store_nearby_observation(conn, fetch_time, obs):
    """Store a nearby station observation in pws_nearby_observations."""
    elevation = obs.get('elevation_ft')
    slp = compute_sealevel_pressure(obs['pressure_station_in'], elevation)

    def do_insert(database):
        cursor = database.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO pws_nearby_observations (
                station_id, timestamp, fetch_time,
                temp_f, humidity, dewpoint_f,
                heat_index_f, wind_chill_f,
                pressure_station_in, pressure_sealevel_in,
                wind_speed_mph, wind_gust_mph, wind_dir,
                precip_rate_in, precip_total_in,
                uv_index, solar_radiation, qc_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            obs['station_id'], obs['timestamp'], fetch_time,
            obs['temp_f'], obs['humidity'], obs['dewpoint_f'],
            obs['heat_index_f'], obs['wind_chill_f'],
            obs['pressure_station_in'], slp,
            obs['wind_speed_mph'], obs['wind_gust_mph'], obs['wind_dir'],
            obs['precip_rate_in'], obs['precip_total_in'],
            obs['uv_index'], obs['solar_radiation'], obs['qc_status']
        ))
        return cursor.rowcount

    try:
        rowcount = db_utils.execute_with_retry(
            do_insert, conn,
            f"storing nearby obs {obs['station_id']}"
        )
        if rowcount > 0:
            logger.info(
                "Stored %s obs at %s (%.1f\u00b0F)",
                obs['station_id'], obs['timestamp'],
                obs['temp_f'] if obs['temp_f'] is not None else 0
            )
            return True
        logger.info(
            "Obs already exists for %s at %s",
            obs['station_id'], obs['timestamp']
        )
        return False
    except sqlite3.Error as exc:
        logger.error("Failed to store nearby obs: %s", exc)
        return False


# ============================================================================
# Station List Management
# ============================================================================


def should_refresh_station_list(conn):
    """Check if the nearby station list needs refreshing.

    Returns True if no stations cached or last refresh > 24 hours ago.
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT MAX(last_seen) FROM pws_nearby_stations WHERE is_active = 1"
    )
    row = cursor.fetchone()

    if row is None or row[0] is None:
        return True

    last_seen = datetime.fromisoformat(row[0])
    threshold = datetime.now() - timedelta(
        hours=STATION_DISCOVERY_INTERVAL_HOURS
    )
    return last_seen < threshold


def get_cached_station_ids(conn):
    """Get active nearby station IDs from the database."""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT station_id FROM pws_nearby_stations "
        "WHERE is_active = 1 ORDER BY distance_km"
    )
    return [row[0] for row in cursor.fetchall()]


# ============================================================================
# Test Mode
# ============================================================================


def run_test():
    """Run in test mode - fetch and print data without writing to DB."""
    logger.info("=== TEST MODE (no database writes) ===")

    logger.info("Fetching own station %s...", OWN_STATION_ID)
    own_obs = fetch_station_current(OWN_STATION_ID)
    if own_obs:
        slp = compute_sealevel_pressure(
            own_obs['pressure_station_in'], OWN_STATION_ELEVATION_FT
        )
        logger.info("Own station data:")
        logger.info("  Temp: %s\u00b0F, Humidity: %s%%", own_obs['temp_f'],
                     own_obs['humidity'])
        logger.info("  Dewpoint: %s\u00b0F, Wind: %s mph dir %s\u00b0",
                     own_obs['dewpoint_f'], own_obs['wind_speed_mph'],
                     own_obs['wind_dir'])
        logger.info("  Pressure (station): %s inHg",
                     own_obs['pressure_station_in'])
        logger.info("  Pressure (sea-level): %s inHg", slp)
        logger.info("  UV: %s, Solar: %s W/m2",
                     own_obs['uv_index'], own_obs['solar_radiation'])
        logger.info("  Precip rate: %s in/hr, total: %s in",
                     own_obs['precip_rate_in'], own_obs['precip_total_in'])
    else:
        logger.warning("No data from own station")

    logger.info("Fetching nearby stations...")
    stations = fetch_nearby_station_list()
    logger.info("Found %d nearby stations:", len(stations))
    for station in stations:
        logger.info("  %s (%.2f km) - %s",
                     station['station_id'],
                     station['distance_km'] or 0,
                     station['neighborhood'])

    if stations:
        test_id = stations[0]['station_id']
        logger.info("Fetching sample station %s...", test_id)
        sample = fetch_station_current(test_id)
        if sample:
            logger.info("  %s: %.1f\u00b0F, %d%% humidity",
                         test_id,
                         sample['temp_f'] if sample['temp_f'] else 0,
                         sample['humidity'] if sample['humidity'] else 0)
        else:
            logger.info("  %s: no data (offline)", test_id)

    logger.info("=== TEST COMPLETE ===")
    return 0


# ============================================================================
# Main
# ============================================================================


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="WU PWS API Logger")
    parser.add_argument('--test', action='store_true',
                        help='Test mode - print data without DB writes')
    args = parser.parse_args()

    if args.test:
        return run_test()

    logger.info("=" * 50)
    logger.info("WU PWS Logger starting")

    DATA_DIR.mkdir(exist_ok=True)
    init_tables()

    fetch_time = tz_utils.now_utc()
    conn = db_utils.get_connection()
    nearby_ids = []

    try:
        # Phase 1: Refresh nearby station list if stale
        if should_refresh_station_list(conn):
            logger.info("Refreshing nearby station list")
            stations = fetch_nearby_station_list()
            if stations:
                for station in stations:
                    store_nearby_station_meta(conn, fetch_time, station)
                nearby_ids = [s['station_id'] for s in stations]
                logger.info("Discovered %d nearby stations", len(nearby_ids))
            else:
                logger.warning("Station discovery failed, using cache")
                nearby_ids = get_cached_station_ids(conn)
        else:
            nearby_ids = get_cached_station_ids(conn)
            logger.info("Using cached station list (%d stations)",
                        len(nearby_ids))

        expected_items = 1 + len(nearby_ids)

        with ScriptMetrics('wu_pws_logger',
                           expected_items=expected_items) as metrics:
            # Phase 2: Fetch own station
            own_obs = fetch_station_current(OWN_STATION_ID)
            if own_obs:
                stored = store_own_observation(conn, fetch_time, own_obs)
                metrics.item_succeeded(
                    OWN_STATION_ID,
                    records_inserted=1 if stored else 0,
                    item_type='own_pws'
                )
            else:
                metrics.item_failed(
                    OWN_STATION_ID,
                    "No data from own station",
                    item_type='own_pws'
                )

            # Phase 3: Fetch each nearby station
            for station_id in nearby_ids:
                obs = fetch_station_current(station_id)
                if obs:
                    stored = store_nearby_observation(
                        conn, fetch_time, obs
                    )
                    metrics.item_succeeded(
                        station_id,
                        records_inserted=1 if stored else 0,
                        item_type='nearby_pws'
                    )
                else:
                    metrics.item_failed(
                        station_id,
                        "No data (offline or expired)",
                        item_type='nearby_pws'
                    )

            db_utils.commit_with_retry(conn, "final commit")

            succeeded = sum(
                1 for item in metrics.items.values()
                if item.status == "success"
            )
            logger.info(
                "WU PWS Logger complete - %d/%d stations",
                succeeded, expected_items
            )

    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
