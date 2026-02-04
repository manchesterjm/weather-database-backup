#!/usr/bin/env python3
"""
Hourly METAR Logger

Fetches METAR observations from multiple Colorado Springs area airports.
Runs hourly via Windows Task Scheduler.

Note: Digital forecast is handled by weather_logger.py (not this script).

METAR Stations:
- KCOS: Colorado Springs Municipal Airport
- KFLY: Meadow Lake Airport (north Colorado Springs)
- KAFF: USAF Academy Airfield (north Colorado Springs)
- KFCS: Fort Carson (south Colorado Springs)
- KAPA: Centennial Airport (Denver area, ~50 mi N)
- KPUB: Pueblo Memorial Airport (~40 mi S)
"""

import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

import db_utils
import tz_utils
from script_metrics import ScriptMetrics

# Configuration - use paths from db_utils for consistency
SCRIPT_DIR = db_utils.SCRIPTS_DIR
DATA_DIR = db_utils.DATA_DIR
DB_PATH = db_utils.DB_PATH
LOG_PATH = SCRIPT_DIR / "metar_logger.log"

# METAR stations in Colorado Springs area and region
METAR_STATIONS = ["KCOS", "KFLY", "KAFF", "KFCS", "KAPA", "KPUB"]

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


def init_tables():
    """Create required tables if they don't exist."""
    conn = db_utils.get_connection()
    cursor = conn.cursor()

    # METAR table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS metar (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetch_time TEXT NOT NULL,
            station_id TEXT NOT NULL,
            observation_time TEXT NOT NULL,
            raw_metar TEXT NOT NULL,
            wind_direction_deg INTEGER,
            wind_speed_kt INTEGER,
            wind_gust_kt INTEGER,
            visibility_sm REAL,
            weather_phenomena TEXT,
            ceiling_ft INTEGER,
            sky_condition TEXT,
            temperature_c INTEGER,
            dewpoint_c INTEGER,
            altimeter_inhg REAL,
            flight_category TEXT,
            UNIQUE(station_id, observation_time)
        )
    """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_metar_time ON metar(observation_time)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_metar_station ON metar(station_id)")

    db_utils.commit_with_retry(conn, "init metar table")
    conn.close()


def fetch_metar(station_id: str) -> dict | None:
    """Fetch current METAR from AirNav for specified station."""
    airnav_url = f"https://www.airnav.com/airport/{station_id}"

    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        }
        response = requests.get(airnav_url, headers=headers, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        raw_metar = None

        metar_header = soup.find('th', string='METAR')
        if metar_header:
            metar_table = metar_header.find_parent('table')
            if metar_table:
                for tr in metar_table.find_all('tr'):
                    tds = tr.find_all('td')
                    if len(tds) >= 2:
                        station_cell = tds[0].get_text(strip=True)
                        if station_cell == station_id:
                            raw_metar = station_id + ' ' + tds[1].get_text(strip=True)
                            break

        if not raw_metar:
            logger.warning("Could not find %s METAR on AirNav page", station_id)
            return None

        raw_metar = raw_metar.rstrip('$ ').strip()
        logger.info("Raw METAR [%s]: %s", station_id, raw_metar)

        result = {
            'station_id': station_id,
            'raw_metar': raw_metar,
            'observation_time': None,
            'wind_direction_deg': None,
            'wind_speed_kt': None,
            'wind_gust_kt': None,
            'visibility_sm': None,
            'weather_phenomena': None,
            'ceiling_ft': None,
            'sky_condition': None,
            'temperature_c': None,
            'dewpoint_c': None,
            'altimeter_inhg': None,
            'flight_category': None
        }

        parts = raw_metar.split()

        for i, part in enumerate(parts):
            # Observation time (DDHHMMz)
            if re.match(r'^\d{6}Z$', part):
                day = int(part[:2])
                hour = int(part[2:4])
                minute = int(part[4:6])
                now = datetime.now()
                obs_time = datetime(now.year, now.month, day, hour, minute)
                result['observation_time'] = obs_time.strftime('%Y-%m-%dT%H:%M:00Z')

            # Wind
            elif re.match(r'^\d{3}\d{2,3}(G\d{2,3})?KT$', part):
                result['wind_direction_deg'] = int(part[:3])
                if 'G' in part:
                    gust_match = re.match(r'^\d{3}(\d{2,3})G(\d{2,3})KT$', part)
                    if gust_match:
                        result['wind_speed_kt'] = int(gust_match.group(1))
                        result['wind_gust_kt'] = int(gust_match.group(2))
                else:
                    speed_match = re.match(r'^\d{3}(\d{2,3})KT$', part)
                    if speed_match:
                        result['wind_speed_kt'] = int(speed_match.group(1))

            elif re.match(r'^VRB\d{2,3}KT$', part):
                result['wind_direction_deg'] = 0
                speed_match = re.match(r'^VRB(\d{2,3})KT$', part)
                if speed_match:
                    result['wind_speed_kt'] = int(speed_match.group(1))

            # Visibility
            elif part.endswith('SM'):
                vis_str = part[:-2]
                try:
                    if '/' in vis_str:
                        if ' ' in vis_str:
                            whole, frac = vis_str.split(' ')
                            num, den = frac.split('/')
                            result['visibility_sm'] = float(whole) + float(num) / float(den)
                        else:
                            num, den = vis_str.split('/')
                            result['visibility_sm'] = float(num) / float(den)
                    else:
                        result['visibility_sm'] = float(vis_str)
                except ValueError:
                    pass

            # Temperature/Dewpoint
            elif re.match(r'^M?\d{2}/M?\d{2}$', part):
                temp_dew = part.split('/')
                temp_str = temp_dew[0]
                dew_str = temp_dew[1]
                result['temperature_c'] = -int(temp_str[1:]) if temp_str.startswith('M') else int(temp_str)
                result['dewpoint_c'] = -int(dew_str[1:]) if dew_str.startswith('M') else int(dew_str)

            # Altimeter
            elif re.match(r'^A\d{4}$', part):
                result['altimeter_inhg'] = int(part[1:]) / 100.0

            # Sky conditions
            elif re.match(r'^(FEW|SCT|BKN|OVC|CLR|SKC|VV)\d{0,3}$', part):
                if result['sky_condition']:
                    result['sky_condition'] += ', ' + part
                else:
                    result['sky_condition'] = part

                if part.startswith(('BKN', 'OVC', 'VV')) and result['ceiling_ft'] is None:
                    height_match = re.match(r'^(BKN|OVC|VV)(\d{3})$', part)
                    if height_match:
                        result['ceiling_ft'] = int(height_match.group(2)) * 100

        # Weather phenomena
        weather_codes = []
        for part in parts:
            if re.match(r'^[-+]?(VC)?(MI|PR|BC|DR|BL|SH|TS|FZ)?(DZ|RA|SN|SG|IC|PL|GR|GS|UP|BR|FG|FU|VA|DU|SA|HZ|PY|PO|SQ|FC|SS|DS)+$', part):
                weather_codes.append(part)
        if weather_codes:
            result['weather_phenomena'] = ' '.join(weather_codes)

        # Flight category
        ceiling = result['ceiling_ft']
        vis = result['visibility_sm']

        if ceiling is not None and vis is not None:
            if ceiling < 200 or vis < 0.5:
                result['flight_category'] = 'LIFR'
            elif ceiling < 500 or vis < 1:
                result['flight_category'] = 'IFR'
            elif ceiling < 1000 or vis < 3:
                result['flight_category'] = 'MVFR'
            else:
                result['flight_category'] = 'VFR'
        elif ceiling is not None:
            if ceiling < 200:
                result['flight_category'] = 'LIFR'
            elif ceiling < 500:
                result['flight_category'] = 'IFR'
            elif ceiling < 1000:
                result['flight_category'] = 'MVFR'
            else:
                result['flight_category'] = 'VFR'
        elif vis is not None:
            if vis < 0.5:
                result['flight_category'] = 'LIFR'
            elif vis < 1:
                result['flight_category'] = 'IFR'
            elif vis < 3:
                result['flight_category'] = 'MVFR'
            else:
                result['flight_category'] = 'VFR'

        return result

    except requests.exceptions.RequestException as e:
        logger.error("Failed to fetch METAR for %s: %s", station_id, e)
        return None
    except (ValueError, KeyError, AttributeError) as e:
        logger.error("Error parsing METAR for %s: %s", station_id, e)
        return None


def store_metar(conn: sqlite3.Connection, fetch_time: str, metar: dict):
    """Store METAR data with retry logic for database locks."""

    def do_insert(c):
        cursor = c.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO metar (
                fetch_time, station_id, observation_time, raw_metar,
                wind_direction_deg, wind_speed_kt, wind_gust_kt,
                visibility_sm, weather_phenomena, ceiling_ft, sky_condition,
                temperature_c, dewpoint_c, altimeter_inhg, flight_category
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            fetch_time,
            metar['station_id'],
            metar['observation_time'],
            metar['raw_metar'],
            metar['wind_direction_deg'],
            metar['wind_speed_kt'],
            metar['wind_gust_kt'],
            metar['visibility_sm'],
            metar['weather_phenomena'],
            metar['ceiling_ft'],
            metar['sky_condition'],
            metar['temperature_c'],
            metar['dewpoint_c'],
            metar['altimeter_inhg'],
            metar['flight_category']
        ))
        return cursor.rowcount

    try:
        rowcount = db_utils.execute_with_retry(
            do_insert, conn, f"storing METAR for {metar['station_id']}"
        )

        if rowcount > 0:
            logger.info("Stored METAR for %s at %s",
                        metar['station_id'], metar['observation_time'])
            return True
        logger.info("METAR already exists for %s at %s",
                    metar['station_id'], metar['observation_time'])
        return False

    except sqlite3.Error as e:
        logger.error("Failed to store METAR: %s", e)
        return False


def main():
    """Main entry point."""
    logger.info("=" * 50)
    logger.info("Hourly METAR Logger starting")

    DATA_DIR.mkdir(exist_ok=True)
    init_tables()

    with ScriptMetrics('metar_logger', expected_items=len(METAR_STATIONS)) as metrics:
        fetch_time = tz_utils.now_utc()
        conn = db_utils.get_connection()

        try:
            # Fetch and store METAR for all stations
            for station in METAR_STATIONS:
                metar = fetch_metar(station)
                if metar and metar['observation_time']:
                    if store_metar(conn, fetch_time, metar):
                        conn.commit()  # Release lock before metrics operation
                        metrics.item_succeeded(station, records_inserted=1, item_type='metar')
                    else:
                        # Duplicate or already exists - still counts as "success"
                        metrics.item_succeeded(station, records_inserted=0, item_type='metar')
                else:
                    metrics.item_failed(station, "Failed to fetch or parse METAR", item_type='metar')

            db_utils.commit_with_retry(conn, "final commit")
        finally:
            conn.close()

        metar_stored = sum(1 for i in metrics.items.values()
                          if i.status == "success" and i.records_inserted > 0)
        logger.info("Hourly METAR Logger complete - stored %d/%d stations",
                    metar_stored, len(METAR_STATIONS))

    return 0 if metrics.status != "failed" else 1


if __name__ == "__main__":
    exit(main())
