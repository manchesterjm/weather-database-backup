#!/usr/bin/env python3
"""
Weather Forecast Logger

Fetches NWS weather forecasts for Colorado Springs and stores them in SQLite
for later analysis (spaghetti charts, forecast accuracy tracking).

Runs every 6 hours via Windows Task Scheduler.

Location: Colorado Springs, CO
Coordinates: 38.9194, -104.7509
NWS Office: Pueblo (PUB)
Grid: 82,107
"""

import csv
import io
import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from script_metrics import ScriptMetrics

# Configuration
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "weather_data"
DB_PATH = DATA_DIR / "weather.db"
LOG_PATH = SCRIPT_DIR / "weather_logger.log"

# NWS API endpoints for Colorado Springs
BASE_URL = "https://api.weather.gov"
FORECAST_URL = f"{BASE_URL}/gridpoints/PUB/82,107/forecast"
HOURLY_URL = f"{BASE_URL}/gridpoints/PUB/82,107/forecast/hourly"
ALERTS_URL = f"{BASE_URL}/alerts/active?point=38.9194,-104.7509"

# Observation stations near Colorado Springs
# KCOS = Colorado Springs Municipal Airport (primary)
OBSERVATION_STATIONS = ["KCOS"]
OBSERVATIONS_URL = f"{BASE_URL}/stations/KCOS/observations"

# AirNav METAR source (more reliable/current than NWS API for raw METAR)
# Stations in Colorado Springs area and region
METAR_STATIONS = ["KCOS", "KFLY", "KAFF", "KFCS", "KAPA", "KPUB"]
# KCOS = Colorado Springs Municipal Airport
# KFLY = Meadow Lake Airport (north Colorado Springs)
# KAFF = USAF Academy Airfield (north Colorado Springs)
# KFCS = Fort Carson (south Colorado Springs)
# KAPA = Centennial Airport (Denver area, ~50 mi N)
# KPUB = Pueblo Memorial Airport (~40 mi S)

# NWS Digital Forecast (HTML) - often more up-to-date than JSON API
# Base URL template - AheadHour parameter controls which 48-hour window to fetch
# AheadHour=0: hours 0-47, AheadHour=48: hours 48-95, AheadHour=96: hours 96-143
DIGITAL_FORECAST_BASE_URL = (
    "https://forecast.weather.gov/MapClick.php?"
    "w0=t&w1=td&w2=hi&w3=sfcwind&w4=sky&w5=pop&w6=rh&w7=rain&"
    "pqpfhr=3&w15=pqpf0&w16=pqpf1&w17=pqpf2&w18=pqpf3&psnwhr=3&"
    "AheadHour={ahead_hour}&Submit=Submit&FcstType=digital&"
    "textField1=38.9194&textField2=-104.7509&site=all"
)
# Fetch 6 pages to get full 6-day forecast (each page = 24 hours, from 6am to 5am next day)
DIGITAL_FORECAST_AHEAD_HOURS = [0, 24, 48, 72, 96, 120]

# Iowa Environmental Mesonet (IEM) - Daily climate summaries
# Provides daily max/min temps, precipitation, and snowfall for forecast verification
# Data available same-day, updated throughout the day
IEM_DAILY_URL = (
    "https://mesonet.agron.iastate.edu/cgi-bin/request/daily.py?"
    "network=CO_ASOS&stations={stations}&"
    "var=max_temp_f,min_temp_f,precip_in,snow_in,avg_wind_speed_kts,max_wind_gust_kts&"
    "sts={start_date}&ets={end_date}&format=csv&na=M"
)
# Stations to fetch (Colorado Springs area)
IEM_STATIONS = ["COS", "FLY", "AFF", "FCS", "APA", "PUB"]

# NOAA NCEI Daily Snowfall - Colorado stations
# URL pattern: CO-snowfall-YYYYMM.csv (e.g., 202601 for January 2026)
NOAA_SNOWFALL_BASE_URL = "https://www.ncei.noaa.gov/access/monitoring/daily-snow"

# El Paso County station for Colorado Springs area
# Primary: Colorado Springs Municipal AP (USW00093037)
# We'll also capture other El Paso county stations for coverage
TARGET_COUNTY = "ELPASO"
PRIMARY_STATION = "USW00093037"

# Required headers for NWS API
HEADERS = {
    "User-Agent": "(Weather Logger Script, contact@example.com)",
    "Accept": "application/geo+json"
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


def get_safe_connection() -> sqlite3.Connection:
    """
    Create a database connection with crash-resilient settings.

    Uses WAL (Write-Ahead Logging) mode which:
    - Prevents corruption during unexpected shutdowns
    - Allows concurrent reads during writes
    - Provides better performance for most workloads
    """
    conn = sqlite3.connect(DB_PATH)

    # Enable WAL mode for crash resilience
    conn.execute("PRAGMA journal_mode=WAL")

    # NORMAL sync is safe with WAL mode (commits survive OS crashes)
    # FULL is only needed if the storage device lies about flush
    conn.execute("PRAGMA synchronous=NORMAL")

    # Checkpoint WAL file every 1000 pages (~4MB) to prevent unbounded growth
    conn.execute("PRAGMA wal_autocheckpoint=1000")

    # Busy timeout: wait up to 5 seconds if database is locked
    conn.execute("PRAGMA busy_timeout=5000")

    return conn


def init_database():
    """Create database tables if they don't exist."""
    conn = get_safe_connection()
    cursor = conn.cursor()

    # 7-day forecast snapshots (14 periods: day/night alternating)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS forecast_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetch_time TEXT NOT NULL,
            forecast_time TEXT NOT NULL,
            period_name TEXT,
            is_daytime INTEGER,
            temperature INTEGER,
            temperature_trend TEXT,
            wind_speed TEXT,
            wind_direction TEXT,
            precipitation_probability INTEGER,
            short_forecast TEXT,
            detailed_forecast TEXT
        )
    """)

    # Hourly forecast snapshots (156 hours)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS hourly_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetch_time TEXT NOT NULL,
            forecast_time TEXT NOT NULL,
            temperature INTEGER,
            dewpoint REAL,
            humidity INTEGER,
            wind_speed TEXT,
            wind_direction TEXT,
            precipitation_probability INTEGER,
            short_forecast TEXT
        )
    """)

    # Weather alerts
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetch_time TEXT NOT NULL,
            alert_id TEXT,
            event TEXT,
            severity TEXT,
            certainty TEXT,
            urgency TEXT,
            headline TEXT,
            description TEXT,
            effective TEXT,
            expires TEXT
        )
    """)

    # Digital forecast (HTML scrape) - often more accurate than JSON API
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS digital_forecast (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetch_time TEXT NOT NULL,
            forecast_date TEXT NOT NULL,
            forecast_hour INTEGER NOT NULL,
            temperature INTEGER,
            dewpoint INTEGER,
            heat_index INTEGER,
            wind_speed INTEGER,
            wind_direction TEXT,
            sky_cover INTEGER,
            precip_probability INTEGER,
            relative_humidity INTEGER
        )
    """)

    # Actual snowfall measurements from NOAA NCEI
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS actual_snowfall (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetch_time TEXT NOT NULL,
            station_id TEXT NOT NULL,
            station_name TEXT,
            county TEXT,
            elevation INTEGER,
            latitude REAL,
            longitude REAL,
            observation_date TEXT NOT NULL,
            snowfall_inches REAL,
            is_trace INTEGER DEFAULT 0,
            is_missing INTEGER DEFAULT 0,
            UNIQUE(station_id, observation_date)
        )
    """)

    # Actual weather observations from KCOS (Colorado Springs Municipal Airport)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetch_time TEXT NOT NULL,
            station_id TEXT NOT NULL,
            observation_time TEXT NOT NULL,
            temperature_c REAL,
            temperature_f REAL,
            dewpoint_c REAL,
            dewpoint_f REAL,
            wind_speed_ms REAL,
            wind_speed_mph REAL,
            wind_direction_deg INTEGER,
            wind_gust_ms REAL,
            wind_gust_mph REAL,
            barometric_pressure_pa REAL,
            sea_level_pressure_pa REAL,
            visibility_m REAL,
            visibility_mi REAL,
            relative_humidity REAL,
            wind_chill_c REAL,
            wind_chill_f REAL,
            heat_index_c REAL,
            heat_index_f REAL,
            cloud_coverage TEXT,
            text_description TEXT,
            raw_message TEXT,
            UNIQUE(station_id, observation_time)
        )
    """)

    # Raw METAR observations from AirNav (more current than NWS API)
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

    # Actual daily climate observations from IEM (Iowa Environmental Mesonet)
    # Used for forecast verification - daily max/min temps, precip, snowfall
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS actual_daily_climate (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetch_time TEXT NOT NULL,
            station_id TEXT NOT NULL,
            observation_date TEXT NOT NULL,
            max_temp_f REAL,
            min_temp_f REAL,
            precip_in REAL,
            snow_in REAL,
            avg_wind_speed_kts REAL,
            max_wind_gust_kts REAL,
            is_precip_trace INTEGER DEFAULT 0,
            is_snow_trace INTEGER DEFAULT 0,
            UNIQUE(station_id, observation_date)
        )
    """)

    # Indexes for efficient spaghetti chart queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_forecast_time ON forecast_snapshots(forecast_time)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fetch_time ON forecast_snapshots(fetch_time)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_hourly_forecast ON hourly_snapshots(forecast_time)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_hourly_fetch ON hourly_snapshots(fetch_time)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_alert_fetch ON alerts(fetch_time)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_digital_date ON digital_forecast(forecast_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_digital_fetch ON digital_forecast(fetch_time)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_snowfall_date ON actual_snowfall(observation_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_snowfall_station ON actual_snowfall(station_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_observations_time ON observations(observation_time)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_observations_station ON observations(station_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_metar_time ON metar(observation_time)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_metar_station ON metar(station_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_daily_climate_date ON actual_daily_climate(observation_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_daily_climate_station ON actual_daily_climate(station_id)")

    conn.commit()
    conn.close()
    logger.info("Database initialized")


def fetch_json(url: str) -> dict | None:
    """Fetch JSON from URL with error handling."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return None


def fetch_digital_forecast_page(ahead_hour: int) -> list[dict] | None:
    """
    Fetch and parse a single page of the NWS digital forecast HTML.
    Each page contains a 48-hour window starting at ahead_hour.

    Args:
        ahead_hour: Starting hour offset (0, 48, or 96)

    Returns:
        List of hourly forecast dicts with date, hour, and weather data.
    """
    url = DIGITAL_FORECAST_BASE_URL.format(ahead_hour=ahead_hour)

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # The data table is table index 4 (5th table on page)
        tables = soup.find_all('table')
        if len(tables) < 5:
            logger.error(f"Expected at least 5 tables, found {len(tables)} (AheadHour={ahead_hour})")
            return None

        data_table = tables[4]
        rows = data_table.find_all('tr')

        if len(rows) < 12:
            logger.error(f"Data table has insufficient rows: {len(rows)} (AheadHour={ahead_hour})")
            return None

        current_year = datetime.now().year
        forecasts = []

        # Row 1: Date row - first cell is "Date", rest are dates like "01/05"
        date_row = rows[1]
        date_cells = date_row.find_all(['td', 'th'])

        # Build column -> date mapping
        col_dates = {}
        current_date = None
        for col_idx, cell in enumerate(date_cells[1:], start=1):  # Skip first "Date" cell
            text = cell.get_text(strip=True)
            date_match = re.search(r'(\d{1,2})/(\d{1,2})', text)
            if date_match:
                month = int(date_match.group(1))
                day = int(date_match.group(2))
                current_date = f"{current_year}-{month:02d}-{day:02d}"
            if current_date:
                col_dates[col_idx] = current_date

        # Row 2: Hour row - first cell is "Hour (MST)", rest are hours like "09", "10"
        hour_row = rows[2]
        hour_cells = hour_row.find_all(['td', 'th'])
        col_hours = {}
        for col_idx, cell in enumerate(hour_cells[1:], start=1):
            text = cell.get_text(strip=True)
            if text.isdigit():
                col_hours[col_idx] = int(text)

        # Data rows mapping (row index -> field name)
        row_mapping = {
            3: 'temperature',      # Temperature (°F)
            4: 'dewpoint',         # Dewpoint (°F)
            # 5: wind_chill - we store as heat_index field
            6: 'wind_speed',       # Surface Wind (mph)
            7: 'wind_direction',   # Wind Dir
            # 8: gust - skip
            9: 'sky_cover',        # Sky Cover (%)
            10: 'precip_probability',  # Precipitation Potential
            11: 'relative_humidity'    # Relative Humidity (%)
        }

        # Parse data rows into column-indexed dict
        row_data = {field: {} for field in row_mapping.values()}

        for row_idx, field in row_mapping.items():
            if row_idx >= len(rows):
                continue
            cells = rows[row_idx].find_all(['td', 'th'])
            for col_idx, cell in enumerate(cells[1:], start=1):  # Skip label cell
                text = cell.get_text(strip=True)
                row_data[field][col_idx] = text

        # Also get wind chill/heat index from row 5
        heat_index_data = {}
        if len(rows) > 5:
            cells = rows[5].find_all(['td', 'th'])
            for col_idx, cell in enumerate(cells[1:], start=1):
                text = cell.get_text(strip=True)
                heat_index_data[col_idx] = text

        # Combine into forecast entries
        for col_idx in sorted(col_hours.keys()):
            if col_idx not in col_dates:
                continue

            forecast = {
                'date': col_dates[col_idx],
                'hour': col_hours[col_idx],
                'temperature': None,
                'dewpoint': None,
                'heat_index': None,
                'wind_speed': None,
                'wind_direction': None,
                'sky_cover': None,
                'precip_probability': None,
                'relative_humidity': None
            }

            # Extract values for each field
            for field in row_data:
                if col_idx in row_data[field]:
                    val = row_data[field][col_idx]
                    if field == 'wind_direction':
                        forecast[field] = val if val else None
                    else:
                        num_match = re.search(r'(-?\d+)', val)
                        if num_match:
                            forecast[field] = int(num_match.group(1))

            # Heat index / wind chill
            if col_idx in heat_index_data:
                num_match = re.search(r'(-?\d+)', heat_index_data[col_idx])
                if num_match:
                    forecast['heat_index'] = int(num_match.group(1))

            forecasts.append(forecast)

        return forecasts

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch digital forecast (AheadHour={ahead_hour}): {e}")
        return None
    except Exception as e:
        logger.error(f"Error parsing digital forecast (AheadHour={ahead_hour}): {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


def fetch_digital_forecast() -> list[dict] | None:
    """
    Fetch the full 6-day NWS digital forecast by fetching multiple pages.
    Each page contains a 48-hour window, so we fetch 3 pages (AheadHour=0, 48, 96).

    Returns:
        Combined list of hourly forecast dicts, deduplicated by date+hour.
    """
    all_forecasts = []
    seen = set()  # Track date+hour to avoid duplicates

    for ahead_hour in DIGITAL_FORECAST_AHEAD_HOURS:
        page_forecasts = fetch_digital_forecast_page(ahead_hour)
        if page_forecasts:
            for f in page_forecasts:
                key = (f['date'], f['hour'])
                if key not in seen:
                    seen.add(key)
                    all_forecasts.append(f)
            logger.info(f"Fetched {len(page_forecasts)} periods from AheadHour={ahead_hour}")
        else:
            logger.warning(f"No data from AheadHour={ahead_hour}")

    if not all_forecasts:
        return None

    # Sort by date and hour
    all_forecasts.sort(key=lambda x: (x['date'], x['hour']))
    logger.info(f"Total digital forecast periods: {len(all_forecasts)}")

    return all_forecasts


def store_forecast(conn: sqlite3.Connection, fetch_time: str, data: dict):
    """Store 7-day forecast periods."""
    cursor = conn.cursor()
    periods = data.get("properties", {}).get("periods", [])

    for period in periods:
        pop = period.get("probabilityOfPrecipitation", {})
        pop_value = pop.get("value") if pop else None

        cursor.execute("""
            INSERT INTO forecast_snapshots (
                fetch_time, forecast_time, period_name, is_daytime,
                temperature, temperature_trend, wind_speed, wind_direction,
                precipitation_probability, short_forecast, detailed_forecast
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            fetch_time,
            period.get("startTime"),
            period.get("name"),
            1 if period.get("isDaytime") else 0,
            period.get("temperature"),
            period.get("temperatureTrend"),
            period.get("windSpeed"),
            period.get("windDirection"),
            pop_value,
            period.get("shortForecast"),
            period.get("detailedForecast")
        ))

    logger.info(f"Stored {len(periods)} forecast periods")


def store_hourly(conn: sqlite3.Connection, fetch_time: str, data: dict):
    """Store hourly forecast data."""
    cursor = conn.cursor()
    periods = data.get("properties", {}).get("periods", [])

    for period in periods:
        pop = period.get("probabilityOfPrecipitation", {})
        pop_value = pop.get("value") if pop else None

        dewpoint = period.get("dewpoint", {})
        dewpoint_value = dewpoint.get("value") if dewpoint else None

        humidity = period.get("relativeHumidity", {})
        humidity_value = humidity.get("value") if humidity else None

        cursor.execute("""
            INSERT INTO hourly_snapshots (
                fetch_time, forecast_time, temperature, dewpoint, humidity,
                wind_speed, wind_direction, precipitation_probability, short_forecast
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            fetch_time,
            period.get("startTime"),
            period.get("temperature"),
            dewpoint_value,
            humidity_value,
            period.get("windSpeed"),
            period.get("windDirection"),
            pop_value,
            period.get("shortForecast")
        ))

    logger.info(f"Stored {len(periods)} hourly periods")


def store_alerts(conn: sqlite3.Connection, fetch_time: str, data: dict):
    """Store active weather alerts."""
    cursor = conn.cursor()
    features = data.get("features", [])

    for feature in features:
        props = feature.get("properties", {})

        cursor.execute("""
            INSERT INTO alerts (
                fetch_time, alert_id, event, severity, certainty, urgency,
                headline, description, effective, expires
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            fetch_time,
            props.get("id"),
            props.get("event"),
            props.get("severity"),
            props.get("certainty"),
            props.get("urgency"),
            props.get("headline"),
            props.get("description"),
            props.get("effective"),
            props.get("expires")
        ))

    alert_count = len(features)
    if alert_count > 0:
        logger.info(f"Stored {alert_count} active alerts")
    else:
        logger.info("No active alerts")


def store_digital_forecast(conn: sqlite3.Connection, fetch_time: str, forecasts: list[dict]):
    """Store digital forecast data from HTML scrape."""
    cursor = conn.cursor()

    for f in forecasts:
        cursor.execute("""
            INSERT INTO digital_forecast (
                fetch_time, forecast_date, forecast_hour,
                temperature, dewpoint, heat_index,
                wind_speed, wind_direction, sky_cover,
                precip_probability, relative_humidity
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            fetch_time,
            f['date'],
            f['hour'],
            f['temperature'],
            f['dewpoint'],
            f['heat_index'],
            f['wind_speed'],
            f['wind_direction'],
            f['sky_cover'],
            f['precip_probability'],
            f['relative_humidity']
        ))

    logger.info(f"Stored {len(forecasts)} digital forecast periods")


def fetch_noaa_snowfall() -> list[dict] | None:
    """
    Fetch NOAA NCEI daily snowfall data for Colorado.
    Returns list of station readings for El Paso county (Colorado Springs area).

    CSV format (first line is title, second line is headers):
    # Colorado Snowfall, January 2026
    GHCN ID,Station Name,County,State,Elevation,Latitude,Longitude,Jan 1,Jan 2,...
    Values: numeric inches, "M" for missing, "T" for trace
    """
    now = datetime.now()
    month_str = now.strftime("%Y%m")  # e.g., "202601" for January 2026
    url = f"{NOAA_SNOWFALL_BASE_URL}/CO-snowfall-{month_str}.csv"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # Skip comment lines at the start (lines beginning with #)
        # File format:
        #   # Colorado Snowfall, January 2026
        #   # Units: inches
        #   # Missing: M
        #   # Trace: T
        #   GHCN ID,Station Name,County,...
        lines = response.text.strip().split('\n')

        # Find first non-comment line (the actual header)
        data_start = 0
        for i, line in enumerate(lines):
            if not line.startswith('#'):
                data_start = i
                break

        if data_start >= len(lines):
            logger.error("NOAA CSV has no data (only comment lines)")
            return None

        # Use lines from data_start onward
        csv_content = '\n'.join(lines[data_start:])

        # Parse CSV
        reader = csv.DictReader(io.StringIO(csv_content))
        results = []

        for row in reader:
            # Filter for El Paso county stations
            # Handle variation: some months have "EL PASO", others "ELPASO"
            county = row.get('County', '').strip().upper().replace(' ', '')
            if county != TARGET_COUNTY:
                continue

            station_id = row.get('GHCN ID', '').strip()
            station_name = row.get('Station Name', '').strip()
            elevation = row.get('Elevation', '').strip()
            latitude = row.get('Latitude', '').strip()
            longitude = row.get('Longitude', '').strip()

            # Parse elevation, lat, lon
            try:
                elevation = int(elevation) if elevation else None
            except ValueError:
                elevation = None

            try:
                latitude = float(latitude) if latitude else None
            except ValueError:
                latitude = None

            try:
                longitude = float(longitude) if longitude else None
            except ValueError:
                longitude = None

            # Extract daily snowfall values (columns after Longitude)
            # Column names are like "Jan 1", "Jan 2", etc.
            for col_name, value in row.items():
                if col_name in ['GHCN ID', 'Station Name', 'County', 'State',
                                'Elevation', 'Latitude', 'Longitude']:
                    continue

                # Parse date from column name (e.g., "Jan 1" -> "2026-01-01")
                date_match = re.match(r'(\w+)\s+(\d+)', col_name.strip())
                if not date_match:
                    continue

                month_name = date_match.group(1)
                day = int(date_match.group(2))

                # Convert month name to number
                month_map = {
                    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,
                    'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8,
                    'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
                }
                month_num = month_map.get(month_name)
                if not month_num:
                    continue

                obs_date = f"{now.year}-{month_num:02d}-{day:02d}"

                # Parse snowfall value
                value = value.strip()
                snowfall = None
                is_trace = 0
                is_missing = 0

                if value == 'M' or value == '':
                    is_missing = 1
                elif value == 'T':
                    is_trace = 1
                    snowfall = 0.0  # Trace = measurable but < 0.1"
                else:
                    try:
                        snowfall = float(value)
                    except ValueError:
                        is_missing = 1

                results.append({
                    'station_id': station_id,
                    'station_name': station_name,
                    'county': county,
                    'elevation': elevation,
                    'latitude': latitude,
                    'longitude': longitude,
                    'observation_date': obs_date,
                    'snowfall_inches': snowfall,
                    'is_trace': is_trace,
                    'is_missing': is_missing
                })

        logger.info(f"Fetched {len(results)} snowfall readings from NOAA")
        return results

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch NOAA snowfall data: {e}")
        return None
    except Exception as e:
        logger.error(f"Error parsing NOAA snowfall data: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


def store_snowfall(conn: sqlite3.Connection, fetch_time: str, readings: list[dict]):
    """
    Store snowfall readings, using INSERT OR REPLACE to update existing records.
    Each station/date combination is unique.
    """
    cursor = conn.cursor()
    stored = 0
    skipped = 0

    for r in readings:
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO actual_snowfall (
                    fetch_time, station_id, station_name, county,
                    elevation, latitude, longitude, observation_date,
                    snowfall_inches, is_trace, is_missing
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                fetch_time,
                r['station_id'],
                r['station_name'],
                r['county'],
                r['elevation'],
                r['latitude'],
                r['longitude'],
                r['observation_date'],
                r['snowfall_inches'],
                r['is_trace'],
                r['is_missing']
            ))
            stored += 1
        except sqlite3.Error as e:
            logger.warning(f"Failed to store snowfall for {r['station_id']} on {r['observation_date']}: {e}")
            skipped += 1

    logger.info(f"Stored {stored} snowfall readings ({skipped} skipped)")


def fetch_iem_daily_climate() -> list[dict] | None:
    """
    Fetch daily climate summaries from Iowa Environmental Mesonet (IEM).
    Returns list of daily observations for Colorado Springs area ASOS stations.

    Data includes:
    - max_temp_f, min_temp_f: Daily high/low temperatures
    - precip_in: Total liquid precipitation (rain + melted snow)
    - snow_in: Snowfall in inches
    - avg_wind_speed_kts, max_wind_gust_kts: Wind data

    Trace values are represented as 0.0001 in the source data.
    """
    # Fetch last 30 days to ensure we capture any late-reported data
    from datetime import timedelta
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    stations = ",".join(IEM_STATIONS)
    url = IEM_DAILY_URL.format(
        stations=stations,
        start_date=start_date,
        end_date=end_date
    )

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        results = []
        lines = response.text.strip().split('\n')

        if len(lines) < 2:
            logger.warning("IEM returned no data")
            return None

        # Parse CSV (first line is header)
        header = lines[0].split(',')
        for line in lines[1:]:
            values = line.split(',')
            if len(values) < len(header):
                continue

            row = dict(zip(header, values))

            # Parse values, handling 'M' for missing
            def parse_float(val):
                if val == 'M' or val == '' or val is None:
                    return None
                try:
                    return float(val)
                except ValueError:
                    return None

            max_temp = parse_float(row.get('max_temp_f'))
            min_temp = parse_float(row.get('min_temp_f'))
            precip = parse_float(row.get('precip_in'))
            snow = parse_float(row.get('snow_in'))
            avg_wind = parse_float(row.get('avg_wind_speed_kts'))
            max_gust = parse_float(row.get('max_wind_gust_kts'))

            # Detect trace amounts (0.0001 in IEM data)
            is_precip_trace = 1 if precip is not None and 0 < precip < 0.005 else 0
            is_snow_trace = 1 if snow is not None and 0 < snow < 0.005 else 0

            # Convert trace to 0 for storage (flag indicates trace)
            if is_precip_trace:
                precip = 0.0
            if is_snow_trace:
                snow = 0.0

            results.append({
                'station_id': row.get('station', ''),
                'observation_date': row.get('day', ''),
                'max_temp_f': max_temp,
                'min_temp_f': min_temp,
                'precip_in': precip,
                'snow_in': snow,
                'avg_wind_speed_kts': avg_wind,
                'max_wind_gust_kts': max_gust,
                'is_precip_trace': is_precip_trace,
                'is_snow_trace': is_snow_trace
            })

        logger.info(f"Fetched {len(results)} daily climate records from IEM")
        return results

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch IEM daily climate: {e}")
        return None
    except Exception as e:
        logger.error(f"Error parsing IEM daily climate: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


def store_daily_climate(conn: sqlite3.Connection, fetch_time: str, readings: list[dict]):
    """
    Store daily climate readings, using INSERT OR REPLACE to update existing records.
    Each station/date combination is unique.
    """
    cursor = conn.cursor()
    stored = 0
    skipped = 0

    for r in readings:
        # Skip records with no valid data
        if (r['max_temp_f'] is None and r['min_temp_f'] is None and
                r['precip_in'] is None and r['snow_in'] is None):
            skipped += 1
            continue

        try:
            cursor.execute("""
                INSERT OR REPLACE INTO actual_daily_climate (
                    fetch_time, station_id, observation_date,
                    max_temp_f, min_temp_f, precip_in, snow_in,
                    avg_wind_speed_kts, max_wind_gust_kts,
                    is_precip_trace, is_snow_trace
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                fetch_time,
                r['station_id'],
                r['observation_date'],
                r['max_temp_f'],
                r['min_temp_f'],
                r['precip_in'],
                r['snow_in'],
                r['avg_wind_speed_kts'],
                r['max_wind_gust_kts'],
                r['is_precip_trace'],
                r['is_snow_trace']
            ))
            stored += 1
        except sqlite3.Error as e:
            logger.warning(f"Failed to store daily climate for {r['station_id']} on {r['observation_date']}: {e}")
            skipped += 1

    logger.info(f"Stored {stored} daily climate readings ({skipped} skipped)")


def fetch_observations() -> list[dict] | None:
    """
    Fetch recent weather observations from KCOS (Colorado Springs Municipal Airport).
    Returns list of observation records from the NWS API.

    The API returns observations in reverse chronological order.
    We fetch the last 24 hours of observations to capture hourly readings.
    """
    try:
        # Get observations - API returns most recent first
        response = requests.get(OBSERVATIONS_URL, headers=HEADERS, timeout=30)
        response.raise_for_status()
        data = response.json()

        features = data.get("features", [])
        if not features:
            logger.warning("No observation data returned from NWS API")
            return None

        observations = []
        for feature in features:
            props = feature.get("properties", {})

            # Extract values - NWS API uses nested objects with unitCode and value
            def get_value(obj):
                """Extract numeric value from NWS API response object."""
                if obj is None:
                    return None
                if isinstance(obj, dict):
                    return obj.get("value")
                return obj

            # Temperature (Celsius from API)
            temp_c = get_value(props.get("temperature"))
            temp_f = (temp_c * 9/5 + 32) if temp_c is not None else None

            # Dewpoint
            dewpoint_c = get_value(props.get("dewpoint"))
            dewpoint_f = (dewpoint_c * 9/5 + 32) if dewpoint_c is not None else None

            # Wind speed (m/s from API, convert to mph)
            wind_speed_ms = get_value(props.get("windSpeed"))
            wind_speed_mph = (wind_speed_ms * 2.237) if wind_speed_ms is not None else None

            # Wind direction (degrees)
            wind_direction = get_value(props.get("windDirection"))

            # Wind gust
            wind_gust_ms = get_value(props.get("windGust"))
            wind_gust_mph = (wind_gust_ms * 2.237) if wind_gust_ms is not None else None

            # Pressure (Pa from API)
            baro_pressure = get_value(props.get("barometricPressure"))
            sea_pressure = get_value(props.get("seaLevelPressure"))

            # Visibility (meters from API, convert to miles)
            visibility_m = get_value(props.get("visibility"))
            visibility_mi = (visibility_m / 1609.34) if visibility_m is not None else None

            # Relative humidity (percentage)
            humidity = get_value(props.get("relativeHumidity"))

            # Wind chill
            wind_chill_c = get_value(props.get("windChill"))
            wind_chill_f = (wind_chill_c * 9/5 + 32) if wind_chill_c is not None else None

            # Heat index
            heat_index_c = get_value(props.get("heatIndex"))
            heat_index_f = (heat_index_c * 9/5 + 32) if heat_index_c is not None else None

            # Cloud layers - extract coverage description
            cloud_layers = props.get("cloudLayers", [])
            cloud_coverage = None
            if cloud_layers:
                # Get the most significant cloud layer
                coverages = [layer.get("amount") for layer in cloud_layers if layer.get("amount")]
                if coverages:
                    cloud_coverage = ", ".join(coverages)

            observations.append({
                'station_id': props.get("station", "").split("/")[-1],  # Extract station ID from URL
                'observation_time': props.get("timestamp"),
                'temperature_c': temp_c,
                'temperature_f': temp_f,
                'dewpoint_c': dewpoint_c,
                'dewpoint_f': dewpoint_f,
                'wind_speed_ms': wind_speed_ms,
                'wind_speed_mph': wind_speed_mph,
                'wind_direction_deg': wind_direction,
                'wind_gust_ms': wind_gust_ms,
                'wind_gust_mph': wind_gust_mph,
                'barometric_pressure_pa': baro_pressure,
                'sea_level_pressure_pa': sea_pressure,
                'visibility_m': visibility_m,
                'visibility_mi': visibility_mi,
                'relative_humidity': humidity,
                'wind_chill_c': wind_chill_c,
                'wind_chill_f': wind_chill_f,
                'heat_index_c': heat_index_c,
                'heat_index_f': heat_index_f,
                'cloud_coverage': cloud_coverage,
                'text_description': props.get("textDescription"),
                'raw_message': props.get("rawMessage")
            })

        logger.info(f"Fetched {len(observations)} observations from KCOS")
        return observations

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch observations: {e}")
        return None
    except Exception as e:
        logger.error(f"Error parsing observations: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


def store_observations(conn: sqlite3.Connection, fetch_time: str, observations: list[dict]):
    """
    Store weather observations, using INSERT OR IGNORE to avoid duplicates.
    Each station/observation_time combination is unique.
    """
    cursor = conn.cursor()
    stored = 0
    skipped = 0

    for obs in observations:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO observations (
                    fetch_time, station_id, observation_time,
                    temperature_c, temperature_f, dewpoint_c, dewpoint_f,
                    wind_speed_ms, wind_speed_mph, wind_direction_deg,
                    wind_gust_ms, wind_gust_mph,
                    barometric_pressure_pa, sea_level_pressure_pa,
                    visibility_m, visibility_mi, relative_humidity,
                    wind_chill_c, wind_chill_f, heat_index_c, heat_index_f,
                    cloud_coverage, text_description, raw_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                fetch_time,
                obs['station_id'],
                obs['observation_time'],
                obs['temperature_c'],
                obs['temperature_f'],
                obs['dewpoint_c'],
                obs['dewpoint_f'],
                obs['wind_speed_ms'],
                obs['wind_speed_mph'],
                obs['wind_direction_deg'],
                obs['wind_gust_ms'],
                obs['wind_gust_mph'],
                obs['barometric_pressure_pa'],
                obs['sea_level_pressure_pa'],
                obs['visibility_m'],
                obs['visibility_mi'],
                obs['relative_humidity'],
                obs['wind_chill_c'],
                obs['wind_chill_f'],
                obs['heat_index_c'],
                obs['heat_index_f'],
                obs['cloud_coverage'],
                obs['text_description'],
                obs['raw_message']
            ))
            if cursor.rowcount > 0:
                stored += 1
            else:
                skipped += 1  # Already exists
        except sqlite3.Error as e:
            logger.warning(f"Failed to store observation: {e}")
            skipped += 1

    logger.info(f"Stored {stored} new observations ({skipped} duplicates skipped)")


def fetch_metar(station_id: str = "KCOS") -> dict | None:
    """
    Fetch current METAR from AirNav for specified station.
    Returns parsed METAR data dict or None on failure.

    AirNav page contains METAR in a specific format that we parse.
    Example METAR: KCOS 082354Z 04006KT 4SM -SN BR FEW004 OVC009 M01/M02 A2973
    """
    airnav_url = f"https://www.airnav.com/airport/{station_id}"

    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        }
        response = requests.get(airnav_url, headers=headers, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the METAR section header, then find station row
        raw_metar = None

        # Look for the METAR table header
        metar_header = soup.find('th', string='METAR')
        if metar_header:
            # Navigate to the parent table and find station row
            metar_table = metar_header.find_parent('table')
            if metar_table:
                # Find all rows in the nested table
                for tr in metar_table.find_all('tr'):
                    tds = tr.find_all('td')
                    if len(tds) >= 2:
                        station_cell = tds[0].get_text(strip=True)
                        if station_cell == station_id:
                            # Second TD contains the actual METAR
                            raw_metar = station_id + ' ' + tds[1].get_text(strip=True)
                            break

        if not raw_metar:
            logger.error(f"Could not find {station_id} METAR on AirNav page")
            return None

        # Clean up the METAR - remove trailing $ and extra whitespace
        raw_metar = raw_metar.rstrip('$ ').strip()
        logger.info(f"Raw METAR: {raw_metar}")

        # Parse METAR components
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
            # Observation time (DDHHMMz format)
            if re.match(r'^\d{6}Z$', part):
                # Convert to ISO format using current month/year
                day = int(part[:2])
                hour = int(part[2:4])
                minute = int(part[4:6])
                now = datetime.now()
                obs_time = datetime(now.year, now.month, day, hour, minute)
                result['observation_time'] = obs_time.strftime('%Y-%m-%dT%H:%M:00Z')

            # Wind (DDDSSkt or DDDSSGGGkt format)
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

            # Variable wind (VRB)
            elif re.match(r'^VRB\d{2,3}KT$', part):
                result['wind_direction_deg'] = 0  # Variable
                speed_match = re.match(r'^VRB(\d{2,3})KT$', part)
                if speed_match:
                    result['wind_speed_kt'] = int(speed_match.group(1))

            # Visibility (SM format)
            elif part.endswith('SM'):
                vis_str = part[:-2]
                try:
                    # Handle fractions like 1/2SM
                    if '/' in vis_str:
                        # Could be "1 1/2" or "1/2"
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

            # Check previous part for visibility whole number
            elif part == 'SM' and i > 0:
                try:
                    # Previous part might be visibility number
                    prev = parts[i-1]
                    if '/' in prev:
                        num, den = prev.split('/')
                        result['visibility_sm'] = float(num) / float(den)
                except (ValueError, IndexError):
                    pass

            # Temperature/Dewpoint (M = negative)
            elif re.match(r'^M?\d{2}/M?\d{2}$', part):
                temp_dew = part.split('/')
                temp_str = temp_dew[0]
                dew_str = temp_dew[1]

                if temp_str.startswith('M'):
                    result['temperature_c'] = -int(temp_str[1:])
                else:
                    result['temperature_c'] = int(temp_str)

                if dew_str.startswith('M'):
                    result['dewpoint_c'] = -int(dew_str[1:])
                else:
                    result['dewpoint_c'] = int(dew_str)

            # Altimeter (A#### format, in hundredths of inches)
            elif re.match(r'^A\d{4}$', part):
                result['altimeter_inhg'] = int(part[1:]) / 100.0

            # Sky conditions (FEW, SCT, BKN, OVC + height)
            elif re.match(r'^(FEW|SCT|BKN|OVC|CLR|SKC|VV)\d{0,3}$', part):
                if result['sky_condition']:
                    result['sky_condition'] += ', ' + part
                else:
                    result['sky_condition'] = part

                # Extract ceiling (lowest BKN or OVC)
                if part.startswith(('BKN', 'OVC', 'VV')) and result['ceiling_ft'] is None:
                    height_match = re.match(r'^(BKN|OVC|VV)(\d{3})$', part)
                    if height_match:
                        result['ceiling_ft'] = int(height_match.group(2)) * 100

        # Collect weather phenomena (between visibility and sky conditions)
        weather_codes = []
        for part in parts:
            if re.match(r'^[-+]?(VC)?(MI|PR|BC|DR|BL|SH|TS|FZ)?(DZ|RA|SN|SG|IC|PL|GR|GS|UP|BR|FG|FU|VA|DU|SA|HZ|PY|PO|SQ|FC|SS|DS)+$', part):
                weather_codes.append(part)
        if weather_codes:
            result['weather_phenomena'] = ' '.join(weather_codes)

        # Determine flight category based on ceiling and visibility
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

        logger.info(f"Parsed METAR: {result['temperature_c']}°C, {result['altimeter_inhg']} inHg, {result['flight_category']}")
        return result

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch METAR from AirNav: {e}")
        return None
    except Exception as e:
        logger.error(f"Error parsing METAR: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


def store_metar(conn: sqlite3.Connection, fetch_time: str, metar: dict):
    """
    Store METAR data, using INSERT OR IGNORE to avoid duplicates.
    Each station/observation_time combination is unique.
    """
    cursor = conn.cursor()

    try:
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

        if cursor.rowcount > 0:
            logger.info(f"Stored METAR for {metar['station_id']} at {metar['observation_time']}")
        else:
            logger.info(f"METAR already exists for {metar['station_id']} at {metar['observation_time']}")

    except sqlite3.Error as e:
        logger.error(f"Failed to store METAR: {e}")


def main():
    """Main entry point."""
    logger.info("=" * 50)
    logger.info("Weather Logger starting")

    # Ensure data directory exists
    DATA_DIR.mkdir(exist_ok=True)

    # Initialize database
    init_database()

    # Current fetch time (ISO 8601)
    fetch_time = datetime.now().isoformat()
    logger.info(f"Fetch time: {fetch_time}")

    # 8 data types: forecast, hourly, alerts, digital, snowfall, daily_climate, observations, metar
    with ScriptMetrics('weather_logger', expected_items=8) as metrics:
        # Fetch all data
        forecast_data = fetch_json(FORECAST_URL)
        hourly_data = fetch_json(HOURLY_URL)
        alerts_data = fetch_json(ALERTS_URL)
        digital_data = fetch_digital_forecast()
        snowfall_data = fetch_noaa_snowfall()
        daily_climate_data = fetch_iem_daily_climate()
        observation_data = fetch_observations()

        # Fetch METAR for all stations
        metar_data_list = []
        for station in METAR_STATIONS:
            metar = fetch_metar(station)
            if metar:
                metar_data_list.append(metar)

        # Store data
        conn = get_safe_connection()

        try:
            # Forecast data (required)
            if forecast_data:
                store_forecast(conn, fetch_time, forecast_data)
                periods = len(forecast_data.get("properties", {}).get("periods", []))
                metrics.item_succeeded('forecast', records_inserted=periods,
                                       item_type='nws_forecast')
            else:
                logger.error("No forecast data to store")
                metrics.item_failed('forecast', "Failed to fetch NWS forecast",
                                    item_type='nws_forecast')

            # Hourly data (required)
            if hourly_data:
                store_hourly(conn, fetch_time, hourly_data)
                periods = len(hourly_data.get("properties", {}).get("periods", []))
                metrics.item_succeeded('hourly', records_inserted=periods,
                                       item_type='nws_hourly')
            else:
                logger.error("No hourly data to store")
                metrics.item_failed('hourly', "Failed to fetch NWS hourly forecast",
                                    item_type='nws_hourly')

            # Alerts data (required - even if empty)
            if alerts_data:
                store_alerts(conn, fetch_time, alerts_data)
                alert_count = len(alerts_data.get("features", []))
                metrics.item_succeeded('alerts', records_inserted=alert_count,
                                       item_type='nws_alerts')
            else:
                logger.error("No alerts data to store")
                metrics.item_failed('alerts', "Failed to fetch NWS alerts",
                                    item_type='nws_alerts')

            # Digital forecast (required)
            if digital_data:
                store_digital_forecast(conn, fetch_time, digital_data)
                metrics.item_succeeded('digital_forecast', records_inserted=len(digital_data),
                                       item_type='nws_digital')
            else:
                logger.error("No digital forecast data to store")
                metrics.item_failed('digital_forecast', "Failed to fetch digital forecast",
                                    item_type='nws_digital')

            # Snowfall data (optional - may not be available)
            if snowfall_data:
                store_snowfall(conn, fetch_time, snowfall_data)
                metrics.item_succeeded('snowfall', records_inserted=len(snowfall_data),
                                       item_type='noaa_snowfall')
            else:
                logger.warning("No NOAA snowfall data to store (may not be available yet)")
                # Still mark as success since it's optional
                metrics.item_succeeded('snowfall', records_inserted=0,
                                       item_type='noaa_snowfall')

            # Daily climate data (optional)
            if daily_climate_data:
                store_daily_climate(conn, fetch_time, daily_climate_data)
                metrics.item_succeeded('daily_climate', records_inserted=len(daily_climate_data),
                                       item_type='iem_climate')
            else:
                logger.warning("No IEM daily climate data to store")
                # Still mark as success since it's optional
                metrics.item_succeeded('daily_climate', records_inserted=0,
                                       item_type='iem_climate')

            # Observation data (required)
            if observation_data:
                store_observations(conn, fetch_time, observation_data)
                metrics.item_succeeded('observations', records_inserted=len(observation_data),
                                       item_type='nws_observations')
            else:
                logger.error("No observation data to store")
                metrics.item_failed('observations', "Failed to fetch NWS observations",
                                    item_type='nws_observations')

            # METAR data (required)
            if metar_data_list:
                for metar in metar_data_list:
                    store_metar(conn, fetch_time, metar)
                logger.info(f"Processed METAR for {len(metar_data_list)} stations")
                metrics.item_succeeded('metar', records_inserted=len(metar_data_list),
                                       item_type='airnav_metar')
            else:
                logger.warning("No METAR data to store")
                metrics.item_failed('metar', "Failed to fetch METAR data",
                                    item_type='airnav_metar')

            conn.commit()
            logger.info("All data committed to database")

        except Exception as e:
            logger.error(f"Database error: {e}")
            conn.rollback()
            raise

        finally:
            conn.close()

        logger.info("Weather Logger complete")

    return 0 if metrics.status in ("success", "partial") else 1


if __name__ == "__main__":
    exit(main())
