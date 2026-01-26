#!/usr/bin/env python3
"""
PWS Data Capture Script

Extracts temperature data from Weather Underground's WunderMap
and saves it to the weather database.

Usage:
    python capture_pws_data.py           # Capture and save data
    python capture_pws_data.py --test    # Test extraction without saving
"""

import subprocess
import json
import sqlite3
import argparse
import logging
import time
from datetime import datetime
from pathlib import Path
import sys

import db_utils
from script_metrics import ScriptMetrics

# Configuration
DB_PATH = db_utils.DB_PATH
WUNDERMAP_URL = "https://www.wunderground.com/wundermap?lat=38.9194&lon=-104.7509&zoom=12"
SHOT_SCRAPER = "/home/josh/.local/bin/shot-scraper"  # Full path for scheduled tasks
LOG_FILE = "/mnt/d/Scripts/pws_capture.log"
MIN_STATIONS = 20  # Minimum stations required for valid data
MAX_RETRIES = 3
RETRY_DELAY = 30  # seconds between retries

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# JavaScript to extract temperature data from WunderMap
EXTRACT_JS = """
new Promise(done => setTimeout(() => {
    var temps = [];
    var markers = document.querySelectorAll('[class*=marker]');

    // Get all text content that looks like temperature readings
    var allText = document.body.innerText;
    var lines = allText.split('\\n');

    // Find lines with just numbers (temp readings)
    for (var i = 0; i < lines.length; i++) {
        var line = lines[i].trim();
        if (/^-?[0-9]{1,3}$/.test(line)) {
            var temp = parseInt(line);
            // Filter reasonable temps (-50 to 130 F)
            if (temp >= -50 && temp <= 130) {
                temps.push(temp);
            }
        }
    }

    done({
        timestamp: new Date().toISOString(),
        markerCount: markers.length,
        tempCount: temps.length,
        temps: temps,
        minTemp: temps.length > 0 ? Math.min.apply(null, temps) : null,
        maxTemp: temps.length > 0 ? Math.max.apply(null, temps) : null,
        avgTemp: temps.length > 0 ? (temps.reduce(function(a,b){return a+b}, 0) / temps.length) : null
    });
}, 15000))
"""


def extract_pws_data():
    """Use shot-scraper to extract PWS data from WunderMap."""
    logger.info("Fetching WunderMap data...")

    try:
        result = subprocess.run(
            [SHOT_SCRAPER, 'javascript', WUNDERMAP_URL, EXTRACT_JS],
            capture_output=True,
            text=True,
            timeout=90
        )

        if result.returncode != 0:
            logger.error(f"shot-scraper failed (exit {result.returncode}): {result.stderr}")
            return None

        data = json.loads(result.stdout)
        return data

    except subprocess.TimeoutExpired:
        logger.error("shot-scraper timed out (90s)")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON: {e}")
        logger.debug(f"Raw output: {result.stdout[:200]}")
        return None
    except FileNotFoundError:
        logger.error(f"shot-scraper not found at {SHOT_SCRAPER}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return None


def extract_with_retry(metrics=None):
    """Try to extract data with retries on failure."""
    for attempt in range(1, MAX_RETRIES + 1):
        logger.info(f"Attempt {attempt}/{MAX_RETRIES}")

        data = extract_pws_data()

        if data is None:
            error_msg = "Extraction returned None"
            if metrics and attempt > 1:
                metrics.record_retry(attempt, error_msg, error_type='ExtractionFailure',
                                     item_name='pws_extraction')
            if attempt < MAX_RETRIES:
                logger.warning(f"Extraction failed, retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
            continue

        # Check minimum station count
        station_count = data.get('tempCount', 0)
        if station_count < MIN_STATIONS:
            error_msg = f"Only {station_count} stations (minimum: {MIN_STATIONS})"
            if metrics and attempt > 1:
                metrics.record_retry(attempt, error_msg, error_type='InsufficientData',
                                     item_name='pws_extraction')
            logger.warning(error_msg)
            if attempt < MAX_RETRIES:
                logger.warning(f"Retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
            continue

        # Success
        return data

    logger.error(f"All {MAX_RETRIES} attempts failed")
    return None


def save_to_database(data):
    """Save extracted PWS data to the weather database."""
    if not data or data.get('tempCount', 0) == 0:
        logger.warning("No temperature data to save")
        return False

    # Round timestamp to the nearest hour for deduplication
    ts = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
    hour_ts = ts.replace(minute=0, second=0, microsecond=0).isoformat()

    conn = db_utils.get_connection()
    try:
        def do_insert(c):
            cursor = c.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO pws_observations
                (timestamp, station_count, min_temp, max_temp, avg_temp, temps_json)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                hour_ts,
                data['tempCount'],
                data['minTemp'],
                data['maxTemp'],
                round(data['avgTemp'], 1) if data['avgTemp'] else None,
                json.dumps(data['temps'])
            ))

        db_utils.execute_with_retry(do_insert, conn, "inserting PWS data")
        db_utils.commit_with_retry(conn, "committing PWS data")

        logger.info(f"Saved: {data['tempCount']} stations, "
                   f"range {data['minTemp']}-{data['maxTemp']}°F, avg {data['avgTemp']:.1f}°F")
        return True

    except sqlite3.IntegrityError:
        logger.warning(f"Data for {hour_ts} already exists")
        return False
    except sqlite3.OperationalError as e:
        logger.error(f"Database error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description='Capture PWS data from WunderMap')
    parser.add_argument('--test', action='store_true', help='Test extraction without saving')
    args = parser.parse_args()

    logger.info("=" * 50)
    logger.info("PWS Data Capture started")

    with ScriptMetrics('capture_pws_data', expected_items=1) as metrics:
        data = extract_with_retry(metrics)

        if not data:
            metrics.item_failed('pws_extraction', f"All {MAX_RETRIES} retries exhausted",
                               item_type='data_extraction')
            logger.error("Failed to extract valid data after all retries")
            metrics.set_exit_code(1)
        else:
            logger.info(f"Extracted {data['tempCount']} temps from {data['markerCount']} markers")
            logger.info(f"Range: {data['minTemp']}°F to {data['maxTemp']}°F, "
                        f"Avg: {data['avgTemp']:.1f}°F")

            if args.test:
                logger.info("[Test mode - not saving to database]")
                print(f"Sample temps: {data['temps'][:20]}")
                metrics.item_succeeded('pws_extraction', records_inserted=0,
                                       item_type='data_extraction')
                metrics.add_note("Test mode - data not saved")
            else:
                if save_to_database(data):
                    metrics.item_succeeded('pws_extraction', records_inserted=1,
                                           item_type='data_extraction')
                    logger.info("Capture complete")
                else:
                    metrics.item_failed('pws_extraction', "Database save failed",
                                       item_type='data_extraction')
                    logger.error("Failed to save to database")
                    metrics.set_exit_code(1)

    return metrics.exit_code


if __name__ == '__main__':
    sys.exit(main())
