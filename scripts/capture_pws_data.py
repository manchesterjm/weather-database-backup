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
from datetime import datetime
import sys

# Configuration
DB_PATH = "/mnt/d/Scripts/weather_data/weather.db"
WUNDERMAP_URL = "https://www.wunderground.com/wundermap?lat=38.9194&lon=-104.7509&zoom=12"

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
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Fetching WunderMap data...")

    try:
        result = subprocess.run(
            ['shot-scraper', 'javascript', WUNDERMAP_URL, EXTRACT_JS],
            capture_output=True,
            text=True,
            timeout=90
        )

        if result.returncode != 0:
            print(f"Error: shot-scraper failed: {result.stderr}")
            return None

        data = json.loads(result.stdout)
        return data

    except subprocess.TimeoutExpired:
        print("Error: shot-scraper timed out")
        return None
    except json.JSONDecodeError as e:
        print(f"Error: Failed to parse JSON: {e}")
        print(f"Raw output: {result.stdout[:200]}")
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None


def save_to_database(data):
    """Save extracted PWS data to the weather database."""
    if not data or data.get('tempCount', 0) == 0:
        print("No temperature data to save")
        return False

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Round timestamp to the nearest hour for deduplication
        ts = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
        hour_ts = ts.replace(minute=0, second=0, microsecond=0).isoformat()

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

        conn.commit()
        conn.close()

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Saved: {data['tempCount']} stations, "
              f"temp range {data['minTemp']}-{data['maxTemp']}°F, avg {data['avgTemp']:.1f}°F")
        return True

    except sqlite3.IntegrityError:
        print(f"Data for {hour_ts} already exists")
        return False
    except Exception as e:
        print(f"Database error: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Capture PWS data from WunderMap')
    parser.add_argument('--test', action='store_true', help='Test extraction without saving')
    args = parser.parse_args()

    data = extract_pws_data()

    if not data:
        print("Failed to extract data")
        sys.exit(1)

    print(f"Extracted {data['tempCount']} temperatures from {data['markerCount']} markers")
    print(f"Range: {data['minTemp']}°F to {data['maxTemp']}°F")
    print(f"Average: {data['avgTemp']:.1f}°F")

    if args.test:
        print("\n[Test mode - not saving to database]")
        print(f"Sample temps: {data['temps'][:20]}")
    else:
        save_to_database(data)


if __name__ == '__main__':
    main()
