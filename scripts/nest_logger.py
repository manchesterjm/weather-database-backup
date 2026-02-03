#!/usr/bin/env python3
"""
Nest Thermostat Logger - Polls Nest API and stores data in weather database.
Runs every 5 minutes via scheduled task.
"""

import json
import requests
from datetime import datetime
from pathlib import Path

import db_utils
import tz_utils

# Paths - use db_utils for consistency
SCRIPTS_DIR = db_utils.SCRIPTS_DIR
CREDENTIALS_FILE = SCRIPTS_DIR / "nest_credentials.json"
DB_PATH = db_utils.DB_PATH
LOG_FILE = SCRIPTS_DIR / "nest_logger.log"

def log(message):
    """Log message to file and print."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

def load_credentials():
    """Load OAuth credentials from file."""
    with open(CREDENTIALS_FILE) as f:
        return json.load(f)

def save_credentials(creds):
    """Save updated credentials (refresh token may change)."""
    with open(CREDENTIALS_FILE, "w") as f:
        json.dump(creds, f, indent=2)

def get_access_token(creds):
    """Get fresh access token using refresh token."""
    response = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id": creds["client_id"],
            "client_secret": creds["client_secret"],
            "refresh_token": creds["refresh_token"],
            "grant_type": "refresh_token"
        }
    )
    response.raise_for_status()
    data = response.json()

    # Update refresh token if a new one was issued
    if "refresh_token" in data:
        creds["refresh_token"] = data["refresh_token"]
        save_credentials(creds)

    return data["access_token"]

def get_thermostat_data(access_token, project_id):
    """Fetch thermostat data from Nest API."""
    url = f"https://smartdevicemanagement.googleapis.com/v1/enterprises/{project_id}/devices"
    response = requests.get(url, headers={"Authorization": f"Bearer {access_token}"})
    response.raise_for_status()

    devices = response.json().get("devices", [])
    thermostats = [d for d in devices if d["type"] == "sdm.devices.types.THERMOSTAT"]

    return thermostats

def parse_thermostat(device):
    """Parse thermostat device into flat dict."""
    traits = device.get("traits", {})

    # Get parent room name
    parent_relations = device.get("parentRelations", [])
    room_name = parent_relations[0].get("displayName", "Unknown") if parent_relations else "Unknown"

    # Extract device ID from full name
    device_id = device["name"].split("/")[-1]

    # Temperature trait
    temp_trait = traits.get("sdm.devices.traits.Temperature", {})
    temp_c = temp_trait.get("ambientTemperatureCelsius")
    temp_f = temp_c * 9/5 + 32 if temp_c else None

    # Humidity trait
    humidity_trait = traits.get("sdm.devices.traits.Humidity", {})
    humidity = humidity_trait.get("ambientHumidityPercent")

    # Thermostat mode trait
    mode_trait = traits.get("sdm.devices.traits.ThermostatMode", {})
    mode = mode_trait.get("mode")

    # HVAC status trait
    hvac_trait = traits.get("sdm.devices.traits.ThermostatHvac", {})
    hvac_status = hvac_trait.get("status")

    # Setpoint trait
    setpoint_trait = traits.get("sdm.devices.traits.ThermostatTemperatureSetpoint", {})
    heat_setpoint_c = setpoint_trait.get("heatCelsius")
    cool_setpoint_c = setpoint_trait.get("coolCelsius")
    heat_setpoint_f = heat_setpoint_c * 9/5 + 32 if heat_setpoint_c else None
    cool_setpoint_f = cool_setpoint_c * 9/5 + 32 if cool_setpoint_c else None

    # Eco mode trait
    eco_trait = traits.get("sdm.devices.traits.ThermostatEco", {})
    eco_mode = eco_trait.get("mode")

    # Connectivity trait
    conn_trait = traits.get("sdm.devices.traits.Connectivity", {})
    connectivity = conn_trait.get("status")

    # Fan trait
    fan_trait = traits.get("sdm.devices.traits.Fan", {})
    fan_status = fan_trait.get("timerMode")

    return {
        "device_id": device_id,
        "room_name": room_name,
        "temperature_c": temp_c,
        "temperature_f": temp_f,
        "humidity": humidity,
        "mode": mode,
        "hvac_status": hvac_status,
        "heat_setpoint_c": heat_setpoint_c,
        "heat_setpoint_f": heat_setpoint_f,
        "cool_setpoint_c": cool_setpoint_c,
        "cool_setpoint_f": cool_setpoint_f,
        "eco_mode": eco_mode,
        "connectivity": connectivity,
        "fan_status": fan_status
    }

def init_database():
    """Create nest_observations table if it doesn't exist."""
    conn = db_utils.get_connection()

    def do_init(c):
        c.execute("""
            CREATE TABLE IF NOT EXISTS nest_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                fetch_time TEXT NOT NULL,
                device_id TEXT NOT NULL,
                room_name TEXT,
                temperature_c REAL,
                temperature_f REAL,
                humidity INTEGER,
                mode TEXT,
                hvac_status TEXT,
                heat_setpoint_c REAL,
                heat_setpoint_f REAL,
                cool_setpoint_c REAL,
                cool_setpoint_f REAL,
                eco_mode TEXT,
                connectivity TEXT,
                fan_status TEXT
            )
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_nest_timestamp ON nest_observations(timestamp)
        """)

    db_utils.execute_with_retry(do_init, conn, "init nest table")
    db_utils.commit_with_retry(conn, "commit nest init")
    conn.close()

def store_observation(data):
    """Store thermostat observation in database with retry logic."""
    conn = db_utils.get_connection()
    now = tz_utils.now_utc()

    def do_insert(c):
        c.execute("""
            INSERT INTO nest_observations (
                timestamp, fetch_time, device_id, room_name,
                temperature_c, temperature_f, humidity,
                mode, hvac_status, heat_setpoint_c, heat_setpoint_f,
                cool_setpoint_c, cool_setpoint_f, eco_mode, connectivity, fan_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            now, now, data["device_id"], data["room_name"],
            data["temperature_c"], data["temperature_f"], data["humidity"],
            data["mode"], data["hvac_status"], data["heat_setpoint_c"], data["heat_setpoint_f"],
            data["cool_setpoint_c"], data["cool_setpoint_f"], data["eco_mode"],
            data["connectivity"], data["fan_status"]
        ))

    db_utils.execute_with_retry(do_insert, conn, "insert nest observation")
    db_utils.commit_with_retry(conn, "commit nest observation")
    conn.close()

def main():
    try:
        # Initialize database
        init_database()

        # Load credentials and get access token
        creds = load_credentials()
        access_token = get_access_token(creds)

        # Fetch thermostat data
        thermostats = get_thermostat_data(access_token, creds["project_id"])

        if not thermostats:
            log("No thermostats found")
            return

        # Process each thermostat
        for device in thermostats:
            data = parse_thermostat(device)
            store_observation(data)
            log(f"{data['room_name']}: {data['temperature_f']:.1f}°F, {data['humidity']}% RH, "
                f"Mode={data['mode']}, HVAC={data['hvac_status']}, Setpoint={data['heat_setpoint_f']:.1f}°F")

        log(f"Logged {len(thermostats)} thermostat(s)")

    except Exception as e:
        log(f"ERROR: {e}")
        raise

if __name__ == "__main__":
    main()
