#!/usr/bin/env python3
"""
Full weather forecast script.

Provides comprehensive forecast including:
- Current conditions (METAR + PWS)
- Hourly forecast until midnight
- 3-day detailed forecast
- 7-day extended forecast
- 14-day outlook (CPC 8-14 day)
- 30-day outlook (CPC monthly)
"""

import sqlite3
from datetime import datetime
from typing import Optional

# ============================================================================
# Constants
# ============================================================================

DB_PATH = r"D:\Scripts\weather_data\weather.db"


# ============================================================================
# Low-level: Database utilities
# ============================================================================

def get_connection() -> sqlite3.Connection:
    """Create database connection with row factory."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ============================================================================
# Low-level: Query functions
# ============================================================================

def query_metar(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
    """Get most recent METAR observation."""
    return conn.execute("""
        SELECT observation_time,
               temperature_c * 9.0 / 5.0 + 32 AS temp_f,
               dewpoint_c * 9.0 / 5.0 + 32 AS dewpoint_f,
               wind_speed_kt,
               wind_direction_deg,
               sky_condition,
               weather_phenomena,
               altimeter_inhg
        FROM metar
        ORDER BY observation_time DESC
        LIMIT 1
    """).fetchone()


def query_pws(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
    """Get most recent PWS observation."""
    return conn.execute("""
        SELECT timestamp, temp_f, feels_like_f, humidity,
               wind_speed_mph, wind_gust_mph, pressure_rel_in
        FROM ambient_observations
        ORDER BY timestamp DESC
        LIMIT 1
    """).fetchone()


def query_hourly_today(conn: sqlite3.Connection) -> list:
    """Get all hourly forecast data for today (00:00 - 23:00)."""
    today = datetime.now().strftime("%Y-%m-%d")
    return conn.execute("""
        SELECT forecast_hour, temperature, dewpoint, wind_direction,
               wind_speed, sky_cover, precip_probability, relative_humidity
        FROM digital_forecast
        WHERE forecast_date = ?
        GROUP BY forecast_hour
        ORDER BY forecast_hour
    """, (today,)).fetchall()


def query_daily_summary(conn: sqlite3.Connection, days: int) -> list:
    """Get daily high/low summary for specified number of days."""
    return conn.execute("""
        SELECT forecast_date,
               MAX(temperature) AS high,
               MIN(temperature) AS low,
               ROUND(AVG(sky_cover)) AS avg_sky,
               MAX(precip_probability) AS max_pop,
               ROUND(AVG(wind_speed)) AS avg_wind
        FROM digital_forecast
        WHERE forecast_date >= date('now', 'localtime')
        GROUP BY forecast_date
        ORDER BY forecast_date
        LIMIT ?
    """, (days,)).fetchall()


def query_cpc_outlook(conn: sqlite3.Connection, outlook_type: str) -> Optional[sqlite3.Row]:
    """Get most recent CPC outlook by type."""
    return conn.execute("""
        SELECT issued_date, valid_start, valid_end, discussion
        FROM cpc_outlooks
        WHERE outlook_type = ?
        ORDER BY fetch_time DESC
        LIMIT 1
    """, (outlook_type,)).fetchone()


# ============================================================================
# Mid-level: Formatting functions
# ============================================================================

def degrees_to_cardinal(degrees: Optional[int]) -> str:
    """Convert wind direction in degrees to cardinal direction."""
    if degrees is None:
        return "VRB"
    directions = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
                  "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    index = round(degrees / 22.5) % 16
    return directions[index]


def sky_description(cover: int) -> str:
    """Convert sky cover percentage to description."""
    if cover <= 6:
        return "Clear"
    if cover <= 25:
        return "Mostly Clear"
    if cover <= 50:
        return "Partly Cloudy"
    if cover <= 87:
        return "Mostly Cloudy"
    return "Overcast"


def format_wind(direction: str, speed: int) -> str:
    """Format wind direction and speed."""
    if speed == 0:
        return "Calm"
    return f"{direction} {speed} mph"


def format_hourly_row(row: sqlite3.Row) -> str:
    """Format a single hourly forecast row."""
    hour = row["forecast_hour"]
    time_str = f"{hour:02d}:00"
    temp = row["temperature"]
    sky = sky_description(row["sky_cover"])
    wind = format_wind(row["wind_direction"], row["wind_speed"])
    pop = row["precip_probability"]
    pop_str = f" | POP {pop}%" if pop else ""
    return f"  {time_str}  {temp:3d}°F  {sky:<14} {wind:<12}{pop_str}"


def format_daily_row(row: sqlite3.Row) -> str:
    """Format a single daily forecast row."""
    date_str = row["forecast_date"]
    high = row["high"]
    low = row["low"]
    sky = sky_description(row["avg_sky"])
    pop = row["max_pop"]
    wind = row["avg_wind"]
    pop_str = f" | POP {pop}%" if pop else ""
    return f"  {date_str}  High {high:3d}°F  Low {low:3d}°F  {sky:<14} Wind ~{wind} mph{pop_str}"


# ============================================================================
# High-level: Print sections
# ============================================================================

def print_separator(title: str) -> None:
    """Print a section separator with title."""
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print("=" * 60)


def print_current_conditions(conn: sqlite3.Connection) -> None:
    """Print current conditions from METAR and PWS."""
    print_separator("CURRENT CONDITIONS")

    metar = query_metar(conn)
    if metar:
        weather = metar["weather_phenomena"] or "Clear"
        wind_dir = degrees_to_cardinal(metar["wind_direction_deg"])
        print(f"\n  METAR (KCOS) - {metar['observation_time']}")
        print(f"    Temperature: {metar['temp_f']:.0f}°F (Dewpoint {metar['dewpoint_f']:.0f}°F)")
        print(f"    Wind: {wind_dir} at {metar['wind_speed_kt']} kt")
        print(f"    Sky: {metar['sky_condition']}")
        print(f"    Weather: {weather}")
        print(f"    Pressure: {metar['altimeter_inhg']:.2f} inHg")

    pws = query_pws(conn)
    if pws:
        print(f"\n  Your PWS - {pws['timestamp']}")
        print(f"    Temperature: {pws['temp_f']:.1f}°F (Feels like {pws['feels_like_f']:.1f}°F)")
        print(f"    Humidity: {pws['humidity']}%")
        print(f"    Wind: {pws['wind_speed_mph']:.1f} mph (Gusts {pws['wind_gust_mph']:.1f} mph)")
        print(f"    Pressure: {pws['pressure_rel_in']:.2f} inHg")


def print_hourly_today(conn: sqlite3.Connection) -> None:
    """Print all hourly forecast data for today."""
    print_separator("HOURLY FORECAST (Today - Full Day)")

    rows = query_hourly_today(conn)
    if not rows:
        print("\n  No hourly data available for today.")
        return

    print(f"\n  {'Time':<8}{'Temp':<8}{'Sky':<16}{'Wind':<14}{'Precip'}")
    print("  " + "-" * 54)
    for row in rows:
        print(format_hourly_row(row))


def print_3_day_forecast(conn: sqlite3.Connection) -> None:
    """Print 3-day detailed forecast."""
    print_separator("3-DAY FORECAST")

    rows = query_daily_summary(conn, 3)
    if not rows:
        print("\n  No forecast data available.")
        return

    for row in rows:
        print(format_daily_row(row))


def print_7_day_forecast(conn: sqlite3.Connection) -> None:
    """Print 7-day extended forecast."""
    print_separator("7-DAY EXTENDED FORECAST")

    rows = query_daily_summary(conn, 7)
    if not rows:
        print("\n  No forecast data available.")
        return

    for row in rows:
        print(format_daily_row(row))


def print_14_day_outlook(conn: sqlite3.Connection) -> None:
    """Print CPC 8-14 day outlook."""
    print_separator("14-DAY OUTLOOK (CPC 8-14 Day)")

    outlook = query_cpc_outlook(conn, "8_14_day")
    if not outlook:
        print("\n  No 8-14 day outlook available.")
        return

    print(f"\n  Issued: {outlook['issued_date']}")
    if outlook["valid_start"] and outlook["valid_end"]:
        print(f"  Valid:  {outlook['valid_start']} to {outlook['valid_end']}")
    print()
    print(outlook["discussion"])


def print_30_day_outlook(conn: sqlite3.Connection) -> None:
    """Print CPC 30-day (monthly) outlook."""
    print_separator("30-DAY OUTLOOK (CPC Monthly)")

    outlook = query_cpc_outlook(conn, "monthly")
    if not outlook:
        print("\n  No monthly outlook available.")
        return

    print(f"\n  Issued: {outlook['issued_date']}")
    if outlook["valid_start"] and outlook["valid_end"]:
        print(f"  Valid:  {outlook['valid_start']} to {outlook['valid_end']}")
    print()
    print(outlook["discussion"])


# ============================================================================
# Entry Point
# ============================================================================

def main() -> None:
    """Run full forecast report."""
    conn = get_connection()
    try:
        print_current_conditions(conn)
        print_hourly_today(conn)
        print_3_day_forecast(conn)
        print_7_day_forecast(conn)
        print_14_day_outlook(conn)
        print_30_day_outlook(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
