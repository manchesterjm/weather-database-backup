#!/usr/bin/env python3
"""
Weather Database Integrity Checker

Checks the weather database for:
- File integrity (SQLite PRAGMA integrity_check)
- Table row counts
- Recent data freshness for all loggers
- Data gaps and anomalies

Usage:
    python D:/Scripts/check_weather_db.py           # Standard check
    python D:/Scripts/check_weather_db.py --verbose # Show all tables
    python D:/Scripts/check_weather_db.py --json    # JSON output

Author: Claude (for Josh Manchester)
Created: 2026-02-01
"""

import sqlite3
import sys
import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path

import tz_utils

# Configuration
DB_PATH = Path(r"D:\Scripts\weather_data\weather.db")
METRICS_DB_PATH = Path(r"D:\Scripts\weather_data\metrics.db")

# Tables to check for freshness (table, time_column, description, max_age_hours)
# All timestamps are now stored as UTC (with Z suffix or +00:00)
FRESHNESS_CHECKS = [
    ("metar", "observation_time", "METAR observations", 2),
    ("observations", "fetch_time", "NWS observations", 4),
    ("forecast_snapshots", "fetch_time", "Forecast snapshots", 4),
    ("hourly_snapshots", "fetch_time", "Hourly snapshots", 4),
    ("ambient_observations", "timestamp", "Ambient Weather", 1),
    ("nest_observations", "timestamp", "Nest thermostat", 1),
    ("gfs_forecasts", "fetch_time", "GFS forecasts", 8),
    ("nbm_forecasts", "fetch_time", "NBM forecasts", 8),
    ("cpc_outlooks", "fetch_time", "CPC outlooks", 26),
]

# Table categories for summary display
TABLE_CATEGORIES = {
    "Observations": [
        "metar", "observations", "ambient_observations", "nest_observations",
        "pws_observations", "pws_nearby_observations"
    ],
    "Forecasts": [
        "forecast_snapshots", "hourly_snapshots", "digital_forecast",
        "gfs_forecasts", "nbm_forecasts", "cpc_outlooks"
    ],
    "Climate Data": ["actual_daily_climate", "actual_snowfall"],
    "Alerts": ["alerts"],
    "Station Info": ["pws_station_data", "pws_nearby_stations"],
}


def get_db_size(path: Path) -> float:
    """Return database size in MB."""
    if path.exists():
        return path.stat().st_size / (1024 * 1024)
    return 0


def check_integrity(cursor) -> tuple[bool, str]:
    """Run SQLite integrity check."""
    result = cursor.execute("PRAGMA integrity_check").fetchone()[0]
    return result == "ok", result


def get_table_counts(cursor) -> dict:
    """Get row counts for all tables."""
    tables = cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()

    counts = {}
    for (table,) in tables:
        if table.startswith("sqlite_"):
            continue
        count = cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        counts[table] = count

    return counts


def parse_timestamp_to_local(ts_str: str) -> datetime | None:
    """
    Parse various timestamp formats and return as local datetime.

    Uses tz_utils for proper timezone handling including DST.

    Args:
        ts_str: Timestamp string (UTC or with offset)

    Returns:
        Local datetime object, or None if parsing fails
    """
    local_dt = tz_utils.to_local(ts_str)
    if local_dt is None:
        return None
    # Return naive datetime for comparison with datetime.now()
    return local_dt.replace(tzinfo=None)


def check_freshness(cursor) -> list[dict]:
    """Check data freshness for each logger."""
    now = datetime.now()
    results = []

    for table, time_col, desc, max_age in FRESHNESS_CHECKS:
        try:
            row = cursor.execute(
                f"SELECT MAX({time_col}) FROM {table}"
            ).fetchone()

            if row and row[0]:
                last_time = parse_timestamp_to_local(row[0])
                if last_time:
                    age = now - last_time
                    hours_ago = age.total_seconds() / 3600

                    # Handle negative ages (shouldn't happen but just in case)
                    hours_ago = max(hours_ago, 0)

                    if hours_ago <= max_age:
                        status = "OK"
                    elif hours_ago <= max_age * 2:
                        status = "WARN"
                    else:
                        status = "STALE"

                    # Format for display with local time
                    last_update_display = tz_utils.format_local_datetime(row[0])

                    results.append({
                        "table": table,
                        "description": desc,
                        "last_update": last_update_display,
                        "hours_ago": round(hours_ago, 1),
                        "max_age": max_age,
                        "status": status,
                    })
                else:
                    results.append({
                        "table": table,
                        "description": desc,
                        "last_update": row[0],
                        "hours_ago": None,
                        "max_age": max_age,
                        "status": "PARSE_ERROR",
                    })
            else:
                results.append({
                    "table": table,
                    "description": desc,
                    "last_update": None,
                    "hours_ago": None,
                    "max_age": max_age,
                    "status": "NO_DATA",
                })
        except sqlite3.Error as e:
            results.append({
                "table": table,
                "description": desc,
                "last_update": None,
                "hours_ago": None,
                "max_age": max_age,
                "status": f"ERROR: {e}",
            })

    return results


def get_today_counts(cursor) -> dict:
    """Get record counts for today."""
    today = datetime.now().strftime("%Y-%m-%d")

    tables = [
        ("metar", "observation_time"),
        ("observations", "fetch_time"),
        ("hourly_snapshots", "fetch_time"),
        ("forecast_snapshots", "fetch_time"),
        ("ambient_observations", "timestamp"),
        ("nest_observations", "timestamp"),
    ]

    counts = {}
    for table, time_col in tables:
        try:
            count = cursor.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {time_col} LIKE ?",
                (f"{today}%",)
            ).fetchone()[0]
            counts[table] = count
        except sqlite3.Error:
            counts[table] = -1

    return counts


def get_journal_mode(cursor) -> str:
    """Get SQLite journal mode."""
    return cursor.execute("PRAGMA journal_mode").fetchone()[0]


def check_data_gaps(cursor) -> list[dict]:
    """
    Detect gaps in time-series data that exceed expected intervals.

    Returns list of gap info dicts with table, gap_start, gap_end, gap_hours.
    """
    # Tables with expected update intervals (table, time_col, max_gap_hours)
    gap_checks = [
        ("metar", "observation_time", 2),           # Hourly, allow 2h gap
        ("ambient_observations", "timestamp", 0.5), # Every 5min, allow 30min gap
        ("nest_observations", "timestamp", 0.5),    # Every 5min, allow 30min gap
        ("hourly_snapshots", "fetch_time", 4),      # Every 3h, allow 4h gap
    ]

    gaps = []
    # Check last 7 days for gaps
    cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    for table, time_col, max_gap_hours in gap_checks:
        try:
            # Get timestamps ordered
            rows = cursor.execute(f"""
                SELECT {time_col} FROM {table}
                WHERE {time_col} > ?
                ORDER BY {time_col}
            """, (cutoff,)).fetchall()

            if len(rows) < 2:
                continue

            for i in range(1, len(rows)):
                prev_ts = rows[i-1][0]
                curr_ts = rows[i][0]

                prev_dt = parse_timestamp_to_local(prev_ts)
                curr_dt = parse_timestamp_to_local(curr_ts)

                if prev_dt and curr_dt:
                    gap_hours = (curr_dt - prev_dt).total_seconds() / 3600
                    if gap_hours > max_gap_hours:
                        gaps.append({
                            "table": table,
                            "gap_start": tz_utils.format_local_datetime(prev_ts),
                            "gap_end": tz_utils.format_local_datetime(curr_ts),
                            "gap_hours": round(gap_hours, 1),
                            "expected_max": max_gap_hours,
                        })

        except sqlite3.Error:
            continue

    # Sort by gap size descending, limit to top 10
    gaps.sort(key=lambda x: x["gap_hours"], reverse=True)
    return gaps[:10]


def check_timestamp_format(cursor) -> dict:
    """
    Verify timestamps are in UTC format with Z suffix.

    Returns dict with table stats: total checked, valid (Z suffix), invalid.
    """
    # Tables and their timestamp columns to check
    ts_checks = [
        ("metar", "observation_time"),
        ("metar", "fetch_time"),
        ("ambient_observations", "timestamp"),
        ("nest_observations", "timestamp"),
        ("forecast_snapshots", "fetch_time"),
        ("hourly_snapshots", "fetch_time"),
        ("gfs_forecasts", "fetch_time"),
        ("nbm_forecasts", "fetch_time"),
        ("cpc_outlooks", "fetch_time"),
    ]

    results = {}
    total_checked = 0
    total_valid = 0
    total_invalid = 0
    invalid_samples = []

    for table, col in ts_checks:
        try:
            # Sample recent timestamps (last 100)
            rows = cursor.execute(f"""
                SELECT {col} FROM {table}
                WHERE {col} IS NOT NULL
                ORDER BY {col} DESC
                LIMIT 100
            """).fetchall()

            valid = 0
            invalid = 0
            for (ts,) in rows:
                if ts and (ts.endswith('Z') or '+00:00' in ts):
                    valid += 1
                else:
                    invalid += 1
                    if len(invalid_samples) < 5:
                        invalid_samples.append(f"{table}.{col}: {ts}")

            results[f"{table}.{col}"] = {"valid": valid, "invalid": invalid}
            total_checked += valid + invalid
            total_valid += valid
            total_invalid += invalid

        except sqlite3.Error:
            continue

    return {
        "total_checked": total_checked,
        "total_valid": total_valid,
        "total_invalid": total_invalid,
        "pct_valid": round(100 * total_valid / total_checked, 1) if total_checked > 0 else 0,
        "invalid_samples": invalid_samples,
        "by_column": results,
    }


def check_metar_station_health(cursor) -> dict:
    """
    Check METAR station health - observation counts and gaps.

    Returns dict with station stats and any deficient stations.
    """
    # Get observation counts per station for last 3 days
    try:
        rows = cursor.execute("""
            SELECT station_id,
                   COUNT(*) as obs_count,
                   MAX(observation_time) as last_obs
            FROM metar
            WHERE observation_time > datetime('now', '-3 days')
            GROUP BY station_id
            ORDER BY station_id
        """).fetchall()

        if not rows:
            return {"stations": [], "deficient": [], "median_count": 0}

        stations = []
        counts = []
        for row in rows:
            last_obs_display = tz_utils.format_local_datetime(row[2]) if row[2] else "N/A"
            stations.append({
                "station": row[0],
                "count": row[1],
                "last_obs": last_obs_display,
            })
            counts.append(row[1])

        # Calculate median to identify deficient stations
        counts_sorted = sorted(counts)
        median_count = counts_sorted[len(counts_sorted) // 2]

        # Flag stations with <75% of median observations
        threshold = median_count * 0.75
        deficient = [s["station"] for s in stations if s["count"] < threshold]

        return {
            "stations": stations,
            "deficient": deficient,
            "median_count": median_count,
            "threshold": threshold,
        }

    except sqlite3.Error:
        return {"stations": [], "deficient": [], "median_count": 0}


def check_metrics_db(metrics_db_path: Path = None) -> dict:
    """
    Check metrics database integrity and stats.

    Args:
        metrics_db_path: Path to metrics database (defaults to METRICS_DB_PATH)

    Returns dict with integrity status, size, and table counts.
    """
    db_path = metrics_db_path or METRICS_DB_PATH

    if not db_path.exists():
        return {
            "exists": False,
            "size_mb": 0,
            "integrity_ok": None,
            "table_counts": {},
        }

    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        # Integrity check
        result = cursor.execute("PRAGMA integrity_check").fetchone()[0]
        integrity_ok = result == "ok"

        # Table counts
        tables = cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()

        table_counts = {}
        for (table,) in tables:
            if table.startswith("sqlite_"):
                continue
            count = cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            table_counts[table] = count

        conn.close()

        return {
            "exists": True,
            "size_mb": get_db_size(db_path),
            "integrity_ok": integrity_ok,
            "integrity_result": result,
            "table_counts": table_counts,
            "total_records": sum(table_counts.values()),
        }

    except sqlite3.Error as e:
        return {
            "exists": True,
            "size_mb": get_db_size(db_path),
            "integrity_ok": False,
            "integrity_result": str(e),
            "table_counts": {},
        }


def check_recent_script_failures(metrics_db_path: Path = None) -> list[dict]:
    """
    Check script_runs table in metrics.db for recent failures.

    Args:
        metrics_db_path: Path to metrics database (defaults to METRICS_DB_PATH)

    Returns list of failed/partial runs in last 48 hours.
    """
    db_path = metrics_db_path or METRICS_DB_PATH

    if not db_path.exists():
        return []

    cutoff = (datetime.now() - timedelta(hours=48)).strftime("%Y-%m-%d")

    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        rows = cursor.execute("""
            SELECT script_name, start_time, status, error_message,
                   total_items_succeeded, total_items_failed
            FROM script_runs
            WHERE start_time > ?
              AND status IN ('failed', 'partial')
            ORDER BY start_time DESC
            LIMIT 10
        """, (cutoff,)).fetchall()

        failures = []
        for row in rows:
            failures.append({
                "script": row[0],
                "time": tz_utils.format_local_datetime(row[1]) if row[1] else "N/A",
                "status": row[2],
                "error": (row[3][:50] + "...") if row[3] and len(row[3]) > 50 else row[3],
                "succeeded": row[4] or 0,
                "failed": row[5] or 0,
            })

        conn.close()
        return failures

    except sqlite3.Error:
        return []


def get_categorized_counts(table_counts: dict) -> dict:
    """Group table counts by category."""
    categorized = {}
    uncategorized_tables = set(table_counts.keys())

    for category, tables in TABLE_CATEGORIES.items():
        category_total = 0
        category_tables = {}
        for table in tables:
            if table in table_counts:
                category_tables[table] = table_counts[table]
                category_total += table_counts[table]
                uncategorized_tables.discard(table)
        if category_tables:
            categorized[category] = {
                "total": category_total,
                "tables": category_tables
            }

    # Handle any uncategorized tables
    if uncategorized_tables:
        other_total = sum(table_counts[t] for t in uncategorized_tables)
        categorized["Other"] = {
            "total": other_total,
            "tables": {t: table_counts[t] for t in uncategorized_tables}
        }

    return categorized


_CONFIG = {"use_color": False}  # Set via --color flag


def supports_color() -> bool:
    """Check if colors are enabled."""
    return _CONFIG["use_color"]


def color(text: str, code: str) -> str:
    """Apply ANSI color if supported."""
    if not supports_color():
        return text
    colors = {
        "green": "\033[92m",
        "yellow": "\033[93m",
        "red": "\033[91m",
        "reset": "\033[0m",
    }
    return f"{colors.get(code, '')}{text}{colors['reset']}"


def print_data_gaps(gaps: list[dict]):
    """Print data gaps section."""
    print("\n" + "-" * 60)
    print("DATA GAPS (Last 7 Days)")
    print("-" * 60)

    if not gaps:
        print("No significant data gaps detected.")
        return

    print(f"{'Table':<25} {'Gap Start':<18} {'Gap End':<18} {'Hours':>6}")
    print("-" * 60)
    for gap in gaps:
        print(f"{gap['table']:<25} {gap['gap_start']:<18} {gap['gap_end']:<18} {gap['gap_hours']:>6}")


def print_timestamp_format(ts_data: dict):
    """Print timestamp format validation section."""
    print("\n" + "-" * 60)
    print("TIMESTAMP FORMAT VALIDATION")
    print("-" * 60)

    pct = ts_data['pct_valid']
    total = ts_data['total_checked']
    invalid = ts_data['total_invalid']

    if pct == 100:
        status = color("ALL VALID", "green")
    elif pct >= 95:
        status = color(f"{pct}% valid", "yellow")
    else:
        status = color(f"{pct}% valid", "red")

    print(f"Checked {total} timestamps: {status}")

    if invalid > 0:
        print(f"\nInvalid samples (missing Z suffix):")
        for sample in ts_data['invalid_samples']:
            print(f"  - {sample}")


def print_metar_health(metar_data: dict):
    """Print METAR station health section."""
    print("\n" + "-" * 60)
    print("METAR STATION HEALTH (Last 3 Days)")
    print("-" * 60)

    if not metar_data.get("stations"):
        print("No METAR data available.")
        return

    print(f"{'Station':<8} {'Count':>6} {'Last Observation':<20} {'Status':>10}")
    print("-" * 60)

    for s in metar_data["stations"]:
        if s["station"] in metar_data.get("deficient", []):
            status = color("DEFICIENT", "yellow")
        else:
            status = color("OK", "green")
        print(f"{s['station']:<8} {s['count']:>6} {s['last_obs']:<20} {status:>10}")

    if metar_data.get("deficient"):
        print(f"\nDeficient threshold: <{metar_data['threshold']:.0f} obs "
              f"(75% of median {metar_data['median_count']})")


def print_script_failures(failures: list[dict]):
    """Print recent script failures section."""
    print("\n" + "-" * 60)
    print("RECENT SCRIPT FAILURES (Last 48 Hours)")
    print("-" * 60)

    if not failures:
        print(color("No failures in last 48 hours.", "green"))
        return

    for f in failures:
        status_color = "red" if f['status'] == 'failed' else "yellow"
        print(f"\n{color(f['status'].upper(), status_color)} {f['script']}")
        print(f"  Time: {f['time']}")
        if f['succeeded'] or f['failed']:
            print(f"  Items: {f['succeeded']} ok, {f['failed']} failed")
        if f['error']:
            print(f"  Error: {f['error']}")


def print_metrics_db(metrics_data: dict):
    """Print metrics database status section."""
    print("\n" + "-" * 60)
    print("METRICS DATABASE")
    print("-" * 60)

    if not metrics_data.get("exists"):
        print(color("metrics.db not found (will be created on first script run)", "yellow"))
        return

    size = metrics_data.get("size_mb", 0)
    integrity_ok = metrics_data.get("integrity_ok", False)
    status = color("[OK]", "green") if integrity_ok else color("[FAIL]", "red")

    print(f"Path: {METRICS_DB_PATH}")
    print(f"Size: {size:.2f} MB")
    print(f"Integrity: {status} {metrics_data.get('integrity_result', 'unknown')}")

    if metrics_data.get("table_counts"):
        print(f"\n{'Table':<25} {'Records':>12}")
        print("-" * 40)
        for table, count in sorted(metrics_data["table_counts"].items()):
            print(f"{table:<25} {count:>12,}")


def print_db_summary(data: dict):
    """Print database summary section."""
    categorized = get_categorized_counts(data['table_counts'])

    print("\n" + "-" * 60)
    print("DATABASE SUMMARY")
    print("-" * 60)
    print(f"{'Total Records:':<20} {data['total_records']:>12,}")
    print(f"{'Total Tables:':<20} {len(data['table_counts']):>12}")
    print()
    print(f"{'Category':<25} {'Records':>12}  {'Tables':>8}")
    print("-" * 60)
    for category, info in categorized.items():
        print(f"{category:<25} {info['total']:>12,}  {len(info['tables']):>8}")


def print_report(data: dict, verbose: bool = False):
    """Print formatted report."""
    print("=" * 60)
    print("WEATHER DATABASE INTEGRITY REPORT")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Database info
    print(f"\nDatabase: {data['db_path']}")
    print(f"Size: {data['size_mb']:.2f} MB")
    print(f"Journal mode: {data['journal_mode']}")

    # Integrity check
    status_icon = "[OK]" if data['integrity_ok'] else "[FAIL]"
    print(f"\nIntegrity check: {status_icon} {data['integrity_result']}")

    # Database summary
    print_db_summary(data)

    # Freshness check
    print("\n" + "-" * 60)
    print("DATA FRESHNESS")
    print("-" * 60)
    print(f"{'Source':<25} {'Last Update':<20} {'Age':>8} {'Status':>8}")
    print("-" * 60)

    issues = []
    for item in data['freshness']:
        last = item['last_update'] or 'N/A'
        age = f"{item['hours_ago']}h" if item['hours_ago'] is not None else "N/A"
        status = item['status']

        if status == "OK":
            status_str = color(f"{status:>8}", "green")
        elif status == "WARN":
            status_str = color(f"{status:>8}", "yellow")
            issues.append(f"{item['description']}: {age} old (max {item['max_age']}h)")
        else:
            status_str = color(f"{status:>8}", "red")
            issues.append(f"{item['description']}: {status}")

        print(f"{item['description']:<25} {last:<20} {age:>8} {status_str}")

    # Today's counts
    print("\n" + "-" * 60)
    print("TODAY'S RECORD COUNTS")
    print("-" * 60)
    for table, count in data['today_counts'].items():
        print(f"{table:<25} {count:>6} records")

    # Data gaps
    if 'data_gaps' in data:
        print_data_gaps(data['data_gaps'])

    # Timestamp format validation
    if 'timestamp_format' in data:
        print_timestamp_format(data['timestamp_format'])

    # Recent script failures
    if 'script_failures' in data:
        print_script_failures(data['script_failures'])

    # METAR station health
    if 'metar_health' in data:
        print_metar_health(data['metar_health'])

    # Metrics database
    if 'metrics_db' in data:
        print_metrics_db(data['metrics_db'])

    # Table counts (verbose)
    if verbose:
        print("\n" + "-" * 60)
        print("ALL TABLE COUNTS")
        print("-" * 60)
        for table, count in sorted(data['table_counts'].items()):
            print(f"{table:<30} {count:>10,} rows")

    # Add issues from new checks
    if 'data_gaps' in data and data['data_gaps']:
        issues.append(f"{len(data['data_gaps'])} data gap(s) detected")

    if 'timestamp_format' in data and data['timestamp_format']['total_invalid'] > 0:
        issues.append(f"{data['timestamp_format']['total_invalid']} timestamps missing Z suffix")

    if 'script_failures' in data and data['script_failures']:
        failed_count = len(data['script_failures'])
        issues.append(f"{failed_count} script failure(s) in last 48h")

    if 'metar_health' in data and data['metar_health'].get('deficient'):
        deficient = data['metar_health']['deficient']
        issues.append(f"METAR deficient: {', '.join(deficient)}")

    # Summary
    print("\n" + "=" * 60)
    if data['integrity_ok'] and not issues:
        print(color("STATUS: ALL CHECKS PASSED", "green"))
    elif data['integrity_ok']:
        print(color(f"STATUS: {len(issues)} WARNING(S)", "yellow"))
        for issue in issues:
            print(f"  - {issue}")
    else:
        print(color("STATUS: INTEGRITY CHECK FAILED", "red"))
    print("=" * 60)


def main():
    """Entry point for the weather database integrity checker."""
    parser = argparse.ArgumentParser(description="Check weather database integrity")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show all table counts")
    parser.add_argument("--json", "-j", action="store_true", help="Output as JSON")
    parser.add_argument("--color", "-c", action="store_true", help="Enable ANSI colors")
    parser.add_argument("--db", type=str, default=str(DB_PATH), help="Database path")
    args = parser.parse_args()

    _CONFIG["use_color"] = args.color

    db_path = Path(args.db)

    # Check file exists
    if not db_path.exists():
        print(f"ERROR: Database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    # Connect and run checks
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    try:
        integrity_ok, integrity_result = check_integrity(cursor)

        table_counts = get_table_counts(cursor)
        data = {
            "timestamp": datetime.now().isoformat(),
            "db_path": str(db_path),
            "size_mb": get_db_size(db_path),
            "journal_mode": get_journal_mode(cursor),
            "integrity_ok": integrity_ok,
            "integrity_result": integrity_result,
            "total_records": sum(table_counts.values()),
            "table_counts": table_counts,
            "categorized_counts": get_categorized_counts(table_counts),
            "freshness": check_freshness(cursor),
            "today_counts": get_today_counts(cursor),
            "data_gaps": check_data_gaps(cursor),
            "timestamp_format": check_timestamp_format(cursor),
            "script_failures": check_recent_script_failures(),
            "metar_health": check_metar_station_health(cursor),
            "metrics_db": check_metrics_db(),
        }

        if args.json:
            print(json.dumps(data, indent=2))
        else:
            print_report(data, verbose=args.verbose)

        # Exit code based on status
        if not integrity_ok:
            sys.exit(2)
        elif any(f['status'] not in ('OK', 'WARN') for f in data['freshness']):
            sys.exit(1)
        else:
            sys.exit(0)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
