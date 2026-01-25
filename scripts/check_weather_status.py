#!/usr/bin/env python3
"""
Weather Script Status Monitor

View metrics and status for all weather collection scripts.

Usage:
    python check_weather_status.py                    # Last 24 hours summary
    python check_weather_status.py --failures         # Show only failures
    python check_weather_status.py --script gfs_logger  # Filter by script
    python check_weather_status.py --hours 48         # Custom time window
    python check_weather_status.py --details RUN_ID   # Show run details
    python check_weather_status.py --reliability      # 7-day reliability report
"""

import argparse
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Configuration
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "weather_data"
DB_PATH = DATA_DIR / "weather.db"


def get_connection() -> sqlite3.Connection:
    """Create a database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def format_duration(start_str: str, end_str: str) -> str:
    """Format the duration between two ISO timestamps."""
    if not start_str or not end_str:
        return "N/A"
    try:
        start = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
        end = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
        delta = end - start
        seconds = delta.total_seconds()
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            return f"{seconds/60:.1f}m"
        else:
            return f"{seconds/3600:.1f}h"
    except (ValueError, TypeError):
        return "N/A"


def format_time(iso_str: str) -> str:
    """Format ISO timestamp for display."""
    if not iso_str:
        return "N/A"
    try:
        dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
        # Convert to local time for display
        local_dt = dt.astimezone()
        return local_dt.strftime("%Y-%m-%d %H:%M")
    except (ValueError, TypeError):
        return iso_str[:16] if iso_str else "N/A"


def status_icon(status: str) -> str:
    """Return status indicator."""
    icons = {
        "success": "[OK]",
        "partial": "[!!]",
        "failed": "[XX]",
        "running": "[..]"
    }
    return icons.get(status, "[??]")


def show_summary(hours: int = 24, script_filter: str = None, failures_only: bool = False):
    """Show summary of script runs in the specified time window."""
    conn = get_connection()
    cursor = conn.cursor()

    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    query = """
        SELECT run_id, script_name, start_time, end_time, status,
               total_items_expected, total_items_succeeded, total_items_failed,
               total_records_inserted, total_retries, model_run, error_message
        FROM script_runs
        WHERE start_time > ?
    """
    params = [cutoff]

    if script_filter:
        query += " AND script_name LIKE ?"
        params.append(f"%{script_filter}%")

    if failures_only:
        query += " AND status IN ('failed', 'partial')"

    query += " ORDER BY start_time DESC"

    cursor.execute(query, params)
    runs = cursor.fetchall()
    conn.close()

    if not runs:
        print(f"\nNo runs found in the last {hours} hours")
        if script_filter:
            print(f"(filtered by script: {script_filter})")
        if failures_only:
            print("(showing failures only)")
        return

    print(f"\n{'='*80}")
    print(f" Weather Script Status - Last {hours} hours")
    if script_filter:
        print(f" Filtered by: {script_filter}")
    if failures_only:
        print(" Showing failures only")
    print(f"{'='*80}")

    for run in runs:
        items_info = ""
        if run['total_items_expected']:
            items_info = f"{run['total_items_succeeded']}/{run['total_items_expected']} items"
        elif run['total_items_succeeded'] or run['total_items_failed']:
            items_info = f"{run['total_items_succeeded']} ok, {run['total_items_failed']} fail"

        duration = format_duration(run['start_time'], run['end_time'])

        print(f"\n{status_icon(run['status'])} {run['script_name']}")
        print(f"     Run ID: {run['run_id']}")
        print(f"     Time:   {format_time(run['start_time'])} ({duration})")
        if items_info:
            print(f"     Items:  {items_info}")
        if run['total_records_inserted']:
            print(f"     Records: {run['total_records_inserted']} inserted")
        if run['total_retries']:
            print(f"     Retries: {run['total_retries']}")
        if run['model_run']:
            print(f"     Model:  {run['model_run']}")
        if run['error_message']:
            # Truncate long error messages
            error = run['error_message'][:100]
            if len(run['error_message']) > 100:
                error += "..."
            print(f"     Error:  {error}")

    print(f"\n{'='*80}")
    print(f" Total: {len(runs)} runs")
    success = sum(1 for r in runs if r['status'] == 'success')
    partial = sum(1 for r in runs if r['status'] == 'partial')
    failed = sum(1 for r in runs if r['status'] == 'failed')
    print(f" Success: {success} | Partial: {partial} | Failed: {failed}")
    print(f"{'='*80}\n")


def show_details(run_id: str):
    """Show detailed information for a specific run."""
    conn = get_connection()
    cursor = conn.cursor()

    # Get run info
    cursor.execute("""
        SELECT * FROM script_runs WHERE run_id = ?
    """, (run_id,))
    run = cursor.fetchone()

    if not run:
        # Try partial match
        cursor.execute("""
            SELECT * FROM script_runs WHERE run_id LIKE ?
            ORDER BY start_time DESC LIMIT 1
        """, (f"%{run_id}%",))
        run = cursor.fetchone()

    if not run:
        print(f"\nNo run found matching: {run_id}")
        conn.close()
        return

    print(f"\n{'='*80}")
    print(f" Run Details: {run['run_id']}")
    print(f"{'='*80}")

    print(f"\n Script:    {run['script_name']}")
    print(f" Status:    {status_icon(run['status'])} {run['status']}")
    print(f" Started:   {format_time(run['start_time'])}")
    print(f" Ended:     {format_time(run['end_time'])}")
    print(f" Duration:  {format_duration(run['start_time'], run['end_time'])}")
    if run['exit_code'] is not None:
        print(f" Exit Code: {run['exit_code']}")
    if run['model_run']:
        print(f" Model Run: {run['model_run']}")
    if run['notes']:
        print(f" Notes:     {run['notes']}")

    print(f"\n Items Expected:  {run['total_items_expected'] or 'N/A'}")
    print(f" Items Succeeded: {run['total_items_succeeded']}")
    print(f" Items Failed:    {run['total_items_failed']}")
    print(f" Records Inserted: {run['total_records_inserted']}")
    print(f" Total Retries:   {run['total_retries']}")

    if run['error_message']:
        print(f"\n Error Message:")
        print(f"   {run['error_message']}")

    if run['error_traceback']:
        print(f"\n Traceback:")
        for line in run['error_traceback'].split('\n')[:10]:
            print(f"   {line}")
        if run['error_traceback'].count('\n') > 10:
            print("   ... (truncated)")

    # Get items
    cursor.execute("""
        SELECT * FROM script_run_items WHERE run_id = ?
        ORDER BY start_time
    """, (run['run_id'],))
    items = cursor.fetchall()

    if items:
        print(f"\n{'-'*40}")
        print(f" Items ({len(items)})")
        print(f"{'-'*40}")

        for item in items:
            icon = "[OK]" if item['status'] == 'success' else "[XX]"
            print(f"\n {icon} {item['item_name']}")
            if item['item_type']:
                print(f"      Type: {item['item_type']}")
            print(f"      Duration: {format_duration(item['start_time'], item['end_time'])}")
            if item['records_inserted']:
                print(f"      Records: {item['records_inserted']}")
            if item['retry_count']:
                print(f"      Retries: {item['retry_count']}")
            if item['error_message']:
                print(f"      Error: {item['error_message'][:80]}")

    # Get retries
    cursor.execute("""
        SELECT * FROM script_retries WHERE run_id = ?
        ORDER BY attempt_time
    """, (run['run_id'],))
    retries = cursor.fetchall()

    if retries:
        print(f"\n{'-'*40}")
        print(f" Retries ({len(retries)})")
        print(f"{'-'*40}")

        for retry in retries:
            item_info = f" ({retry['item_name']})" if retry['item_name'] else ""
            print(f"\n Attempt #{retry['attempt_number']}{item_info}")
            print(f"      Time: {format_time(retry['attempt_time'])}")
            if retry['error_type']:
                print(f"      Type: {retry['error_type']}")
            if retry['error_message']:
                print(f"      Error: {retry['error_message'][:80]}")

    conn.close()
    print(f"\n{'='*80}\n")


def show_reliability(days: int = 7):
    """Show reliability report for all scripts over the specified period."""
    conn = get_connection()
    cursor = conn.cursor()

    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    cursor.execute("""
        SELECT script_name,
               COUNT(*) AS runs,
               SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successes,
               SUM(CASE WHEN status = 'partial' THEN 1 ELSE 0 END) AS partials,
               SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failures,
               SUM(total_records_inserted) AS total_records,
               SUM(total_retries) AS total_retries,
               MAX(start_time) AS last_run
        FROM script_runs
        WHERE start_time > ?
        GROUP BY script_name
        ORDER BY script_name
    """, (cutoff,))

    results = cursor.fetchall()
    conn.close()

    if not results:
        print(f"\nNo runs found in the last {days} days")
        return

    print(f"\n{'='*80}")
    print(f" Script Reliability Report - Last {days} Days")
    print(f"{'='*80}")
    print(f"\n {'Script':<25} {'Runs':>6} {'Success':>8} {'Rate':>7} {'Records':>10}")
    print(f" {'-'*25} {'-'*6} {'-'*8} {'-'*7} {'-'*10}")

    for row in results:
        success_rate = (row['successes'] / row['runs'] * 100) if row['runs'] > 0 else 0
        rate_str = f"{success_rate:.1f}%"

        # Add warning indicator for low success rates
        if success_rate < 80:
            rate_str = f"!{rate_str}"

        print(f" {row['script_name']:<25} {row['runs']:>6} {row['successes']:>8} "
              f"{rate_str:>7} {row['total_records'] or 0:>10}")

    print(f"\n Last runs:")
    for row in results:
        print(f"   {row['script_name']:<25} {format_time(row['last_run'])}")

    print(f"\n{'='*80}\n")


def show_recent_failures(hours: int = 24):
    """Show recent failures with error details."""
    conn = get_connection()
    cursor = conn.cursor()

    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    cursor.execute("""
        SELECT run_id, script_name, start_time, status, error_message,
               total_items_failed, total_retries
        FROM script_runs
        WHERE start_time > ?
          AND status IN ('failed', 'partial')
        ORDER BY start_time DESC
    """, (cutoff,))

    runs = cursor.fetchall()
    conn.close()

    if not runs:
        print(f"\n No failures in the last {hours} hours")
        return

    print(f"\n{'='*80}")
    print(f" Recent Failures - Last {hours} Hours")
    print(f"{'='*80}")

    for run in runs:
        print(f"\n{status_icon(run['status'])} {run['script_name']} ({run['run_id']})")
        print(f"   Time: {format_time(run['start_time'])}")
        if run['total_items_failed']:
            print(f"   Failed items: {run['total_items_failed']}")
        if run['total_retries']:
            print(f"   Retries: {run['total_retries']}")
        if run['error_message']:
            error = run['error_message'][:150]
            if len(run['error_message']) > 150:
                error += "..."
            print(f"   Error: {error}")

    print(f"\n{'='*80}")
    print(f" Total: {len(runs)} failed/partial runs")
    print(f"{'='*80}\n")


def main():
    parser = argparse.ArgumentParser(
        description='Check weather script status and metrics',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                       Show last 24 hours summary
  %(prog)s --hours 48            Show last 48 hours
  %(prog)s --failures            Show only failures
  %(prog)s --script gfs          Filter by script name
  %(prog)s --details abc123      Show details for run ID
  %(prog)s --reliability         Show 7-day reliability report
        """
    )

    parser.add_argument('--hours', type=int, default=24,
                        help='Time window in hours (default: 24)')
    parser.add_argument('--failures', action='store_true',
                        help='Show only failures and partial runs')
    parser.add_argument('--script', type=str,
                        help='Filter by script name (partial match)')
    parser.add_argument('--details', type=str, metavar='RUN_ID',
                        help='Show detailed info for a specific run')
    parser.add_argument('--reliability', action='store_true',
                        help='Show reliability report for last 7 days')
    parser.add_argument('--days', type=int, default=7,
                        help='Days for reliability report (default: 7)')

    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        return 1

    if args.details:
        show_details(args.details)
    elif args.reliability:
        show_reliability(args.days)
    elif args.failures:
        show_recent_failures(args.hours)
    else:
        show_summary(args.hours, args.script, args.failures)

    return 0


if __name__ == "__main__":
    exit(main())
