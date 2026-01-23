#!/usr/bin/env python3
"""
Weather Forecast Spaghetti Chart Visualization

Shows how forecasts for specific dates changed over time as new predictions were made.
This reveals forecast uncertainty and how models converge (or diverge) as the target
date approaches.

Usage:
    python weather_spaghetti.py                    # Temperature chart (default)
    python weather_spaghetti.py --metric snow     # Snow accumulation chart
    python weather_spaghetti.py --metric precip   # Precipitation probability chart
    python weather_spaghetti.py --save            # Save to file instead of display
"""

import sqlite3
import argparse
import re
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

# Database location
DB_PATH = Path(r"D:\Scripts\weather_data\weather.db")

# Color palette for different forecast vintages
COLORS = [
    '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
    '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf',
    '#aec7e8', '#ffbb78', '#98df8a', '#ff9896', '#c5b0d5'
]


def get_db_connection():
    """Connect to the weather database."""
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found: {DB_PATH}")
    return sqlite3.connect(str(DB_PATH))


def parse_fetch_time(fetch_time_str):
    """Parse fetch_time string to datetime, handling both ISO and space-separated formats."""
    # Truncate to minute precision and normalize separator
    fetch_key = fetch_time_str[:16].replace('T', ' ')
    return datetime.strptime(fetch_key, '%Y-%m-%d %H:%M')


def extract_snow_accumulation(detailed_forecast):
    """Extract snow accumulation range from detailed forecast text."""
    if not detailed_forecast:
        return None, None

    text = detailed_forecast.lower()

    # Look for accumulation patterns
    patterns = [
        r'accumulation[s]? of (\d+) to (\d+) inch',
        r'accumulation[s]? of (\d+)-(\d+) inch',
        r'(\d+) to (\d+) inch[es]* of snow',
        r'(\d+)-(\d+) inch[es]* of snow',
        r'around (\d+) inch',
        r'up to (\d+) inch',
        r'less than (\d+) inch',
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            groups = match.groups()
            if len(groups) == 2:
                return float(groups[0]), float(groups[1])
            elif len(groups) == 1:
                val = float(groups[0])
                if 'less than' in pattern:
                    return 0, val
                elif 'up to' in pattern:
                    return 0, val
                else:
                    return val, val

    # Check for "no accumulation" or similar
    if 'no accumulation' in text or 'little or no' in text:
        return 0, 0

    return None, None


def fetch_temperature_data(conn):
    """Fetch temperature forecast data grouped by target date and fetch time."""
    cur = conn.cursor()

    cur.execute('''
        SELECT
            fetch_time,
            date(forecast_time) as target_date,
            period_name,
            temperature,
            is_daytime
        FROM forecast_snapshots
        WHERE temperature IS NOT NULL
        ORDER BY fetch_time, forecast_time
    ''')

    return cur.fetchall()


def fetch_snow_data(conn):
    """Fetch snow accumulation data from detailed forecasts."""
    cur = conn.cursor()

    cur.execute('''
        SELECT
            fetch_time,
            date(forecast_time) as target_date,
            period_name,
            detailed_forecast,
            is_daytime
        FROM forecast_snapshots
        WHERE detailed_forecast IS NOT NULL
        ORDER BY fetch_time, forecast_time
    ''')

    return cur.fetchall()


def fetch_precip_data(conn):
    """Fetch precipitation probability data."""
    cur = conn.cursor()

    cur.execute('''
        SELECT
            fetch_time,
            date(forecast_time) as target_date,
            period_name,
            precipitation_probability,
            is_daytime
        FROM forecast_snapshots
        WHERE precipitation_probability IS NOT NULL
        ORDER BY fetch_time, forecast_time
    ''')

    return cur.fetchall()


def build_temperature_spaghetti(data):
    """
    Build spaghetti chart data structure for temperature.

    Returns dict: {target_date: {fetch_time: {'high': temp, 'low': temp}}}
    """
    spaghetti = {}

    for fetch_time, target_date, period_name, temp, is_daytime in data:
        if target_date not in spaghetti:
            spaghetti[target_date] = {}

        fetch_key = fetch_time[:16]  # Truncate to minute precision
        if fetch_key not in spaghetti[target_date]:
            spaghetti[target_date][fetch_key] = {'high': None, 'low': None}

        if is_daytime:
            spaghetti[target_date][fetch_key]['high'] = temp
        else:
            spaghetti[target_date][fetch_key]['low'] = temp

    return spaghetti


def build_snow_spaghetti(data):
    """
    Build spaghetti chart data for snow accumulation.

    Returns dict: {target_date: {fetch_time: {'low': inches, 'high': inches}}}
    """
    spaghetti = {}

    for fetch_time, target_date, period_name, detailed, is_daytime in data:
        low_snow, high_snow = extract_snow_accumulation(detailed)

        if low_snow is None:
            continue

        if target_date not in spaghetti:
            spaghetti[target_date] = {}

        fetch_key = fetch_time[:16]
        if fetch_key not in spaghetti[target_date]:
            spaghetti[target_date][fetch_key] = {'low': 0, 'high': 0}

        # Accumulate snow predictions for the day
        spaghetti[target_date][fetch_key]['low'] += low_snow
        spaghetti[target_date][fetch_key]['high'] += high_snow

    return spaghetti


def build_precip_spaghetti(data):
    """
    Build spaghetti chart data for precipitation probability.

    Returns dict: {target_date: {fetch_time: {'max': prob}}}
    """
    spaghetti = {}

    for fetch_time, target_date, period_name, precip, is_daytime in data:
        if precip is None:
            continue

        if target_date not in spaghetti:
            spaghetti[target_date] = {}

        fetch_key = fetch_time[:16]
        if fetch_key not in spaghetti[target_date]:
            spaghetti[target_date][fetch_key] = {'max': 0}

        # Track max precip probability for the day
        spaghetti[target_date][fetch_key]['max'] = max(
            spaghetti[target_date][fetch_key]['max'],
            precip
        )

    return spaghetti


def plot_temperature_spaghetti(spaghetti, save_path=None):
    """Plot temperature spaghetti chart."""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), sharex=True)
    fig.suptitle('Temperature Forecast Evolution\nHow predictions changed as target dates approached',
                 fontsize=14, fontweight='bold')

    # Get all unique fetch times across all dates
    all_fetches = set()
    for date_data in spaghetti.values():
        all_fetches.update(date_data.keys())
    fetch_list = sorted(all_fetches)

    # Plot each target date as a separate line
    dates = sorted(spaghetti.keys())

    for i, target_date in enumerate(dates):
        color = COLORS[i % len(COLORS)]
        date_label = datetime.strptime(target_date, '%Y-%m-%d').strftime('%a %m/%d')

        # Extract data points
        fetches = []
        highs = []
        lows = []

        for fetch_time in sorted(spaghetti[target_date].keys()):
            temps = spaghetti[target_date][fetch_time]
            fetch_dt = parse_fetch_time(fetch_time)

            if temps['high'] is not None:
                fetches.append(fetch_dt)
                highs.append(temps['high'])
            if temps['low'] is not None:
                if len(lows) < len(fetches):
                    lows.append(temps['low'])

        # Plot highs
        if highs:
            ax1.plot(fetches[:len(highs)], highs, 'o-', color=color, label=date_label,
                    linewidth=2, markersize=6, alpha=0.8)

        # Plot lows
        if lows:
            ax2.plot(fetches[:len(lows)], lows, 'o-', color=color, label=date_label,
                    linewidth=2, markersize=6, alpha=0.8)

    # Format axes
    ax1.set_ylabel('High Temperature (°F)', fontsize=11)
    ax1.set_title('Daytime High Forecasts', fontsize=12)
    ax1.legend(loc='upper left', bbox_to_anchor=(1.01, 1), fontsize=9)
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))

    ax2.set_ylabel('Low Temperature (°F)', fontsize=11)
    ax2.set_xlabel('Forecast Made At', fontsize=11)
    ax2.set_title('Overnight Low Forecasts', fontsize=12)
    ax2.legend(loc='upper left', bbox_to_anchor=(1.01, 1), fontsize=9)
    ax2.grid(True, alpha=0.3)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))

    plt.xticks(rotation=45)
    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"Saved to: {save_path}")
    else:
        plt.show()


def plot_snow_spaghetti(spaghetti, save_path=None):
    """Plot snow accumulation spaghetti chart with uncertainty bands."""
    fig, ax = plt.subplots(figsize=(14, 8))
    fig.suptitle('Snow Accumulation Forecast Evolution\nHow predictions changed as storm approached',
                 fontsize=14, fontweight='bold')

    dates = sorted(spaghetti.keys())

    for i, target_date in enumerate(dates):
        color = COLORS[i % len(COLORS)]
        date_label = datetime.strptime(target_date, '%Y-%m-%d').strftime('%a %m/%d')

        fetches = []
        lows = []
        highs = []
        mids = []

        for fetch_time in sorted(spaghetti[target_date].keys()):
            snow = spaghetti[target_date][fetch_time]
            fetch_dt = parse_fetch_time(fetch_time)

            if snow['high'] > 0 or snow['low'] > 0:
                fetches.append(fetch_dt)
                lows.append(snow['low'])
                highs.append(snow['high'])
                mids.append((snow['low'] + snow['high']) / 2)

        if fetches:
            # Plot uncertainty band
            ax.fill_between(fetches, lows, highs, color=color, alpha=0.2)
            # Plot midpoint line
            ax.plot(fetches, mids, 'o-', color=color, label=f'{date_label}',
                   linewidth=2, markersize=6)
            # Add range annotations at endpoints
            if len(fetches) > 0:
                ax.annotate(f'{lows[-1]:.0f}-{highs[-1]:.0f}"',
                           (fetches[-1], mids[-1]),
                           textcoords="offset points", xytext=(10, 0),
                           fontsize=8, color=color)

    ax.set_ylabel('Snow Accumulation (inches)', fontsize=11)
    ax.set_xlabel('Forecast Made At', fontsize=11)
    ax.legend(loc='upper left', bbox_to_anchor=(1.01, 1), fontsize=10,
             title='Target Date')
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
    ax.set_ylim(bottom=0)

    plt.xticks(rotation=45)
    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"Saved to: {save_path}")
    else:
        plt.show()


def plot_precip_spaghetti(spaghetti, save_path=None):
    """Plot precipitation probability spaghetti chart."""
    fig, ax = plt.subplots(figsize=(14, 8))
    fig.suptitle('Precipitation Probability Forecast Evolution\nHow confidence in precipitation changed over time',
                 fontsize=14, fontweight='bold')

    dates = sorted(spaghetti.keys())

    for i, target_date in enumerate(dates):
        color = COLORS[i % len(COLORS)]
        date_label = datetime.strptime(target_date, '%Y-%m-%d').strftime('%a %m/%d')

        fetches = []
        probs = []

        for fetch_time in sorted(spaghetti[target_date].keys()):
            precip = spaghetti[target_date][fetch_time]
            fetch_dt = parse_fetch_time(fetch_time)

            fetches.append(fetch_dt)
            probs.append(precip['max'])

        if fetches:
            ax.plot(fetches, probs, 'o-', color=color, label=date_label,
                   linewidth=2, markersize=6, alpha=0.8)

    ax.set_ylabel('Precipitation Probability (%)', fontsize=11)
    ax.set_xlabel('Forecast Made At', fontsize=11)
    ax.legend(loc='upper left', bbox_to_anchor=(1.01, 1), fontsize=10,
             title='Target Date')
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
    ax.set_ylim(0, 105)

    plt.xticks(rotation=45)
    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"Saved to: {save_path}")
    else:
        plt.show()


def print_data_summary(conn):
    """Print summary of available data."""
    cur = conn.cursor()

    cur.execute('SELECT MIN(fetch_time), MAX(fetch_time), COUNT(DISTINCT fetch_time) FROM forecast_snapshots')
    min_fetch, max_fetch, fetch_count = cur.fetchone()

    cur.execute('SELECT MIN(date(forecast_time)), MAX(date(forecast_time)) FROM forecast_snapshots')
    min_target, max_target = cur.fetchone()

    print("\n" + "="*60)
    print("WEATHER DATABASE SUMMARY")
    print("="*60)
    print(f"Data collection period: {min_fetch[:16]} to {max_fetch[:16]}")
    print(f"Number of forecast snapshots: {fetch_count}")
    print(f"Target dates covered: {min_target} to {max_target}")
    print("="*60 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description='Generate weather forecast spaghetti charts',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
    python weather_spaghetti.py                    # Show temperature chart
    python weather_spaghetti.py --metric snow     # Show snow accumulation
    python weather_spaghetti.py --metric precip   # Show precipitation probability
    python weather_spaghetti.py --save            # Save to PNG file
    python weather_spaghetti.py --metric snow --save output.png
        '''
    )
    parser.add_argument('--metric', choices=['temp', 'snow', 'precip'], default='temp',
                       help='Which metric to chart (default: temp)')
    parser.add_argument('--save', nargs='?', const='auto', default=None,
                       help='Save to file instead of displaying. Optionally specify filename.')

    args = parser.parse_args()

    # Connect to database
    conn = get_db_connection()
    print_data_summary(conn)

    # Determine save path
    save_path = None
    if args.save:
        if args.save == 'auto':
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            save_path = Path(f"D:/Scripts/weather_data/spaghetti_{args.metric}_{timestamp}.png")
        else:
            save_path = Path(args.save)

    # Generate appropriate chart
    if args.metric == 'temp':
        print("Generating temperature spaghetti chart...")
        data = fetch_temperature_data(conn)
        spaghetti = build_temperature_spaghetti(data)
        plot_temperature_spaghetti(spaghetti, save_path)

    elif args.metric == 'snow':
        print("Generating snow accumulation spaghetti chart...")
        data = fetch_snow_data(conn)
        spaghetti = build_snow_spaghetti(data)
        if not any(spaghetti.values()):
            print("No snow accumulation data found in forecasts.")
            return
        plot_snow_spaghetti(spaghetti, save_path)

    elif args.metric == 'precip':
        print("Generating precipitation probability spaghetti chart...")
        data = fetch_precip_data(conn)
        spaghetti = build_precip_spaghetti(data)
        plot_precip_spaghetti(spaghetti, save_path)

    conn.close()
    print("Done!")


if __name__ == '__main__':
    main()
