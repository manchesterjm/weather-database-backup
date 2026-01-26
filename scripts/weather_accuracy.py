#!/usr/bin/env python3
"""
Weather Forecast Accuracy Visualization
Compares forecasted temperatures vs actual observed temperatures.

Usage:
    python weather_accuracy.py                # Show temperature accuracy chart
    python weather_accuracy.py --save         # Save to file
    python weather_accuracy.py --daily        # Daily high/low comparison
    python weather_accuracy.py --hourly       # Hourly forecast vs actual
    python weather_accuracy.py --lead-time    # Accuracy by forecast lead time
"""

import sqlite3
import argparse
from datetime import datetime, timedelta

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

import db_utils

# Use paths from db_utils for consistency
DB_PATH = db_utils.DB_PATH
OUTPUT_DIR = db_utils.DATA_DIR


def get_connection():
    """Create a database connection with row factory."""
    conn = db_utils.get_connection()
    conn.row_factory = sqlite3.Row
    return conn


def fetch_daily_comparison(conn):
    """Get daily high/low forecasts vs actual observations."""
    query = """
    WITH daily_actuals AS (
        SELECT
            date(observation_time) as obs_date,
            MAX(temperature_c) * 9.0/5.0 + 32 as actual_high_f,
            MIN(temperature_c) * 9.0/5.0 + 32 as actual_low_f
        FROM metar
        WHERE station_id IN ('KCOS', 'KFLY', 'KFCS', 'KAFF') AND temperature_c IS NOT NULL
        GROUP BY date(observation_time)
    ),
    daily_forecasts AS (
        SELECT
            date(forecast_time) as forecast_date,
            date(fetch_time) as fetch_date,
            MAX(CASE WHEN is_daytime = 1 THEN temperature END) as forecast_high,
            MIN(CASE WHEN is_daytime = 0 THEN temperature END) as forecast_low,
            julianday(forecast_time) - julianday(fetch_time) as lead_days
        FROM forecast_snapshots
        WHERE temperature IS NOT NULL
        GROUP BY date(forecast_time), date(fetch_time)
    )
    SELECT
        a.obs_date,
        a.actual_high_f,
        a.actual_low_f,
        f.forecast_high,
        f.forecast_low,
        f.lead_days
    FROM daily_actuals a
    INNER JOIN daily_forecasts f ON a.obs_date = f.forecast_date
    WHERE f.lead_days BETWEEN 0 AND 7
    ORDER BY a.obs_date, f.lead_days
    """
    return conn.execute(query).fetchall()


def fetch_hourly_comparison(conn):
    """Get hourly forecasts vs actual observations."""
    query = """
    WITH hourly_actuals AS (
        SELECT
            strftime('%Y-%m-%d %H:00', observation_time) as obs_hour,
            AVG(temperature_c) * 9.0/5.0 + 32 as actual_temp_f
        FROM metar
        WHERE station_id IN ('KCOS', 'KFLY', 'KFCS', 'KAFF') AND temperature_c IS NOT NULL
        GROUP BY strftime('%Y-%m-%d %H:00', observation_time)
    ),
    hourly_forecasts AS (
        SELECT
            forecast_date || ' ' || printf('%02d:00', forecast_hour) as forecast_hour,
            temperature as forecast_temp,
            fetch_time
        FROM digital_forecast
        WHERE temperature IS NOT NULL
    )
    SELECT
        a.obs_hour,
        a.actual_temp_f,
        f.forecast_temp,
        (julianday(a.obs_hour) - julianday(f.fetch_time)) * 24 as lead_hours
    FROM hourly_actuals a
    INNER JOIN hourly_forecasts f ON a.obs_hour = f.forecast_hour
    WHERE lead_hours BETWEEN 0 AND 48
    ORDER BY a.obs_hour, lead_hours
    """
    return conn.execute(query).fetchall()


def fetch_lead_time_accuracy(conn):
    """Get forecast accuracy grouped by lead time."""
    query = """
    WITH daily_actuals AS (
        SELECT
            date(observation_time) as obs_date,
            MAX(temperature_c) * 9.0/5.0 + 32 as actual_high_f
        FROM metar
        WHERE station_id IN ('KCOS', 'KFLY', 'KFCS', 'KAFF') AND temperature_c IS NOT NULL
        GROUP BY date(observation_time)
    ),
    daily_forecasts AS (
        SELECT
            date(forecast_time) as forecast_date,
            MAX(CASE WHEN is_daytime = 1 THEN temperature END) as forecast_high,
            CAST(julianday(forecast_time) - julianday(fetch_time) AS INTEGER) as lead_days
        FROM forecast_snapshots
        WHERE temperature IS NOT NULL AND is_daytime = 1
        GROUP BY date(forecast_time), CAST(julianday(forecast_time) - julianday(fetch_time) AS INTEGER)
    )
    SELECT
        f.lead_days,
        AVG(ABS(f.forecast_high - a.actual_high_f)) as mae,
        AVG((f.forecast_high - a.actual_high_f) * (f.forecast_high - a.actual_high_f)) as mse,
        AVG(f.forecast_high - a.actual_high_f) as bias,
        COUNT(*) as sample_count
    FROM daily_forecasts f
    INNER JOIN daily_actuals a ON f.forecast_date = a.obs_date
    WHERE f.lead_days BETWEEN 0 AND 7
    GROUP BY f.lead_days
    ORDER BY f.lead_days
    """
    return conn.execute(query).fetchall()


def plot_daily_comparison(conn, save_path=None):
    """Plot daily high/low forecast vs actual."""
    # Get 1-day ahead forecasts for cleaner comparison
    query = """
    WITH daily_actuals AS (
        SELECT
            date(observation_time) as obs_date,
            MAX(temperature_c) * 9.0/5.0 + 32 as actual_high_f,
            MIN(temperature_c) * 9.0/5.0 + 32 as actual_low_f
        FROM metar
        WHERE station_id IN ('KCOS', 'KFLY', 'KFCS', 'KAFF') AND temperature_c IS NOT NULL
        GROUP BY date(observation_time)
    ),
    daily_forecasts AS (
        SELECT
            date(forecast_time) as forecast_date,
            MAX(CASE WHEN is_daytime = 1 THEN temperature END) as forecast_high,
            MIN(CASE WHEN is_daytime = 0 THEN temperature END) as forecast_low
        FROM forecast_snapshots
        WHERE temperature IS NOT NULL
          AND julianday(forecast_time) - julianday(fetch_time) BETWEEN 0.5 AND 1.5
        GROUP BY date(forecast_time)
    )
    SELECT
        a.obs_date,
        a.actual_high_f,
        a.actual_low_f,
        f.forecast_high,
        f.forecast_low
    FROM daily_actuals a
    INNER JOIN daily_forecasts f ON a.obs_date = f.forecast_date
    ORDER BY a.obs_date
    """
    rows = conn.execute(query).fetchall()

    if not rows:
        print("No data available for daily comparison")
        return

    dates = [datetime.strptime(r['obs_date'], '%Y-%m-%d') for r in rows]
    actual_highs = [r['actual_high_f'] for r in rows]
    actual_lows = [r['actual_low_f'] for r in rows]
    forecast_highs = [r['forecast_high'] for r in rows]
    forecast_lows = [r['forecast_low'] for r in rows]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
    fig.suptitle('Daily Temperature: Forecast vs Actual (1-Day Ahead)', fontsize=14, fontweight='bold')

    # High temperatures
    ax1.plot(dates, actual_highs, 'ro-', label='Actual High', markersize=8)
    ax1.plot(dates, forecast_highs, 'r^--', label='Forecast High', markersize=8, alpha=0.7)
    ax1.fill_between(dates, actual_highs, forecast_highs, alpha=0.3, color='red')
    ax1.set_ylabel('Temperature (°F)')
    ax1.set_title('Daily Highs')
    ax1.legend(loc='upper right')
    ax1.grid(True, alpha=0.3)

    # Calculate and show MAE for highs
    high_errors = [abs(f - a) for f, a in zip(forecast_highs, actual_highs) if f and a]
    if high_errors:
        mae_high = sum(high_errors) / len(high_errors)
        ax1.text(0.02, 0.98, f'MAE: {mae_high:.1f}°F', transform=ax1.transAxes,
                 verticalalignment='top', fontsize=10, bbox=dict(boxstyle='round', facecolor='wheat'))

    # Low temperatures
    ax2.plot(dates, actual_lows, 'bo-', label='Actual Low', markersize=8)
    ax2.plot(dates, forecast_lows, 'b^--', label='Forecast Low', markersize=8, alpha=0.7)
    ax2.fill_between(dates, actual_lows, forecast_lows, alpha=0.3, color='blue')
    ax2.set_ylabel('Temperature (°F)')
    ax2.set_title('Daily Lows')
    ax2.legend(loc='upper right')
    ax2.grid(True, alpha=0.3)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
    ax2.xaxis.set_major_locator(mdates.DayLocator())
    plt.xticks(rotation=45)

    # Calculate and show MAE for lows
    low_errors = [abs(f - a) for f, a in zip(forecast_lows, actual_lows) if f and a]
    if low_errors:
        mae_low = sum(low_errors) / len(low_errors)
        ax2.text(0.02, 0.98, f'MAE: {mae_low:.1f}°F', transform=ax2.transAxes,
                 verticalalignment='top', fontsize=10, bbox=dict(boxstyle='round', facecolor='wheat'))

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"Saved to: {save_path}")
    else:
        plt.show()


def plot_hourly_comparison(conn, save_path=None):
    """Plot hourly temperature forecast vs actual for recent days."""
    query = """
    WITH hourly_actuals AS (
        SELECT
            strftime('%Y-%m-%d %H:00', observation_time) as obs_hour,
            AVG(temperature_c) * 9.0/5.0 + 32 as actual_temp_f
        FROM metar
        WHERE station_id IN ('KCOS', 'KFLY', 'KFCS', 'KAFF') AND temperature_c IS NOT NULL
          AND date(observation_time) >= date('now', '-3 days')
        GROUP BY strftime('%Y-%m-%d %H:00', observation_time)
    ),
    hourly_forecasts AS (
        SELECT
            forecast_date || ' ' || printf('%02d:00', forecast_hour) as forecast_hour,
            temperature as forecast_temp
        FROM digital_forecast
        WHERE temperature IS NOT NULL
        GROUP BY forecast_date, forecast_hour
        HAVING fetch_time = MAX(fetch_time)
    )
    SELECT
        a.obs_hour,
        a.actual_temp_f,
        f.forecast_temp
    FROM hourly_actuals a
    LEFT JOIN hourly_forecasts f ON a.obs_hour = f.forecast_hour
    ORDER BY a.obs_hour
    """
    rows = conn.execute(query).fetchall()

    if not rows:
        print("No data available for hourly comparison")
        return

    times = [datetime.strptime(r['obs_hour'], '%Y-%m-%d %H:%M') for r in rows]
    actuals = [r['actual_temp_f'] for r in rows]
    forecasts = [r['forecast_temp'] for r in rows]

    fig, ax = plt.subplots(figsize=(14, 6))
    fig.suptitle('Hourly Temperature: Forecast vs Actual (Last 3 Days)', fontsize=14, fontweight='bold')

    ax.plot(times, actuals, 'b-', label='Actual (METAR)', linewidth=2)
    ax.plot(times, forecasts, 'r--', label='Forecast (NWS)', linewidth=2, alpha=0.8)

    # Shade the difference
    ax.fill_between(times, actuals, forecasts, alpha=0.2, color='purple')

    ax.set_ylabel('Temperature (°F)')
    ax.set_xlabel('Date/Time')
    ax.legend(loc='upper right')
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=6))
    plt.xticks(rotation=45)

    # Calculate MAE
    errors = [abs(f - a) for f, a in zip(forecasts, actuals) if f and a]
    if errors:
        mae = sum(errors) / len(errors)
        ax.text(0.02, 0.98, f'MAE: {mae:.1f}°F', transform=ax.transAxes,
                verticalalignment='top', fontsize=11, bbox=dict(boxstyle='round', facecolor='wheat'))

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"Saved to: {save_path}")
    else:
        plt.show()


def plot_lead_time_accuracy(conn, save_path=None):
    """Plot forecast accuracy by lead time (days ahead)."""
    rows = fetch_lead_time_accuracy(conn)

    if not rows:
        print("No data available for lead time analysis")
        return

    lead_days = [r['lead_days'] for r in rows]
    mae = [r['mae'] for r in rows]
    bias = [r['bias'] for r in rows]
    rmse = [np.sqrt(r['mse']) for r in rows]
    counts = [r['sample_count'] for r in rows]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle('Forecast Accuracy by Lead Time (Days Ahead)', fontsize=14, fontweight='bold')

    # MAE and RMSE
    x = np.arange(len(lead_days))
    width = 0.35
    ax1.bar(x - width/2, mae, width, label='MAE', color='steelblue')
    ax1.bar(x + width/2, rmse, width, label='RMSE', color='coral')
    ax1.set_xlabel('Days Ahead')
    ax1.set_ylabel('Error (°F)')
    ax1.set_title('Mean Absolute Error & RMSE')
    ax1.set_xticks(x)
    ax1.set_xticklabels([f'{d}' for d in lead_days])
    ax1.legend()
    ax1.grid(True, alpha=0.3, axis='y')

    # Add sample counts on bars
    for i, (m, c) in enumerate(zip(mae, counts)):
        ax1.text(i - width/2, m + 0.2, f'n={c}', ha='center', va='bottom', fontsize=8)

    # Bias
    colors = ['green' if b >= 0 else 'red' for b in bias]
    ax2.bar(lead_days, bias, color=colors, alpha=0.7)
    ax2.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    ax2.set_xlabel('Days Ahead')
    ax2.set_ylabel('Bias (°F)')
    ax2.set_title('Forecast Bias (positive = forecast too warm)')
    ax2.grid(True, alpha=0.3, axis='y')

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"Saved to: {save_path}")
    else:
        plt.show()


def plot_precip_accuracy(conn, save_path=None):
    """Plot precipitation forecast accuracy (PoP reliability)."""
    # Get daily forecast PoP and check if precip actually occurred in METAR
    query = """
    WITH daily_forecasts AS (
        SELECT
            date(forecast_time) as forecast_date,
            MAX(precipitation_probability) as max_pop,
            date(fetch_time) as fetch_date
        FROM forecast_snapshots
        WHERE precipitation_probability IS NOT NULL
          AND julianday(forecast_time) - julianday(fetch_time) BETWEEN 0.5 AND 1.5
        GROUP BY date(forecast_time), date(fetch_time)
    ),
    daily_precip AS (
        SELECT
            date(observation_time) as obs_date,
            MAX(CASE WHEN weather_phenomena IS NOT NULL
                      AND weather_phenomena != ''
                      AND weather_phenomena NOT LIKE '%BR%'
                      AND weather_phenomena NOT LIKE '%HZ%'
                      AND weather_phenomena NOT LIKE '%FG%'
                 THEN 1 ELSE 0 END) as had_precip,
            GROUP_CONCAT(DISTINCT weather_phenomena) as phenomena
        FROM metar
        WHERE station_id IN ('KCOS', 'KFLY', 'KFCS', 'KAFF')
        GROUP BY date(observation_time)
    )
    SELECT
        f.forecast_date,
        f.max_pop,
        COALESCE(p.had_precip, 0) as had_precip,
        p.phenomena
    FROM daily_forecasts f
    LEFT JOIN daily_precip p ON f.forecast_date = p.obs_date
    ORDER BY f.forecast_date
    """
    rows = conn.execute(query).fetchall()

    if not rows:
        print("No data available for precipitation analysis")
        return

    # Bin PoP values and calculate actual frequency
    bins = [(0, 10), (10, 30), (30, 50), (50, 70), (70, 90), (90, 100)]
    bin_labels = ['0-10%', '10-30%', '30-50%', '50-70%', '70-90%', '90-100%']
    bin_data = {label: {'count': 0, 'precip': 0} for label in bin_labels}

    dates = []
    pops = []
    actuals = []

    for row in rows:
        pop = row['max_pop'] if row['max_pop'] else 0
        had_precip = row['had_precip']
        dates.append(datetime.strptime(row['forecast_date'], '%Y-%m-%d'))
        pops.append(pop)
        actuals.append(had_precip * 100)  # Scale to 0-100 for chart

        for i, (low, high) in enumerate(bins):
            if low <= pop < high or (high == 100 and pop == 100):
                bin_data[bin_labels[i]]['count'] += 1
                bin_data[bin_labels[i]]['precip'] += had_precip
                break

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle('Precipitation Forecast Accuracy', fontsize=14, fontweight='bold')

    # Left: PoP reliability diagram
    forecast_pops = []
    observed_freqs = []
    sample_counts = []

    for i, label in enumerate(bin_labels):
        data = bin_data[label]
        if data['count'] > 0:
            mid_pop = (bins[i][0] + bins[i][1]) / 2
            forecast_pops.append(mid_pop)
            observed_freqs.append(data['precip'] / data['count'] * 100)
            sample_counts.append(data['count'])

    if forecast_pops:
        ax1.plot([0, 100], [0, 100], 'k--', alpha=0.5, label='Perfect reliability')
        ax1.scatter(forecast_pops, observed_freqs, s=100, c='steelblue', zorder=5)
        ax1.plot(forecast_pops, observed_freqs, 'b-', alpha=0.7)

        for x, y, n in zip(forecast_pops, observed_freqs, sample_counts):
            ax1.annotate(f'n={n}', (x, y), textcoords="offset points",
                        xytext=(0, 10), ha='center', fontsize=9)

        ax1.set_xlabel('Forecast PoP (%)')
        ax1.set_ylabel('Observed Frequency (%)')
        ax1.set_title('PoP Reliability Diagram')
        ax1.set_xlim(0, 100)
        ax1.set_ylim(0, 100)
        ax1.legend()
        ax1.grid(True, alpha=0.3)

    # Right: Timeline of PoP vs actual precip events
    ax2.bar(dates, pops, width=0.8, alpha=0.6, color='steelblue', label='Forecast PoP')
    precip_dates = [d for d, a in zip(dates, actuals) if a > 0]
    precip_heights = [100 for _ in precip_dates]
    if precip_dates:
        ax2.scatter(precip_dates, [5] * len(precip_dates), marker='^',
                   s=100, c='green', zorder=5, label='Precip Observed')

    ax2.set_xlabel('Date')
    ax2.set_ylabel('PoP (%)')
    ax2.set_title('Daily Forecast PoP vs Actual Precipitation')
    ax2.legend(loc='upper right')
    ax2.grid(True, alpha=0.3, axis='y')
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
    ax2.xaxis.set_major_locator(mdates.DayLocator())
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"Saved to: {save_path}")
    else:
        plt.show()


def sky_condition_to_pct(sky_condition):
    """Convert METAR sky condition to cloud cover percentage."""
    if not sky_condition:
        return None

    # Take the highest coverage layer
    max_cover = 0
    coverage_map = {
        'CLR': 0, 'SKC': 0, 'NSC': 0, 'CAVOK': 0,
        'FEW': 18.75,   # 1-2 oktas -> ~18.75%
        'SCT': 43.75,   # 3-4 oktas -> ~43.75%
        'BKN': 75,      # 5-7 oktas -> ~75%
        'OVC': 100,     # 8 oktas -> 100%
        'VV': 100       # Vertical visibility (obscured) -> 100%
    }

    for code in coverage_map:
        if code in sky_condition.upper():
            max_cover = max(max_cover, coverage_map[code])

    return max_cover if max_cover > 0 or 'CLR' in sky_condition.upper() or 'SKC' in sky_condition.upper() else None


def plot_cloud_accuracy(conn, save_path=None):
    """Plot cloud cover forecast accuracy (NBM vs METAR)."""
    query = """
    WITH hourly_actuals AS (
        SELECT
            strftime('%Y-%m-%d %H:00', observation_time) as obs_hour,
            sky_condition
        FROM metar
        WHERE sky_condition IS NOT NULL AND sky_condition != ''
          AND station_id IN ('KCOS', 'KFLY', 'KFCS', 'KAFF')
        GROUP BY strftime('%Y-%m-%d %H:00', observation_time)
        HAVING observation_time = MAX(observation_time)
    ),
    hourly_forecasts AS (
        SELECT
            strftime('%Y-%m-%d %H:00', valid_time) as forecast_hour,
            sky_cover_pct,
            fetch_time
        FROM nbm_forecasts
        WHERE sky_cover_pct IS NOT NULL
        GROUP BY strftime('%Y-%m-%d %H:00', valid_time)
        HAVING fetch_time = MAX(fetch_time)
    )
    SELECT
        a.obs_hour,
        a.sky_condition,
        f.sky_cover_pct as forecast_cover
    FROM hourly_actuals a
    INNER JOIN hourly_forecasts f ON a.obs_hour = f.forecast_hour
    ORDER BY a.obs_hour
    """
    rows = conn.execute(query).fetchall()

    if not rows:
        print("No data available for cloud cover analysis")
        return

    times = []
    actuals = []
    forecasts = []

    for row in rows:
        actual_pct = sky_condition_to_pct(row['sky_condition'])
        if actual_pct is not None:
            times.append(datetime.strptime(row['obs_hour'], '%Y-%m-%d %H:%M'))
            actuals.append(actual_pct)
            forecasts.append(row['forecast_cover'])

    if not times:
        print("No matching cloud cover data found")
        return

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8))
    fig.suptitle('Cloud Cover: NBM Forecast vs METAR Observed', fontsize=14, fontweight='bold')

    # Top: Time series comparison
    ax1.plot(times, actuals, 'b-', label='Actual (METAR)', linewidth=2, alpha=0.8)
    ax1.plot(times, forecasts, 'r--', label='Forecast (NBM)', linewidth=2, alpha=0.8)
    ax1.fill_between(times, actuals, forecasts, alpha=0.2, color='purple')
    ax1.set_ylabel('Cloud Cover (%)')
    ax1.set_ylim(0, 105)
    ax1.legend(loc='upper right')
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
    ax1.xaxis.set_major_locator(mdates.HourLocator(interval=12))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')

    # Calculate MAE
    errors = [abs(f - a) for f, a in zip(forecasts, actuals)]
    mae = sum(errors) / len(errors)
    bias = sum(f - a for f, a in zip(forecasts, actuals)) / len(forecasts)
    ax1.text(0.02, 0.98, f'MAE: {mae:.1f}%  Bias: {bias:+.1f}%',
             transform=ax1.transAxes, verticalalignment='top', fontsize=11,
             bbox=dict(boxstyle='round', facecolor='wheat'))

    # Bottom: Scatter plot with density
    ax2.scatter(forecasts, actuals, alpha=0.5, c='steelblue', s=30)
    ax2.plot([0, 100], [0, 100], 'k--', alpha=0.5, label='Perfect forecast')
    ax2.set_xlabel('Forecast Cloud Cover (%)')
    ax2.set_ylabel('Actual Cloud Cover (%)')
    ax2.set_title('Forecast vs Actual Scatter')
    ax2.set_xlim(0, 105)
    ax2.set_ylim(0, 105)
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    # Add count
    ax2.text(0.98, 0.02, f'n={len(times)}', transform=ax2.transAxes,
             ha='right', va='bottom', fontsize=10)

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"Saved to: {save_path}")
    else:
        plt.show()


def main():
    parser = argparse.ArgumentParser(description='Weather Forecast Accuracy Visualization')
    parser.add_argument('--save', nargs='?', const='auto', default=None,
                        help='Save chart to file (auto-generates name if no path given)')
    parser.add_argument('--daily', action='store_true', help='Daily high/low comparison')
    parser.add_argument('--hourly', action='store_true', help='Hourly forecast vs actual')
    parser.add_argument('--lead-time', action='store_true', help='Accuracy by forecast lead time')
    parser.add_argument('--precip', action='store_true', help='Precipitation forecast accuracy')
    parser.add_argument('--clouds', action='store_true', help='Cloud cover forecast accuracy')
    args = parser.parse_args()

    # Default to daily if no specific chart requested
    if not any([args.daily, args.hourly, args.lead_time, args.precip, args.clouds]):
        args.daily = True

    conn = get_connection()
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    try:
        if args.daily:
            save_path = None
            if args.save:
                save_path = args.save if args.save != 'auto' else OUTPUT_DIR / f'accuracy_daily_{timestamp}.png'
            plot_daily_comparison(conn, save_path)

        if args.hourly:
            save_path = None
            if args.save:
                save_path = args.save if args.save != 'auto' else OUTPUT_DIR / f'accuracy_hourly_{timestamp}.png'
            plot_hourly_comparison(conn, save_path)

        if args.lead_time:
            save_path = None
            if args.save:
                save_path = args.save if args.save != 'auto' else OUTPUT_DIR / f'accuracy_leadtime_{timestamp}.png'
            plot_lead_time_accuracy(conn, save_path)

        if args.precip:
            save_path = None
            if args.save:
                save_path = args.save if args.save != 'auto' else OUTPUT_DIR / f'accuracy_precip_{timestamp}.png'
            plot_precip_accuracy(conn, save_path)

        if args.clouds:
            save_path = None
            if args.save:
                save_path = args.save if args.save != 'auto' else OUTPUT_DIR / f'accuracy_clouds_{timestamp}.png'
            plot_cloud_accuracy(conn, save_path)

    finally:
        conn.close()


if __name__ == '__main__':
    main()
