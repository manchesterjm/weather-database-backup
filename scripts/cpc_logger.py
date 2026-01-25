#!/usr/bin/env python3
"""
CPC (Climate Prediction Center) Outlook Logger

Fetches extended outlook discussions from CPC and stores them in the weather database:
- 8-14 Day Outlook (from 6-10/8-14 day page)
- Monthly (30-Day) Outlook

These outlooks are issued once daily around 3 PM EST.

Data Sources:
- 8-14 Day: https://www.cpc.ncep.noaa.gov/products/predictions/610day/fxus06.html
- Monthly: https://www.cpc.ncep.noaa.gov/products/predictions/30day/fxus07.html
"""

import logging
import re
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from html import unescape
from pathlib import Path

import requests

from script_metrics import ScriptMetrics

# Configuration
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "weather_data"
DB_PATH = DATA_DIR / "weather.db"
LOG_PATH = SCRIPT_DIR / "cpc_logger.log"

# CPC URLs
URLS = {
    "8_14_day": "https://www.cpc.ncep.noaa.gov/products/predictions/610day/fxus06.html",
    "monthly": "https://www.cpc.ncep.noaa.gov/products/predictions/30day/fxus07.html"
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
    """Create a database connection with crash-resilient settings."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA wal_autocheckpoint=1000")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_cpc_table():
    """Create CPC outlooks table if it doesn't exist."""
    conn = get_safe_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cpc_outlooks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetch_time TEXT NOT NULL,
            outlook_type TEXT NOT NULL,
            issued_date TEXT NOT NULL,
            valid_start TEXT,
            valid_end TEXT,
            discussion TEXT,
            UNIQUE(outlook_type, issued_date)
        )
    """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cpc_type ON cpc_outlooks(outlook_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cpc_issued ON cpc_outlooks(issued_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cpc_fetch ON cpc_outlooks(fetch_time)")

    conn.commit()
    conn.close()
    logger.info("CPC table initialized")


def clean_html(text: str) -> str:
    """Remove HTML tags and clean up text."""
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', text)
    # Decode HTML entities
    text = unescape(text)
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r' +', ' ', text)
    # Fix common patterns
    text = text.replace(' .', '.').replace(' ,', ',')
    return text.strip()


def extract_text_blocks(html: str) -> str:
    """Extract readable text from HTML, preserving paragraph structure."""
    # Split on common block elements
    html = re.sub(r'<br\s*/?>', '\n', html, flags=re.IGNORECASE)
    html = re.sub(r'</p>', '\n\n', html, flags=re.IGNORECASE)
    html = re.sub(r'</tr>', '\n', html, flags=re.IGNORECASE)

    # Remove scripts and styles
    html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.IGNORECASE | re.DOTALL)
    html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.IGNORECASE | re.DOTALL)

    # Remove all remaining tags
    text = re.sub(r'<[^>]+>', '', html)

    # Decode entities
    text = unescape(text)

    # Normalize whitespace but preserve paragraph breaks
    lines = text.split('\n')
    cleaned_lines = []
    for line in lines:
        line = ' '.join(line.split())
        if line:
            cleaned_lines.append(line)

    return '\n'.join(cleaned_lines)


def parse_8_14_day_outlook(html: str) -> dict:
    """Parse the 8-14 day outlook page."""
    result = {
        "issued_date": None,
        "valid_start": None,
        "valid_end": None,
        "discussion": None
    }

    # Extract issued date
    issued_match = re.search(r'Issued:\s*(\w+\s+\d+,\s+\d{4})', html)
    if issued_match:
        try:
            issued_str = issued_match.group(1)
            result["issued_date"] = datetime.strptime(issued_str, "%b %d, %Y").strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Extract 8-14 day valid dates
    valid_match = re.search(r'Valid:\s*(\w+\s+\d+)\s*-\s*(\d+),\s*(\d{4})\s*\(8-14 Day', html)
    if valid_match:
        try:
            month_day_start = valid_match.group(1)
            day_end = valid_match.group(2)
            year = valid_match.group(3)
            start_date = datetime.strptime(f"{month_day_start}, {year}", "%b %d, %Y")
            result["valid_start"] = start_date.strftime("%Y-%m-%d")
            # End date uses same month
            month = start_date.strftime("%b")
            end_date = datetime.strptime(f"{month} {day_end}, {year}", "%b %d, %Y")
            result["valid_end"] = end_date.strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Extract the 8-14 day discussion section
    discussion_match = re.search(
        r'8-14 DAY OUTLOOK FOR.*?(?=FORECAST CONFIDENCE|Forecaster|$)',
        html, re.DOTALL | re.IGNORECASE
    )
    if discussion_match:
        text = extract_text_blocks(discussion_match.group(0))
        result["discussion"] = text

    return result


def parse_monthly_outlook(html: str) -> dict:
    """Parse the monthly outlook page."""
    result = {
        "issued_date": None,
        "valid_start": None,
        "valid_end": None,
        "discussion": None
    }

    # Look for issue date - format: "300 PM EST Wed Dec 31 2025"
    # The time is in format like "300 PM" or "1200 PM"
    issued_match = re.search(
        r'(\d{3,4})\s+(AM|PM)\s+\w+\s+\w+\s+(\w+)\s+(\d+)\s+(\d{4})',
        html
    )
    if issued_match:
        try:
            month = issued_match.group(3)
            day = issued_match.group(4)
            year = issued_match.group(5)
            parsed = datetime.strptime(f"{month} {day} {year}", "%b %d %Y")
            result["issued_date"] = parsed.strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Extract valid period - format: "30-DAY OUTLOOK DISCUSSION FOR JAN 2026"
    valid_patterns = [
        r'30-DAY OUTLOOK.*?FOR\s+(\w+)\s+(\d{4})',
        r'MONTHLY.*?OUTLOOK.*?FOR\s+(\w+)\s+(\d{4})',
        r'ONE MONTH OUTLOOK FOR\s+(\w+)\s+(\d{4})',
    ]

    for pattern in valid_patterns:
        match = re.search(pattern, html, re.IGNORECASE)
        if match:
            try:
                month_name = match.group(1)
                year = match.group(2)
                # First day of month
                start = datetime.strptime(f"{month_name} 1, {year}", "%b %d, %Y")
                result["valid_start"] = start.strftime("%Y-%m-%d")
                # Last day of month
                if start.month == 12:
                    end = datetime(int(year) + 1, 1, 1)
                else:
                    end = datetime(int(year), start.month + 1, 1)
                end = end - timedelta(days=1)
                result["valid_end"] = end.strftime("%Y-%m-%d")
                break
            except ValueError:
                continue

    # Extract discussion from <PRE> tag
    pre_match = re.search(r'<PRE>(.*?)</PRE>', html, re.DOTALL | re.IGNORECASE)
    if pre_match:
        text = pre_match.group(1)
        # Clean HTML tags
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', '', text)
        text = unescape(text)
        # Find the 30-day discussion
        match = re.search(r'(30-DAY OUTLOOK.*?)(?=\*{20,}|$)', text, re.DOTALL | re.IGNORECASE)
        if match:
            discussion = match.group(1).strip()
            # Trim at forecaster signature
            for end_marker in ["\nFORECASTER:", "\n$$"]:
                idx = discussion.find(end_marker)
                if idx > 0:
                    discussion = discussion[:idx]
            result["discussion"] = discussion.strip()

    return result


def fetch_and_store_outlook(outlook_type: str, url: str) -> bool:
    """Fetch an outlook page and store it in the database."""
    try:
        logger.info("Fetching %s outlook from %s", outlook_type, url)
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        html = response.text

        # Parse based on type
        if outlook_type == "8_14_day":
            data = parse_8_14_day_outlook(html)
        elif outlook_type == "monthly":
            data = parse_monthly_outlook(html)
        else:
            logger.error("Unknown outlook type: %s", outlook_type)
            return False

        if not data["issued_date"]:
            logger.warning("Could not parse issued date for %s", outlook_type)
            return False

        if not data["discussion"]:
            logger.warning("Could not parse discussion for %s", outlook_type)
            return False

        # Store in database
        conn = get_safe_connection()
        cursor = conn.cursor()

        fetch_time = datetime.now(timezone.utc).isoformat()

        cursor.execute("""
            INSERT OR REPLACE INTO cpc_outlooks
            (fetch_time, outlook_type, issued_date, valid_start, valid_end, discussion)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            fetch_time,
            outlook_type,
            data["issued_date"],
            data["valid_start"],
            data["valid_end"],
            data["discussion"]
        ))

        conn.commit()
        conn.close()

        logger.info("Stored %s outlook issued %s", outlook_type, data['issued_date'])
        logger.info("  Valid: %s to %s", data['valid_start'], data['valid_end'])
        logger.info("  Discussion length: %d chars", len(data['discussion']))

        return True

    except requests.RequestException as e:
        logger.error("Error fetching %s: %s", outlook_type, e)
        return False
    except (ValueError, KeyError, sqlite3.Error) as e:
        logger.error("Error processing %s: %s", outlook_type, e)
        return False


def main():
    """Main entry point."""
    logger.info("=" * 50)
    logger.info("CPC Outlook Logger starting")

    # Ensure data directory exists
    DATA_DIR.mkdir(exist_ok=True)

    # Initialize database table
    init_cpc_table()

    with ScriptMetrics('cpc_logger', expected_items=len(URLS)) as metrics:
        # Fetch each outlook type
        for outlook_type, url in URLS.items():
            if fetch_and_store_outlook(outlook_type, url):
                metrics.item_succeeded(outlook_type, records_inserted=1, item_type='outlook')
            else:
                metrics.item_failed(outlook_type, f"Failed to fetch/store {outlook_type}",
                                    item_type='outlook')

        success_count = sum(1 for i in metrics.items.values() if i.status == "success")
        logger.info("CPC Logger complete: %d/%d outlooks stored", success_count, len(URLS))
        metrics.set_exit_code(0 if success_count > 0 else 1)

    return metrics.exit_code


if __name__ == "__main__":
    sys.exit(main() or 0)
