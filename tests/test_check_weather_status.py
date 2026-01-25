#!/usr/bin/env python3
"""
Tests for check_weather_status module.

Tests cover formatting functions, query building, and display functions.
Uses pytest fixtures for database isolation.
"""

import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import pytest

# Import module under test
sys.path.insert(0, str(Path(__file__).parent.parent))
from check_weather_status import (
    format_duration, format_time, status_icon, truncate_error,
    build_summary_query, format_items_info, print_header, print_footer,
    show_summary, show_details, show_reliability, show_recent_failures,
    main, get_connection, DB_PATH
)


class TestFormatDuration:
    """Tests for format_duration function."""

    def test_seconds(self):
        """Should format durations under 60 seconds."""
        start = "2026-01-25T10:00:00+00:00"
        end = "2026-01-25T10:00:30+00:00"
        assert format_duration(start, end) == "30.0s"

    def test_minutes(self):
        """Should format durations under 60 minutes."""
        start = "2026-01-25T10:00:00+00:00"
        end = "2026-01-25T10:05:00+00:00"
        assert format_duration(start, end) == "5.0m"

    def test_hours(self):
        """Should format durations over 60 minutes."""
        start = "2026-01-25T10:00:00+00:00"
        end = "2026-01-25T12:30:00+00:00"
        assert format_duration(start, end) == "2.5h"

    def test_missing_start(self):
        """Should return N/A for missing start time."""
        assert format_duration(None, "2026-01-25T10:00:00+00:00") == "N/A"

    def test_missing_end(self):
        """Should return N/A for missing end time."""
        assert format_duration("2026-01-25T10:00:00+00:00", None) == "N/A"

    def test_empty_strings(self):
        """Should return N/A for empty strings."""
        assert format_duration("", "") == "N/A"

    def test_invalid_format(self):
        """Should return N/A for invalid timestamps."""
        assert format_duration("invalid", "also invalid") == "N/A"

    def test_z_suffix(self):
        """Should handle Z suffix for UTC."""
        start = "2026-01-25T10:00:00Z"
        end = "2026-01-25T10:01:00Z"
        assert format_duration(start, end) == "1.0m"


class TestFormatTime:
    """Tests for format_time function."""

    def test_valid_iso_timestamp(self):
        """Should format valid ISO timestamp."""
        result = format_time("2026-01-25T10:30:00+00:00")
        # Result depends on local timezone, but should contain the date
        assert "2026-01-25" in result or "2026-01-24" in result  # Timezone may shift date

    def test_missing_timestamp(self):
        """Should return N/A for None."""
        assert format_time(None) == "N/A"

    def test_empty_string(self):
        """Should return N/A for empty string."""
        assert format_time("") == "N/A"

    def test_invalid_timestamp(self):
        """Should return truncated string for invalid format."""
        result = format_time("invalid-timestamp-here")
        assert result == "invalid-timestam"  # First 16 chars


class TestStatusIcon:
    """Tests for status_icon function."""

    def test_success(self):
        """Should return OK icon for success."""
        assert status_icon("success") == "[OK]"

    def test_partial(self):
        """Should return warning icon for partial."""
        assert status_icon("partial") == "[!!]"

    def test_failed(self):
        """Should return error icon for failed."""
        assert status_icon("failed") == "[XX]"

    def test_running(self):
        """Should return running icon for running."""
        assert status_icon("running") == "[..]"

    def test_unknown(self):
        """Should return unknown icon for unknown status."""
        assert status_icon("unknown") == "[??]"
        assert status_icon("") == "[??]"


class TestTruncateError:
    """Tests for truncate_error function."""

    def test_short_message(self):
        """Should not truncate short messages."""
        msg = "Short error"
        assert truncate_error(msg) == msg

    def test_long_message(self):
        """Should truncate long messages with ellipsis."""
        msg = "x" * 150
        result = truncate_error(msg)
        assert len(result) == 103  # 100 + "..."
        assert result.endswith("...")

    def test_custom_max_len(self):
        """Should respect custom max_len."""
        msg = "This is a test message"
        result = truncate_error(msg, max_len=10)
        assert result == "This is a ..."

    def test_empty_message(self):
        """Should return empty string for empty input."""
        assert truncate_error("") == ""
        assert truncate_error(None) == ""

    def test_exact_length(self):
        """Should not truncate message at exact max length."""
        msg = "x" * 100
        result = truncate_error(msg)
        assert result == msg
        assert len(result) == 100


class TestBuildSummaryQuery:
    """Tests for build_summary_query function."""

    def test_no_filters(self):
        """Should build basic query with no filters."""
        query, params = build_summary_query(None, False)
        assert "WHERE start_time > ?" in query
        assert params == []
        assert "script_name LIKE" not in query
        assert "status IN" not in query

    def test_script_filter(self):
        """Should add script filter when provided."""
        query, params = build_summary_query("gfs", False)
        assert "script_name LIKE ?" in query
        assert params == ["%gfs%"]

    def test_failures_only(self):
        """Should add status filter for failures."""
        query, params = build_summary_query(None, True)
        assert "status IN ('failed', 'partial')" in query
        assert params == []

    def test_both_filters(self):
        """Should combine both filters."""
        query, params = build_summary_query("weather", True)
        assert "script_name LIKE ?" in query
        assert "status IN ('failed', 'partial')" in query
        assert params == ["%weather%"]


class TestFormatItemsInfo:
    """Tests for format_items_info function."""

    def test_with_expected_items(self):
        """Should show succeeded/expected format."""
        row = {
            'total_items_expected': 10,
            'total_items_succeeded': 8,
            'total_items_failed': 2
        }
        # Create a mock Row-like object
        class MockRow(dict):
            def __getitem__(self, key):
                return self.get(key)

        mock_row = MockRow(row)
        result = format_items_info(mock_row)
        assert result == "8/10 items"

    def test_without_expected_items(self):
        """Should show ok/fail format when no expected count."""
        row = {
            'total_items_expected': None,
            'total_items_succeeded': 5,
            'total_items_failed': 2
        }
        class MockRow(dict):
            def __getitem__(self, key):
                return self.get(key)

        mock_row = MockRow(row)
        result = format_items_info(mock_row)
        assert result == "5 ok, 2 fail"

    def test_no_items(self):
        """Should return empty string when no items."""
        row = {
            'total_items_expected': None,
            'total_items_succeeded': 0,
            'total_items_failed': 0
        }
        class MockRow(dict):
            def __getitem__(self, key):
                return self.get(key)

        mock_row = MockRow(row)
        result = format_items_info(mock_row)
        assert result == ""


class TestPrintFunctions:
    """Tests for print_header and print_footer."""

    def test_print_header(self, capsys):
        """Should print formatted header."""
        print_header("Test Title")
        captured = capsys.readouterr()
        assert "=" * 80 in captured.out
        assert "Test Title" in captured.out

    def test_print_footer(self, capsys):
        """Should print formatted footer."""
        print_footer()
        captured = capsys.readouterr()
        assert "=" * 80 in captured.out


@pytest.fixture
def test_db(tmp_path):
    """Create a temporary database with test data."""
    db_path = tmp_path / "weather.db"

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create tables
    cursor.execute("""
        CREATE TABLE script_runs (
            run_id TEXT PRIMARY KEY,
            script_name TEXT NOT NULL,
            start_time TEXT NOT NULL,
            end_time TEXT,
            status TEXT DEFAULT 'running',
            exit_code INTEGER,
            error_message TEXT,
            error_traceback TEXT,
            total_items_expected INTEGER,
            total_items_succeeded INTEGER DEFAULT 0,
            total_items_failed INTEGER DEFAULT 0,
            total_records_inserted INTEGER DEFAULT 0,
            total_retries INTEGER DEFAULT 0,
            model_run TEXT,
            notes TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE script_run_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            item_name TEXT NOT NULL,
            item_type TEXT,
            status TEXT DEFAULT 'pending',
            start_time TEXT,
            end_time TEXT,
            records_inserted INTEGER DEFAULT 0,
            error_message TEXT,
            retry_count INTEGER DEFAULT 0
        )
    """)

    cursor.execute("""
        CREATE TABLE script_retries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            item_name TEXT,
            attempt_number INTEGER NOT NULL,
            attempt_time TEXT NOT NULL,
            error_message TEXT,
            error_type TEXT
        )
    """)

    # Insert test data
    now = datetime.now(timezone.utc)
    recent = (now - timedelta(hours=1)).isoformat()
    old = (now - timedelta(days=10)).isoformat()

    # Recent successful run
    cursor.execute("""
        INSERT INTO script_runs (
            run_id, script_name, start_time, end_time, status,
            total_items_expected, total_items_succeeded, total_items_failed,
            total_records_inserted, model_run
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        "abc123", "gfs_logger", recent,
        (now - timedelta(minutes=55)).isoformat(), "success",
        7, 7, 0, 100, "20260125 12Z"
    ))

    # Recent failed run
    cursor.execute("""
        INSERT INTO script_runs (
            run_id, script_name, start_time, end_time, status,
            total_items_expected, total_items_succeeded, total_items_failed,
            error_message, total_retries
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        "def456", "metar_logger", recent,
        (now - timedelta(minutes=50)).isoformat(), "partial",
        6, 4, 2, "Connection timeout on KDEN", 3
    ))

    # Old run (outside default window)
    cursor.execute("""
        INSERT INTO script_runs (
            run_id, script_name, start_time, end_time, status,
            total_items_succeeded
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, (
        "old789", "weather_logger", old,
        (datetime.now(timezone.utc) - timedelta(days=9, hours=23)).isoformat(),
        "success", 8
    ))

    # Add items for abc123
    cursor.execute("""
        INSERT INTO script_run_items (
            run_id, item_name, item_type, status, records_inserted
        ) VALUES (?, ?, ?, ?, ?)
    """, ("abc123", "f024", "forecast_hour", "success", 10))

    # Add retries for def456
    cursor.execute("""
        INSERT INTO script_retries (
            run_id, item_name, attempt_number, attempt_time, error_message, error_type
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, ("def456", "KDEN", 1, recent, "Connection timeout", "TimeoutError"))

    conn.commit()
    conn.close()

    return db_path


class TestShowSummary:
    """Tests for show_summary function."""

    def test_shows_recent_runs(self, test_db, capsys):
        """Should display recent runs."""
        with patch('check_weather_status.DB_PATH', test_db):
            show_summary(hours=24)

        captured = capsys.readouterr()
        assert "gfs_logger" in captured.out
        assert "metar_logger" in captured.out
        assert "abc123" in captured.out
        assert "def456" in captured.out

    def test_filters_by_script(self, test_db, capsys):
        """Should filter by script name."""
        with patch('check_weather_status.DB_PATH', test_db):
            show_summary(hours=24, script_filter="gfs")

        captured = capsys.readouterr()
        assert "gfs_logger" in captured.out
        assert "metar_logger" not in captured.out

    def test_shows_only_failures(self, test_db, capsys):
        """Should filter to failures only."""
        with patch('check_weather_status.DB_PATH', test_db):
            show_summary(hours=24, failures_only=True)

        captured = capsys.readouterr()
        assert "metar_logger" in captured.out
        assert "gfs_logger" not in captured.out

    def test_empty_results(self, test_db, capsys):
        """Should handle no results gracefully."""
        with patch('check_weather_status.DB_PATH', test_db):
            show_summary(hours=24, script_filter="nonexistent")

        captured = capsys.readouterr()
        assert "No runs found" in captured.out


class TestShowDetails:
    """Tests for show_details function."""

    def test_shows_run_details(self, test_db, capsys):
        """Should display full run details."""
        with patch('check_weather_status.DB_PATH', test_db):
            show_details("abc123")

        captured = capsys.readouterr()
        assert "gfs_logger" in captured.out
        assert "success" in captured.out
        assert "20260125 12Z" in captured.out
        assert "f024" in captured.out  # Item name

    def test_partial_id_match(self, test_db, capsys):
        """Should match partial run IDs."""
        with patch('check_weather_status.DB_PATH', test_db):
            show_details("abc")

        captured = capsys.readouterr()
        assert "gfs_logger" in captured.out

    def test_not_found(self, test_db, capsys):
        """Should handle not found gracefully."""
        with patch('check_weather_status.DB_PATH', test_db):
            show_details("nonexistent")

        captured = capsys.readouterr()
        assert "No run found" in captured.out


class TestShowReliability:
    """Tests for show_reliability function."""

    def test_shows_reliability_stats(self, test_db, capsys):
        """Should display reliability report."""
        with patch('check_weather_status.DB_PATH', test_db):
            show_reliability(days=7)

        captured = capsys.readouterr()
        assert "Reliability Report" in captured.out
        assert "gfs_logger" in captured.out
        assert "metar_logger" in captured.out

    def test_empty_results(self, test_db, capsys):
        """Should handle no results."""
        with patch('check_weather_status.DB_PATH', test_db):
            # Use a date range with no data
            show_reliability(days=0)

        captured = capsys.readouterr()
        assert "No runs found" in captured.out


class TestShowRecentFailures:
    """Tests for show_recent_failures function."""

    def test_shows_failures(self, test_db, capsys):
        """Should display recent failures."""
        with patch('check_weather_status.DB_PATH', test_db):
            show_recent_failures(hours=24)

        captured = capsys.readouterr()
        assert "metar_logger" in captured.out
        assert "Connection timeout" in captured.out

    def test_no_failures(self, test_db, capsys):
        """Should handle no failures gracefully."""
        # Create a new db with only success
        with patch('check_weather_status.DB_PATH', test_db):
            show_recent_failures(hours=0)

        captured = capsys.readouterr()
        assert "No failures" in captured.out


class TestMain:
    """Tests for main function."""

    def test_db_not_found(self, tmp_path, capsys):
        """Should return error when database not found."""
        fake_path = tmp_path / "nonexistent.db"
        with patch('check_weather_status.DB_PATH', fake_path):
            with patch('sys.argv', ['check_weather_status.py']):
                result = main()

        assert result == 1
        captured = capsys.readouterr()
        assert "Database not found" in captured.out

    def test_default_summary(self, test_db, capsys):
        """Should show summary by default."""
        with patch('check_weather_status.DB_PATH', test_db):
            with patch('sys.argv', ['check_weather_status.py']):
                result = main()

        assert result == 0
        captured = capsys.readouterr()
        assert "Weather Script Status" in captured.out

    def test_details_flag(self, test_db, capsys):
        """Should show details with --details flag."""
        with patch('check_weather_status.DB_PATH', test_db):
            with patch('sys.argv', ['check_weather_status.py', '--details', 'abc123']):
                result = main()

        assert result == 0
        captured = capsys.readouterr()
        assert "Run Details" in captured.out

    def test_reliability_flag(self, test_db, capsys):
        """Should show reliability with --reliability flag."""
        with patch('check_weather_status.DB_PATH', test_db):
            with patch('sys.argv', ['check_weather_status.py', '--reliability']):
                result = main()

        assert result == 0
        captured = capsys.readouterr()
        assert "Reliability Report" in captured.out

    def test_failures_flag(self, test_db, capsys):
        """Should show failures with --failures flag."""
        with patch('check_weather_status.DB_PATH', test_db):
            with patch('sys.argv', ['check_weather_status.py', '--failures']):
                result = main()

        assert result == 0
        captured = capsys.readouterr()
        assert "Failures" in captured.out or "metar_logger" in captured.out
