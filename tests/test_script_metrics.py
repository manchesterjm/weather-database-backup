#!/usr/bin/env python3
"""
Tests for script_metrics module.

Tests cover the ScriptMetrics context manager, ItemTracker, and database operations.
Uses pytest fixtures for isolation and cleanup.
"""

import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# Import module under test
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from script_metrics import (
    ScriptMetrics, ItemTracker, get_metrics_connection,
    init_metrics_tables, DB_PATH
)


@pytest.fixture
def temp_db(tmp_path):
    """Create a temporary database for testing."""
    db_file = tmp_path / "test_weather.db"
    with patch('script_metrics.DB_PATH', db_file):
        init_metrics_tables()
        yield db_file


@pytest.fixture
def mock_db_path(tmp_path):
    """Patch DB_PATH to use temporary directory."""
    db_file = tmp_path / "test_weather.db"
    with patch('script_metrics.DB_PATH', db_file):
        yield db_file


class TestItemTracker:
    """Tests for ItemTracker dataclass."""

    def test_default_values(self):
        """ItemTracker should have sensible defaults."""
        item = ItemTracker(name="test_item")
        assert item.name == "test_item"
        assert item.item_type is None
        assert item.status == "pending"
        assert item.start_time is None
        assert item.end_time is None
        assert item.records_inserted == 0
        assert item.error_message is None
        assert item.retry_count == 0

    def test_custom_values(self):
        """ItemTracker should accept custom values."""
        item = ItemTracker(
            name="custom",
            item_type="forecast",
            status="success",
            records_inserted=5,
            retry_count=2
        )
        assert item.name == "custom"
        assert item.item_type == "forecast"
        assert item.status == "success"
        assert item.records_inserted == 5
        assert item.retry_count == 2


class TestScriptMetricsInit:
    """Tests for ScriptMetrics initialization."""

    def test_default_values(self):
        """ScriptMetrics should have sensible defaults."""
        metrics = ScriptMetrics(script_name="test_script")
        assert metrics.script_name == "test_script"
        assert metrics.expected_items is None
        assert metrics.model_run is None
        assert metrics.notes is None
        assert metrics.status == "running"
        assert len(metrics.run_id) == 8
        assert metrics.items == {}
        assert metrics.total_retries == 0

    def test_custom_values(self):
        """ScriptMetrics should accept custom values."""
        metrics = ScriptMetrics(
            script_name="gfs_logger",
            expected_items=7,
            model_run="20260125 12Z",
            notes="Test run"
        )
        assert metrics.script_name == "gfs_logger"
        assert metrics.expected_items == 7
        assert metrics.model_run == "20260125 12Z"
        assert metrics.notes == "Test run"


class TestScriptMetricsContextManager:
    """Tests for ScriptMetrics context manager behavior."""

    def test_context_manager_sets_times(self, mock_db_path):
        """Context manager should set start and end times."""
        with ScriptMetrics("test") as metrics:
            assert metrics.start_time is not None
            assert metrics.end_time is None

        assert metrics.end_time is not None

    def test_context_manager_success_status(self, mock_db_path):
        """Context manager should set success status when no items fail."""
        with ScriptMetrics("test") as metrics:
            metrics.item_succeeded("item1")

        assert metrics.status == "success"
        assert metrics.exit_code == 0

    def test_context_manager_failed_status_all_fail(self, mock_db_path):
        """Status should be 'failed' when all items fail."""
        with ScriptMetrics("test") as metrics:
            metrics.item_failed("item1", "Error 1")
            metrics.item_failed("item2", "Error 2")

        assert metrics.status == "failed"
        assert metrics.exit_code == 1

    def test_context_manager_partial_status(self, mock_db_path):
        """Status should be 'partial' when some items succeed and some fail."""
        with ScriptMetrics("test") as metrics:
            metrics.item_succeeded("item1")
            metrics.item_failed("item2", "Error")

        assert metrics.status == "partial"
        assert metrics.exit_code == 1

    def test_context_manager_exception_handling(self, mock_db_path):
        """Context manager should capture exception info."""
        with pytest.raises(ValueError):
            with ScriptMetrics("test") as metrics:
                raise ValueError("Test error")

        assert metrics.status == "failed"
        assert metrics.exit_code == 1
        assert "Test error" in metrics.error_message
        assert "ValueError" in metrics.error_traceback

    def test_context_manager_no_items_expected_success(self, mock_db_path):
        """No items and no expected items should be success."""
        with ScriptMetrics("test") as metrics:
            pass

        assert metrics.status == "success"

    def test_context_manager_no_items_but_expected_failed(self, mock_db_path):
        """No items but expected items should be failed."""
        with ScriptMetrics("test", expected_items=5) as metrics:
            pass

        assert metrics.status == "failed"


class TestTrackItem:
    """Tests for track_item context manager."""

    def test_track_item_success(self, mock_db_path):
        """track_item should mark item as success on normal exit."""
        with ScriptMetrics("test") as metrics:
            with metrics.track_item("item1", "test_type") as item:
                item.records_inserted = 3

        assert "item1" in metrics.items
        assert metrics.items["item1"].status == "success"
        assert metrics.items["item1"].records_inserted == 3
        assert metrics.items["item1"].item_type == "test_type"

    def test_track_item_failure_on_exception(self, mock_db_path):
        """track_item should mark item as failed on exception."""
        with pytest.raises(RuntimeError):
            with ScriptMetrics("test") as metrics:
                with metrics.track_item("item1") as item:
                    raise RuntimeError("Processing failed")

        assert metrics.items["item1"].status == "failed"
        assert "Processing failed" in metrics.items["item1"].error_message

    def test_track_item_manual_failure(self, mock_db_path):
        """track_item should respect manually set failure status."""
        with ScriptMetrics("test") as metrics:
            with metrics.track_item("item1") as item:
                item.status = "failed"
                item.error_message = "Manual failure"

        assert metrics.items["item1"].status == "failed"
        assert metrics.items["item1"].error_message == "Manual failure"

    def test_track_item_sets_times(self, mock_db_path):
        """track_item should set start and end times."""
        with ScriptMetrics("test") as metrics:
            with metrics.track_item("item1") as item:
                assert item.start_time is not None
                assert item.end_time is None

        assert metrics.items["item1"].end_time is not None


class TestManualItemMethods:
    """Tests for item_succeeded and item_failed methods."""

    def test_item_succeeded(self, mock_db_path):
        """item_succeeded should create a success item."""
        with ScriptMetrics("test") as metrics:
            metrics.item_succeeded("item1", records_inserted=5, item_type="forecast")

        assert "item1" in metrics.items
        assert metrics.items["item1"].status == "success"
        assert metrics.items["item1"].records_inserted == 5
        assert metrics.items["item1"].item_type == "forecast"

    def test_item_failed(self, mock_db_path):
        """item_failed should create a failed item."""
        with ScriptMetrics("test") as metrics:
            metrics.item_failed("item1", "Download failed", item_type="metar")

        assert "item1" in metrics.items
        assert metrics.items["item1"].status == "failed"
        assert metrics.items["item1"].error_message == "Download failed"
        assert metrics.items["item1"].item_type == "metar"


class TestRetryTracking:
    """Tests for retry recording."""

    def test_record_retry_increments_total(self, mock_db_path):
        """record_retry should increment total_retries."""
        with ScriptMetrics("test") as metrics:
            metrics.record_retry(1, "Connection timeout", "TimeoutError")
            metrics.record_retry(2, "Connection reset", "ConnectionError")

        assert metrics.total_retries == 2

    def test_record_retry_with_item_name(self, mock_db_path):
        """record_retry should update item retry_count if item exists."""
        with ScriptMetrics("test") as metrics:
            with metrics.track_item("item1") as item:
                metrics.record_retry(1, "Retry error", item_name="item1")
                metrics.record_retry(2, "Retry error", item_name="item1")

        assert metrics.items["item1"].retry_count == 2


class TestDatabaseOperations:
    """Tests for database operations."""

    def test_tables_created(self, mock_db_path):
        """init_metrics_tables should create all required tables."""
        init_metrics_tables()

        conn = sqlite3.connect(mock_db_path)
        cursor = conn.cursor()

        # Check tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}

        assert "script_runs" in tables
        assert "script_run_items" in tables
        assert "script_retries" in tables

        conn.close()

    def test_run_record_inserted(self, mock_db_path):
        """Context manager should insert run record."""
        with ScriptMetrics("test_script") as metrics:
            metrics.item_succeeded("item1")

        conn = sqlite3.connect(mock_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM script_runs WHERE run_id = ?", (metrics.run_id,))
        row = cursor.fetchone()
        conn.close()

        assert row is not None
        assert row[1] == "test_script"  # script_name
        assert row[4] == "success"  # status

    def test_item_record_inserted(self, mock_db_path):
        """track_item should insert item records."""
        with ScriptMetrics("test") as metrics:
            with metrics.track_item("item1", "forecast") as item:
                item.records_inserted = 10

        conn = sqlite3.connect(mock_db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM script_run_items WHERE run_id = ?",
            (metrics.run_id,)
        )
        row = cursor.fetchone()
        conn.close()

        assert row is not None
        assert row[2] == "item1"  # item_name
        assert row[3] == "forecast"  # item_type
        assert row[4] == "success"  # status

    def test_retry_record_inserted(self, mock_db_path):
        """record_retry should insert retry records."""
        with ScriptMetrics("test") as metrics:
            metrics.record_retry(1, "Test error", "TestError", "item1")

        conn = sqlite3.connect(mock_db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM script_retries WHERE run_id = ?",
            (metrics.run_id,)
        )
        row = cursor.fetchone()
        conn.close()

        assert row is not None
        assert row[2] == "item1"  # item_name
        assert row[3] == 1  # attempt_number


class TestFailSafeBehavior:
    """Tests for fail-safe behavior when database operations fail."""

    def test_db_init_failure_continues(self, tmp_path):
        """Script should continue if database init fails."""
        # Use an invalid path
        invalid_path = tmp_path / "nonexistent" / "subdir" / "db.sqlite"
        with patch('script_metrics.DB_PATH', invalid_path):
            with ScriptMetrics("test") as metrics:
                metrics.item_succeeded("item1")

            # Should complete without exception
            assert metrics.status == "success"

    def test_db_insert_failure_continues(self, mock_db_path):
        """Script should continue if database insert fails."""
        init_metrics_tables()

        # Mock the connection to fail on execute
        with patch('script_metrics.get_metrics_connection') as mock_conn:
            mock_cursor = MagicMock()
            mock_cursor.execute.side_effect = sqlite3.Error("Insert failed")
            mock_conn.return_value.cursor.return_value = mock_cursor

            # Re-patch to allow init but fail on inserts
            with patch('script_metrics.init_metrics_tables'):
                with ScriptMetrics("test") as metrics:
                    metrics._initialized = True
                    metrics.item_succeeded("item1")

                # Should complete without exception
                assert "item1" in metrics.items


class TestAddNote:
    """Tests for add_note method."""

    def test_add_first_note(self, mock_db_path):
        """add_note should set notes when empty."""
        with ScriptMetrics("test") as metrics:
            metrics.add_note("First note")
            assert metrics.notes == "First note"

    def test_add_subsequent_notes(self, mock_db_path):
        """add_note should append with semicolon."""
        with ScriptMetrics("test") as metrics:
            metrics.add_note("First")
            metrics.add_note("Second")
            assert metrics.notes == "First; Second"


class TestSetExitCode:
    """Tests for set_exit_code method."""

    def test_set_exit_code_overrides(self, mock_db_path):
        """set_exit_code should override automatic exit code."""
        with ScriptMetrics("test") as metrics:
            metrics.item_succeeded("item1")
            metrics.set_exit_code(42)

        assert metrics.exit_code == 42


class TestGetMetricsConnection:
    """Tests for get_metrics_connection function."""

    def test_connection_pragmas(self, mock_db_path):
        """Connection should have WAL mode and other pragmas set."""
        init_metrics_tables()
        conn = get_metrics_connection()

        cursor = conn.cursor()
        cursor.execute("PRAGMA journal_mode")
        journal_mode = cursor.fetchone()[0]
        assert journal_mode.lower() == "wal"

        conn.close()
