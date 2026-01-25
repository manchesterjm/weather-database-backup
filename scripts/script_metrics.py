#!/usr/bin/env python3
"""
Script Metrics - Centralized metrics tracking for weather collection scripts.

Provides fail-safe metrics logging with context managers for tracking script
execution, item-level progress, and retry attempts.

Usage:
    from script_metrics import ScriptMetrics

    with ScriptMetrics('gfs_logger', expected_items=7, model_run='20260125 12Z') as metrics:
        for hour in forecast_hours:
            with metrics.track_item(f'f{hour:03d}', 'forecast_hour') as item:
                # Do work...
                item.records_inserted = 1

        # Or manual tracking:
        metrics.item_failed('f048', 'Download timeout')
        metrics.record_retry(attempt=2, error='Connection reset', error_type='ConnectionError')

Key features:
- Fail-safe: If metrics logging fails, the script continues normally
- Automatic timing (start/end times captured)
- Automatic status determination (success/partial/failed based on items)
- Retry tracking with error details
"""

import logging
import sqlite3
import traceback
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Configuration
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "weather_data"
DB_PATH = DATA_DIR / "weather.db"

logger = logging.getLogger(__name__)


def get_metrics_connection() -> sqlite3.Connection:
    """Create a database connection with crash-resilient settings."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA wal_autocheckpoint=1000")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_metrics_tables():
    """Create metrics tables if they don't exist."""
    conn = get_metrics_connection()
    cursor = conn.cursor()

    # One row per script execution
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS script_runs (
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

    # Granular item tracking within a run
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS script_run_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            item_name TEXT NOT NULL,
            item_type TEXT,
            status TEXT DEFAULT 'pending',
            start_time TEXT,
            end_time TEXT,
            records_inserted INTEGER DEFAULT 0,
            error_message TEXT,
            retry_count INTEGER DEFAULT 0,
            FOREIGN KEY (run_id) REFERENCES script_runs(run_id)
        )
    """)

    # Detailed retry history
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS script_retries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            item_name TEXT,
            attempt_number INTEGER NOT NULL,
            attempt_time TEXT NOT NULL,
            error_message TEXT,
            error_type TEXT,
            FOREIGN KEY (run_id) REFERENCES script_runs(run_id)
        )
    """)

    # Indexes for efficient queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_runs_script ON script_runs(script_name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_runs_start ON script_runs(start_time)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_runs_status ON script_runs(status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_items_run ON script_run_items(run_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_retries_run ON script_retries(run_id)")

    conn.commit()
    conn.close()


@dataclass
class ItemTracker:  # pylint: disable=too-many-instance-attributes
    """Tracks a single item being processed within a script run."""
    name: str
    item_type: Optional[str] = None
    status: str = "pending"
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    records_inserted: int = 0
    error_message: Optional[str] = None
    retry_count: int = 0


@dataclass
class ScriptMetrics:  # pylint: disable=too-many-instance-attributes
    """
    Context manager for tracking script execution metrics.

    Fail-safe design: All database operations are wrapped in try/except.
    If metrics logging fails, it logs a warning but allows the script to continue.
    """
    script_name: str
    expected_items: Optional[int] = None
    model_run: Optional[str] = None
    notes: Optional[str] = None

    run_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    status: str = "running"
    exit_code: Optional[int] = None
    error_message: Optional[str] = None
    error_traceback: Optional[str] = None

    items: dict = field(default_factory=dict)
    total_retries: int = 0
    _initialized: bool = field(default=False, repr=False)

    def __enter__(self):
        """Initialize metrics tracking on context entry."""
        self.start_time = datetime.now(timezone.utc).isoformat()
        self._safe_init_db()
        self._safe_insert_run()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Finalize metrics on context exit."""
        self.end_time = datetime.now(timezone.utc).isoformat()

        if exc_type is not None:
            self.status = "failed"
            self.error_message = str(exc_val)
            self.error_traceback = "".join(traceback.format_exception(exc_type, exc_val, exc_tb))
            self.exit_code = 1
        else:
            self._determine_status()
            if self.exit_code is None:
                self.exit_code = 0 if self.status == "success" else 1

        self._safe_update_run()
        return False  # Don't suppress exceptions

    def _safe_init_db(self):
        """Initialize database tables, failing silently on error."""
        try:
            init_metrics_tables()
            self._initialized = True
        except (sqlite3.Error, OSError) as e:
            logger.warning("Failed to initialize metrics tables: %s", e)
            self._initialized = False

    def _safe_insert_run(self):
        """Insert initial run record, failing silently on error."""
        if not self._initialized:
            return
        try:
            conn = get_metrics_connection()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO script_runs (
                    run_id, script_name, start_time, status,
                    total_items_expected, model_run, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                self.run_id,
                self.script_name,
                self.start_time,
                self.status,
                self.expected_items,
                self.model_run,
                self.notes
            ))
            conn.commit()
            conn.close()
        except (sqlite3.Error, OSError) as e:
            logger.warning("Failed to insert run record: %s", e)

    def _safe_update_run(self):
        """Update run record with final state, failing silently on error."""
        if not self._initialized:
            return
        try:
            conn = get_metrics_connection()
            cursor = conn.cursor()

            # Calculate totals from items
            succeeded = sum(1 for i in self.items.values() if i.status == "success")
            failed = sum(1 for i in self.items.values() if i.status == "failed")
            records = sum(i.records_inserted for i in self.items.values())

            cursor.execute("""
                UPDATE script_runs SET
                    end_time = ?,
                    status = ?,
                    exit_code = ?,
                    error_message = ?,
                    error_traceback = ?,
                    total_items_succeeded = ?,
                    total_items_failed = ?,
                    total_records_inserted = ?,
                    total_retries = ?
                WHERE run_id = ?
            """, (
                self.end_time,
                self.status,
                self.exit_code,
                self.error_message,
                self.error_traceback,
                succeeded,
                failed,
                records,
                self.total_retries,
                self.run_id
            ))
            conn.commit()
            conn.close()
        except (sqlite3.Error, OSError) as e:
            logger.warning("Failed to update run record: %s", e)

    def _determine_status(self):
        """Determine final status based on item outcomes."""
        if not self.items:
            # No items tracked - use expected_items if set
            if self.expected_items is not None and self.expected_items > 0:
                self.status = "failed"
            else:
                self.status = "success"
            return

        succeeded = sum(1 for i in self.items.values() if i.status == "success")
        failed = sum(1 for i in self.items.values() if i.status == "failed")

        if failed == 0:
            self.status = "success"
        elif succeeded == 0:
            self.status = "failed"
        else:
            self.status = "partial"

    @contextmanager
    def track_item(self, name: str, item_type: Optional[str] = None):
        """
        Context manager for tracking a single item's processing.

        Usage:
            with metrics.track_item('f024', 'forecast_hour') as item:
                # Process the item...
                item.records_inserted = 5

            # Can also manually mark failure inside the block:
            with metrics.track_item('f048', 'forecast_hour') as item:
                if not do_work():
                    item.status = "failed"
                    item.error_message = "Work failed"
        """
        item = ItemTracker(name=name, item_type=item_type)
        item.status = "running"
        item.start_time = datetime.now(timezone.utc).isoformat()
        self.items[name] = item

        try:
            yield item
            # Only set to success if not already marked as failed
            if item.status == "running":
                item.status = "success"
        except Exception as e:
            item.status = "failed"
            item.error_message = str(e)
            raise
        finally:
            item.end_time = datetime.now(timezone.utc).isoformat()
            self._safe_insert_item(item)

    def _safe_insert_item(self, item: ItemTracker):
        """Insert item record, failing silently on error."""
        if not self._initialized:
            return
        try:
            conn = get_metrics_connection()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO script_run_items (
                    run_id, item_name, item_type, status,
                    start_time, end_time, records_inserted,
                    error_message, retry_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                self.run_id,
                item.name,
                item.item_type,
                item.status,
                item.start_time,
                item.end_time,
                item.records_inserted,
                item.error_message,
                item.retry_count
            ))
            conn.commit()
            conn.close()
        except (sqlite3.Error, OSError) as e:
            logger.warning("Failed to insert item record: %s", e)

    def item_succeeded(self, name: str, records_inserted: int = 0,
                       item_type: Optional[str] = None):
        """Manually mark an item as succeeded."""
        item = ItemTracker(
            name=name,
            item_type=item_type,
            status="success",
            start_time=datetime.now(timezone.utc).isoformat(),
            end_time=datetime.now(timezone.utc).isoformat(),
            records_inserted=records_inserted
        )
        self.items[name] = item
        self._safe_insert_item(item)

    def item_failed(self, name: str, error: str, item_type: Optional[str] = None):
        """Manually mark an item as failed."""
        item = ItemTracker(
            name=name,
            item_type=item_type,
            status="failed",
            start_time=datetime.now(timezone.utc).isoformat(),
            end_time=datetime.now(timezone.utc).isoformat(),
            error_message=error
        )
        self.items[name] = item
        self._safe_insert_item(item)

    def record_retry(self, attempt: int, error: str, error_type: Optional[str] = None,
                     item_name: Optional[str] = None):
        """Record a retry attempt."""
        self.total_retries += 1

        # Update item retry count if tracking specific item
        if item_name and item_name in self.items:
            self.items[item_name].retry_count += 1

        self._safe_insert_retry(attempt, error, error_type, item_name)

    def _safe_insert_retry(self, attempt: int, error: str,
                           error_type: Optional[str], item_name: Optional[str]):
        """Insert retry record, failing silently on error."""
        if not self._initialized:
            return
        try:
            conn = get_metrics_connection()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO script_retries (
                    run_id, item_name, attempt_number,
                    attempt_time, error_message, error_type
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                self.run_id,
                item_name,
                attempt,
                datetime.now(timezone.utc).isoformat(),
                error,
                error_type
            ))
            conn.commit()
            conn.close()
        except (sqlite3.Error, OSError) as e:
            logger.warning("Failed to insert retry record: %s", e)

    def set_exit_code(self, code: int):
        """Explicitly set the exit code."""
        self.exit_code = code

    def add_note(self, note: str):
        """Append a note to the run record."""
        if self.notes:
            self.notes += f"; {note}"
        else:
            self.notes = note


if __name__ == "__main__":
    # Self-test: Initialize tables and verify they exist
    logging.basicConfig(level=logging.INFO)
    print("Initializing metrics tables...")
    init_metrics_tables()
    print("Tables initialized successfully.")

    # Quick test of the context manager
    print("\nTesting ScriptMetrics context manager...")
    with ScriptMetrics("test_script", expected_items=3) as metrics:
        with metrics.track_item("item1", "test") as test_item:
            test_item.records_inserted = 1
        metrics.item_succeeded("item2", records_inserted=2, item_type="test")
        metrics.item_failed("item3", "Test failure", item_type="test")

    print(f"Run ID: {metrics.run_id}")
    print(f"Status: {metrics.status}")
    print(f"Items tracked: {len(metrics.items)}")
    print("Test complete.")
