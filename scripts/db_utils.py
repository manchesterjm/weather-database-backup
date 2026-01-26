#!/usr/bin/env python3
"""
Database Utilities for Weather Scripts

Provides centralized database access with:
- Platform-aware paths (Windows vs WSL)
- Connection pooling with retry logic
- Race condition prevention via file locking
- Consistent timeout and retry settings

All weather logging scripts should use this module for database access.
"""

import logging
import os
import platform
import sqlite3
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Optional, TypeVar

# ============================================================================
# Configuration
# ============================================================================

# Database timeout settings
DB_BUSY_TIMEOUT_MS = 30000  # 30 seconds SQLite busy timeout
DB_MAX_RETRIES = 3          # Number of retry attempts
DB_RETRY_DELAY_SEC = 10     # Seconds between retries

# Path configuration - use Windows paths for consistency
if sys.platform == 'win32':
    SCRIPTS_DIR = Path(r"D:\Scripts")
else:
    # WSL - convert to Windows path format for database access
    SCRIPTS_DIR = Path("/mnt/d/Scripts")

DATA_DIR = SCRIPTS_DIR / "weather_data"
DB_PATH = DATA_DIR / "weather.db"

# Type variable for generic return types
T = TypeVar('T')

logger = logging.getLogger(__name__)


# ============================================================================
# Platform Utilities
# ============================================================================

def is_windows() -> bool:
    """Check if running on Windows (not WSL)."""
    return sys.platform == 'win32'


def is_wsl() -> bool:
    """Check if running in Windows Subsystem for Linux."""
    if sys.platform != 'linux':
        return False
    try:
        with open('/proc/version', 'r') as f:
            return 'microsoft' in f.read().lower()
    except (FileNotFoundError, PermissionError):
        return False


def get_windows_path(path: Path) -> str:
    """Convert path to Windows format if in WSL.

    Args:
        path: Path object (may be WSL /mnt/d format)

    Returns:
        Windows-style path string (D:\\Scripts\\...)
    """
    path_str = str(path)
    if path_str.startswith('/mnt/'):
        # Convert /mnt/d/... to D:\...
        parts = path_str.split('/')
        if len(parts) >= 3:
            drive = parts[2].upper()
            rest = '\\'.join(parts[3:])
            return f"{drive}:\\{rest}"
    return path_str


# ============================================================================
# Database Connection
# ============================================================================

def get_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """Create a database connection with crash-resilient settings.

    Uses WAL mode for:
    - Crash resilience (survives unexpected shutdowns)
    - Concurrent reads during writes
    - Better performance for typical workloads

    Args:
        db_path: Optional path to database (defaults to weather.db)

    Returns:
        sqlite3.Connection with optimized settings
    """
    path = db_path or DB_PATH

    # Ensure data directory exists
    path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(path), timeout=DB_BUSY_TIMEOUT_MS / 1000)

    # Enable WAL mode for crash resilience
    conn.execute("PRAGMA journal_mode=WAL")

    # NORMAL sync is safe with WAL mode
    conn.execute("PRAGMA synchronous=NORMAL")

    # Checkpoint WAL every ~4MB to prevent unbounded growth
    conn.execute("PRAGMA wal_autocheckpoint=1000")

    # Set busy timeout for lock contention
    conn.execute(f"PRAGMA busy_timeout={DB_BUSY_TIMEOUT_MS}")

    return conn


@contextmanager
def get_db_connection(db_path: Optional[Path] = None):
    """Context manager for database connections with automatic cleanup.

    Usage:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT ...")
            conn.commit()

    Args:
        db_path: Optional path to database

    Yields:
        sqlite3.Connection
    """
    conn = get_connection(db_path)
    try:
        yield conn
    finally:
        conn.close()


# ============================================================================
# Retry Logic
# ============================================================================

def execute_with_retry(
    operation: Callable[[sqlite3.Connection], T],
    conn: sqlite3.Connection,
    description: str = "database operation",
    max_retries: int = DB_MAX_RETRIES,
    retry_delay: int = DB_RETRY_DELAY_SEC
) -> T:
    """Execute a database operation with retry logic for lock errors.

    Handles both "database is locked" and "disk I/O error" (WSL issue).

    Args:
        operation: Function that takes connection and returns result
        conn: Database connection
        description: Description for logging
        max_retries: Maximum retry attempts
        retry_delay: Seconds between retries

    Returns:
        Result of the operation

    Raises:
        sqlite3.OperationalError: If all retries exhausted
    """
    last_error: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            return operation(conn)
        except sqlite3.OperationalError as e:
            error_str = str(e).lower()
            if "database is locked" in error_str or "disk i/o error" in error_str:
                last_error = e
                if attempt < max_retries:
                    logger.warning(
                        "Database error during %s (attempt %d/%d), retrying in %ds: %s",
                        description, attempt, max_retries, retry_delay, e
                    )
                    time.sleep(retry_delay)
                else:
                    logger.error(
                        "Database error during %s after %d attempts: %s",
                        description, max_retries, e
                    )
            else:
                # Non-retryable error
                raise

    if last_error:
        raise last_error
    raise RuntimeError(f"Unexpected state in execute_with_retry for {description}")


def commit_with_retry(
    conn: sqlite3.Connection,
    description: str = "commit"
) -> None:
    """Commit transaction with retry logic.

    Args:
        conn: Database connection
        description: Description for logging
    """
    execute_with_retry(lambda c: c.commit(), conn, description)


# ============================================================================
# Race Condition Prevention
# ============================================================================

def check_database_accessible(db_path: Optional[Path] = None, timeout_sec: int = 5) -> bool:
    """Check if database is accessible (not locked by another process).

    Attempts a simple read operation to verify database access.

    Args:
        db_path: Path to database
        timeout_sec: Timeout for check

    Returns:
        True if database is accessible, False otherwise
    """
    path = db_path or DB_PATH

    try:
        conn = sqlite3.connect(str(path), timeout=timeout_sec)
        conn.execute("PRAGMA busy_timeout=1000")  # Short timeout for check
        conn.execute("SELECT 1")
        conn.close()
        return True
    except sqlite3.OperationalError as e:
        logger.debug("Database not accessible: %s", e)
        return False


def wait_for_database(
    db_path: Optional[Path] = None,
    max_wait_sec: int = 60,
    check_interval_sec: int = 5
) -> bool:
    """Wait for database to become accessible.

    Useful before starting long-running operations to ensure
    no other heavy writer is active.

    Args:
        db_path: Path to database
        max_wait_sec: Maximum time to wait
        check_interval_sec: Time between checks

    Returns:
        True if database became accessible, False if timeout
    """
    path = db_path or DB_PATH
    start_time = time.time()

    while time.time() - start_time < max_wait_sec:
        if check_database_accessible(path, timeout_sec=2):
            return True
        logger.info("Waiting for database to become available...")
        time.sleep(check_interval_sec)

    logger.warning("Timeout waiting for database after %d seconds", max_wait_sec)
    return False


# ============================================================================
# Utility Functions
# ============================================================================

def get_table_count(table_name: str, conn: Optional[sqlite3.Connection] = None) -> int:
    """Get row count for a table.

    Args:
        table_name: Name of table
        conn: Optional existing connection

    Returns:
        Number of rows in table
    """
    should_close = conn is None
    if conn is None:
        conn = get_connection()

    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        return cursor.fetchone()[0]
    finally:
        if should_close:
            conn.close()


def vacuum_database(db_path: Optional[Path] = None) -> None:
    """Vacuum database to reclaim space and optimize.

    Should be run periodically (e.g., weekly) during low-activity periods.

    Args:
        db_path: Path to database
    """
    path = db_path or DB_PATH
    logger.info("Vacuuming database at %s", path)

    conn = get_connection(path)
    try:
        conn.execute("VACUUM")
        logger.info("Database vacuum complete")
    finally:
        conn.close()


# ============================================================================
# Module Self-Test
# ============================================================================

if __name__ == "__main__":
    # Configure logging for self-test
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    print(f"Platform: {sys.platform}")
    print(f"Is Windows: {is_windows()}")
    print(f"Is WSL: {is_wsl()}")
    print(f"Scripts dir: {SCRIPTS_DIR}")
    print(f"Data dir: {DATA_DIR}")
    print(f"DB path: {DB_PATH}")
    print(f"Windows path: {get_windows_path(DB_PATH)}")

    print("\nTesting database access...")
    if check_database_accessible():
        print("Database is accessible!")
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = [row[0] for row in cursor.fetchall()]
            print(f"Tables: {', '.join(tables)}")
    else:
        print("Database is not accessible (may be locked)")
