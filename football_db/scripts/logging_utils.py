"""
Logging utilities for football statistics data loading.

This module provides structured logging functionality with
file and console output, rotation, and formatting.
"""

import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import Optional
from datetime import datetime


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors for console output."""

    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
    }
    RESET = '\033[0m'

    def format(self, record):
        """Format log record with color."""
        log_color = self.COLORS.get(record.levelname, self.RESET)
        record.levelname = f"{log_color}{record.levelname}{self.RESET}"
        return super().format(record)


def setup_logging(
    log_dir: Optional[str] = None,
    log_level: str = 'INFO',
    max_log_size_mb: int = 100,
    backup_count: int = 5,
    console_output: bool = True,
    file_output: bool = True,
    log_name: str = 'football_db'
) -> logging.Logger:
    """
    Set up logging configuration.

    Args:
        log_dir: Directory for log files (default: logs/)
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        max_log_size_mb: Maximum size of each log file in MB
        backup_count: Number of backup log files to keep
        console_output: Whether to output to console
        file_output: Whether to output to file
        log_name: Name for the logger

    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(log_name)
    logger.setLevel(getattr(logging, log_level.upper()))

    # Remove existing handlers
    logger.handlers = []

    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    simple_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    colored_formatter = ColoredFormatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Console handler
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(colored_formatter)
        logger.addHandler(console_handler)

    # File handlers
    if file_output:
        if log_dir is None:
            # Default to logs directory in parent of scripts directory
            script_dir = Path(__file__).parent
            log_dir = script_dir.parent / 'logs'
        else:
            log_dir = Path(log_dir)

        log_dir.mkdir(parents=True, exist_ok=True)

        # Main log file (rotating)
        main_log_file = log_dir / f'{log_name}.log'
        file_handler = RotatingFileHandler(
            main_log_file,
            maxBytes=max_log_size_mb * 1024 * 1024,
            backupCount=backup_count
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(detailed_formatter)
        logger.addHandler(file_handler)

        # Error log file (only errors and above)
        error_log_file = log_dir / f'{log_name}_errors.log'
        error_handler = RotatingFileHandler(
            error_log_file,
            maxBytes=max_log_size_mb * 1024 * 1024,
            backupCount=backup_count
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(detailed_formatter)
        logger.addHandler(error_handler)

    return logger


class DataLoadLogger:
    """
    Logger for data loading operations with database tracking.

    This class provides methods to log data loading operations
    to both log files and the database.
    """

    def __init__(self, db_connection, logger: logging.Logger):
        """
        Initialize data load logger.

        Args:
            db_connection: DatabaseConnection instance
            logger: Logger instance
        """
        self.db = db_connection
        self.logger = logger
        self.load_id: Optional[int] = None

    def start_load(
        self,
        source_name: str,
        load_type: str,
        target_table: str,
        league_season_id: Optional[int] = None
    ) -> int:
        """
        Start a data load operation and log to database.

        Args:
            source_name: Name of data source
            load_type: Type of load (initial_load, daily_update, etc.)
            target_table: Target table name
            league_season_id: Optional league season ID

        Returns:
            Load ID from database
        """
        # Get source ID
        query = "SELECT source_id FROM data_sources WHERE source_name = %s"
        result = self.db.execute_query(query, (source_name,))
        source_id = result[0]['source_id'] if result else None

        # Insert load log record
        insert_query = """
            INSERT INTO data_load_log (
                source_id, load_type, target_table, league_season_id,
                load_start, status
            )
            VALUES (%s, %s, %s, %s, %s, 'running')
            RETURNING load_id
        """
        result = self.db.execute_query(
            insert_query,
            (source_id, load_type, target_table, league_season_id, datetime.now())
        )
        self.load_id = result[0]['load_id']

        self.logger.info(
            f"Started {load_type} for {target_table} from {source_name} "
            f"(load_id: {self.load_id})"
        )

        return self.load_id

    def update_progress(
        self,
        records_processed: int = 0,
        records_inserted: int = 0,
        records_updated: int = 0,
        records_failed: int = 0
    ) -> None:
        """
        Update progress of current load operation.

        Args:
            records_processed: Number of records processed
            records_inserted: Number of records inserted
            records_updated: Number of records updated
            records_failed: Number of records failed
        """
        if self.load_id is None:
            return

        query = """
            UPDATE data_load_log
            SET records_processed = %s,
                records_inserted = %s,
                records_updated = %s,
                records_failed = %s
            WHERE load_id = %s
        """
        self.db.execute_query(
            query,
            (records_processed, records_inserted, records_updated,
             records_failed, self.load_id),
            fetch=False
        )

    def complete_load(
        self,
        status: str = 'completed',
        error_message: Optional[str] = None
    ) -> None:
        """
        Mark load operation as complete.

        Args:
            status: Final status (completed, failed, partial)
            error_message: Optional error message
        """
        if self.load_id is None:
            return

        query = """
            UPDATE data_load_log
            SET load_end = %s,
                status = %s,
                error_message = %s
            WHERE load_id = %s
        """
        self.db.execute_query(
            query,
            (datetime.now(), status, error_message, self.load_id),
            fetch=False
        )

        if status == 'completed':
            self.logger.info(f"Load completed successfully (load_id: {self.load_id})")
        else:
            self.logger.error(
                f"Load {status} (load_id: {self.load_id}): {error_message}"
            )

        self.load_id = None

    def log_error(self, error: Exception, context: str = "") -> None:
        """
        Log an error with context.

        Args:
            error: Exception object
            context: Additional context about the error
        """
        error_msg = f"{context}: {str(error)}" if context else str(error)
        self.logger.error(error_msg, exc_info=True)

        if self.load_id is not None:
            self.complete_load(status='failed', error_message=error_msg)


def log_function_call(func):
    """
    Decorator to log function entry and exit.

    Example:
        @log_function_call
        def my_function(arg1, arg2):
            pass
    """
    def wrapper(*args, **kwargs):
        logger = logging.getLogger('football_db')
        logger.debug(f"Entering {func.__name__}")
        try:
            result = func(*args, **kwargs)
            logger.debug(f"Exiting {func.__name__}")
            return result
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}", exc_info=True)
            raise
    return wrapper


if __name__ == "__main__":
    # Test logging setup
    logger = setup_logging(log_level='DEBUG')

    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")

    print("\nLog files created in logs/ directory")
