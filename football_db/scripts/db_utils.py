"""
Database utility functions for football statistics database.

This module provides connection management, query execution,
and common database operations.
"""

import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
import psycopg2
from psycopg2 import pool, sql
from psycopg2.extras import RealDictCursor, execute_values
import yaml
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)


class DatabaseConfig:
    """Database configuration manager."""

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize database configuration.

        Args:
            config_path: Path to configuration YAML file.
                        If None, looks for config/db_config.yaml
        """
        if config_path is None:
            # Look for config file in config directory
            script_dir = Path(__file__).parent
            config_path = script_dir.parent / "config" / "db_config.yaml"

        self.config_path = Path(config_path)
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        if not self.config_path.exists():
            raise FileNotFoundError(
                f"Configuration file not found: {self.config_path}\n"
                f"Please copy db_config.yaml.example to db_config.yaml "
                f"and update with your credentials."
            )

        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)

        # Override with environment variables if present
        if 'POSTGRES_HOST' in os.environ:
            config['database']['host'] = os.environ['POSTGRES_HOST']
        if 'POSTGRES_PORT' in os.environ:
            config['database']['port'] = int(os.environ['POSTGRES_PORT'])
        if 'POSTGRES_DB' in os.environ:
            config['database']['database'] = os.environ['POSTGRES_DB']
        if 'POSTGRES_USER' in os.environ:
            config['database']['user'] = os.environ['POSTGRES_USER']
        if 'POSTGRES_PASSWORD' in os.environ:
            config['database']['password'] = os.environ['POSTGRES_PASSWORD']

        return config

    @property
    def db_params(self) -> Dict[str, Any]:
        """Get database connection parameters."""
        return {
            'host': self.config['database']['host'],
            'port': self.config['database']['port'],
            'database': self.config['database']['database'],
            'user': self.config['database']['user'],
            'password': self.config['database']['password'],
        }

    @property
    def schema(self) -> str:
        """Get database schema name."""
        return self.config['database'].get('schema', 'football')

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key."""
        keys = key.split('.')
        value = self.config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default
            if value is None:
                return default
        return value


class DatabaseConnection:
    """Database connection manager with connection pooling."""

    def __init__(self, config: DatabaseConfig):
        """
        Initialize database connection manager.

        Args:
            config: DatabaseConfig instance
        """
        self.config = config
        self.pool: Optional[pool.SimpleConnectionPool] = None
        self._initialize_pool()

    def _initialize_pool(self) -> None:
        """Initialize connection pool."""
        try:
            min_conn = self.config.config['database'].get('min_connections', 2)
            max_conn = self.config.config['database'].get('max_connections', 10)

            self.pool = pool.SimpleConnectionPool(
                min_conn,
                max_conn,
                **self.config.db_params
            )
            logger.info("Database connection pool initialized")
        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {e}")
            raise

    @contextmanager
    def get_connection(self):
        """
        Get a database connection from the pool.

        Yields:
            psycopg2 connection object

        Example:
            with db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM teams")
        """
        conn = None
        try:
            conn = self.pool.getconn()
            # Set schema search path
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {self.config.schema}, public")
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                self.pool.putconn(conn)

    @contextmanager
    def get_cursor(self, dict_cursor: bool = True):
        """
        Get a database cursor.

        Args:
            dict_cursor: If True, returns RealDictCursor for dict-like access

        Yields:
            Database cursor

        Example:
            with db.get_cursor() as cur:
                cur.execute("SELECT * FROM teams")
                rows = cur.fetchall()
        """
        with self.get_connection() as conn:
            cursor_factory = RealDictCursor if dict_cursor else None
            cursor = conn.cursor(cursor_factory=cursor_factory)
            try:
                yield cursor
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"Cursor operation error: {e}")
                raise
            finally:
                cursor.close()

    def execute_query(
        self,
        query: str,
        params: Optional[Union[Tuple, Dict]] = None,
        fetch: bool = True
    ) -> Optional[List[Dict]]:
        """
        Execute a SQL query.

        Args:
            query: SQL query string
            params: Query parameters
            fetch: If True, fetch and return results

        Returns:
            List of result rows as dictionaries (if fetch=True)
        """
        with self.get_cursor() as cur:
            cur.execute(query, params)
            if fetch:
                return cur.fetchall()
        return None

    def execute_many(
        self,
        query: str,
        data: List[Union[Tuple, Dict]]
    ) -> int:
        """
        Execute a query with multiple parameter sets.

        Args:
            query: SQL query with placeholders
            data: List of parameter tuples/dicts

        Returns:
            Number of rows affected
        """
        with self.get_cursor(dict_cursor=False) as cur:
            cur.executemany(query, data)
            return cur.rowcount

    def bulk_insert(
        self,
        table: str,
        columns: List[str],
        data: List[Tuple]
    ) -> int:
        """
        Perform bulk insert using execute_values for better performance.

        Args:
            table: Table name
            columns: List of column names
            data: List of tuples with values

        Returns:
            Number of rows inserted
        """
        if not data:
            return 0

        column_names = ', '.join(columns)
        query = f"INSERT INTO {table} ({column_names}) VALUES %s"

        with self.get_cursor(dict_cursor=False) as cur:
            execute_values(cur, query, data)
            return cur.rowcount

    def upsert(
        self,
        table: str,
        columns: List[str],
        data: List[Tuple],
        conflict_columns: List[str],
        update_columns: Optional[List[str]] = None
    ) -> int:
        """
        Perform upsert (INSERT ... ON CONFLICT ... UPDATE).

        Args:
            table: Table name
            columns: List of column names
            data: List of tuples with values
            conflict_columns: Columns to check for conflicts
            update_columns: Columns to update on conflict (None = all except conflict)

        Returns:
            Number of rows affected
        """
        if not data:
            return 0

        if update_columns is None:
            update_columns = [c for c in columns if c not in conflict_columns]

        column_names = ', '.join(columns)
        conflict_cols = ', '.join(conflict_columns)
        update_set = ', '.join([f"{c} = EXCLUDED.{c}" for c in update_columns])

        query = f"""
            INSERT INTO {table} ({column_names})
            VALUES %s
            ON CONFLICT ({conflict_cols})
            DO UPDATE SET {update_set}
        """

        with self.get_cursor(dict_cursor=False) as cur:
            execute_values(cur, query, data)
            return cur.rowcount

    def get_league_id(self, league_code: str) -> Optional[int]:
        """Get league ID by code."""
        query = "SELECT league_id FROM leagues WHERE league_code = %s"
        result = self.execute_query(query, (league_code,))
        return result[0]['league_id'] if result else None

    def get_season_id(self, season_name: str) -> Optional[int]:
        """Get season ID by name."""
        query = "SELECT season_id FROM seasons WHERE season_name = %s"
        result = self.execute_query(query, (season_name,))
        return result[0]['season_id'] if result else None

    def get_league_season_id(self, league_code: str, season_name: str) -> Optional[int]:
        """Get league_season_id for a league and season combination."""
        query = """
            SELECT ls.league_season_id
            FROM league_seasons ls
            JOIN leagues l ON ls.league_id = l.league_id
            JOIN seasons s ON ls.season_id = s.season_id
            WHERE l.league_code = %s AND s.season_name = %s
        """
        result = self.execute_query(query, (league_code, season_name))
        return result[0]['league_season_id'] if result else None

    def get_or_create_team(self, team_name: str, country: Optional[str] = None) -> int:
        """
        Get team ID or create if doesn't exist.

        Args:
            team_name: Team name
            country: Country (optional)

        Returns:
            Team ID
        """
        # First try to find existing team
        query = "SELECT team_id FROM teams WHERE team_name = %s"
        result = self.execute_query(query, (team_name,))

        if result:
            return result[0]['team_id']

        # Create new team
        insert_query = """
            INSERT INTO teams (team_name, country)
            VALUES (%s, %s)
            RETURNING team_id
        """
        result = self.execute_query(insert_query, (team_name, country))
        logger.info(f"Created new team: {team_name}")
        return result[0]['team_id']

    def get_or_create_player(
        self,
        player_name: str,
        **kwargs
    ) -> int:
        """
        Get player ID or create if doesn't exist.

        Args:
            player_name: Player name
            **kwargs: Additional player attributes (birth_date, nationality, position, etc.)

        Returns:
            Player ID
        """
        # Try to find existing player
        query = "SELECT player_id FROM players WHERE player_name = %s"
        result = self.execute_query(query, (player_name,))

        if result:
            return result[0]['player_id']

        # Create new player
        columns = ['player_name'] + list(kwargs.keys())
        values = [player_name] + list(kwargs.values())
        placeholders = ', '.join(['%s'] * len(columns))
        column_names = ', '.join(columns)

        insert_query = f"""
            INSERT INTO players ({column_names})
            VALUES ({placeholders})
            RETURNING player_id
        """
        result = self.execute_query(insert_query, tuple(values))
        logger.info(f"Created new player: {player_name}")
        return result[0]['player_id']

    def close(self) -> None:
        """Close all connections in the pool."""
        if self.pool:
            self.pool.closeall()
            logger.info("Database connection pool closed")


# Singleton instance
_db_connection: Optional[DatabaseConnection] = None


def get_db_connection(config_path: Optional[str] = None) -> DatabaseConnection:
    """
    Get or create database connection singleton.

    Args:
        config_path: Path to config file (only used on first call)

    Returns:
        DatabaseConnection instance
    """
    global _db_connection
    if _db_connection is None:
        config = DatabaseConfig(config_path)
        _db_connection = DatabaseConnection(config)
    return _db_connection


if __name__ == "__main__":
    # Test database connection
    import logging
    logging.basicConfig(level=logging.INFO)

    try:
        db = get_db_connection()
        print("Testing database connection...")

        # Test query
        with db.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) as count FROM leagues")
            result = cur.fetchone()
            print(f"Number of leagues in database: {result['count']}")

        print("Database connection test successful!")

    except Exception as e:
        print(f"Database connection test failed: {e}")
        sys.exit(1)
