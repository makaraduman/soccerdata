#!/bin/bash
# ============================================================
# Football Statistics Database - Master Setup Script
# ============================================================
# This script runs all SQL schema files in the correct order
#
# Usage:
#   ./00_setup_database.sh [postgres_host] [postgres_port] [postgres_user]
#
# Example:
#   ./00_setup_database.sh localhost 5432 postgres
#
# Note: You will be prompted for the PostgreSQL password
# ============================================================

set -e  # Exit on error

# Default values
DB_HOST=${1:-localhost}
DB_PORT=${2:-5432}
DB_USER=${3:-postgres}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "============================================================"
echo "Football Statistics Database Setup"
echo "============================================================"
echo "Host: $DB_HOST"
echo "Port: $DB_PORT"
echo "User: $DB_USER"
echo "============================================================"
echo ""

# Check if PostgreSQL is accessible
echo "Checking PostgreSQL connection..."
if ! PGPASSWORD=$PGPASSWORD psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres -c "SELECT 1;" > /dev/null 2>&1; then
    echo "Error: Cannot connect to PostgreSQL server"
    echo "Please ensure PostgreSQL is running and credentials are correct"
    echo "Set PGPASSWORD environment variable or will be prompted for password"
    exit 1
fi

echo "Connection successful!"
echo ""

# Run schema scripts in order
echo "Step 1: Creating database and schema..."
PGPASSWORD=$PGPASSWORD psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres -f "$SCRIPT_DIR/01_create_database.sql"
echo "✓ Database created"
echo ""

echo "Step 2: Creating tables..."
PGPASSWORD=$PGPASSWORD psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d football_stats -f "$SCRIPT_DIR/02_create_tables.sql"
echo "✓ Tables created"
echo ""

echo "Step 3: Inserting reference data..."
PGPASSWORD=$PGPASSWORD psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d football_stats -f "$SCRIPT_DIR/03_insert_reference_data.sql"
echo "✓ Reference data inserted"
echo ""

echo "============================================================"
echo "Database setup completed successfully!"
echo "============================================================"
echo ""
echo "Database: football_stats"
echo "Schema: football"
echo ""
echo "You can now connect to the database using:"
echo "  psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d football_stats"
echo ""
echo "Next steps:"
echo "  1. Configure config/db_config.yaml with your database credentials"
echo "  2. Run scripts/load_historical_data.py to load initial data"
echo "============================================================"
