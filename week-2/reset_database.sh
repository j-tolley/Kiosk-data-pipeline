#!/bin/bash

# Database Reset Script for Week 2 Kafka Consumer
# Removes all interaction data from kiosk_output table
# Preserves exhibitions table and all schema

set -e

#Would consider using source .env if we wanted to load variables in the future

# Load environment variables from .env
if [[ -f .env ]]; then
    export $(grep -v '^#' .env | xargs)
fi

# Use environment variables or defaults
export DB_HOST="${HOST:-localhost}"
export DB_PORT="${PORT:-5432}"
export DB_NAME="${DATABASE:-museum}"
export DB_USER="${DB_USERNAME:-postgres}"
export DB_PASSWORD="${DB_PASSWORD:-}"

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}=== Database Reset ===${NC}"
echo "Host: $DB_HOST"
echo "Database: $DB_NAME"
echo "User: $DB_USER"
echo ""

# Confirm before proceeding
read -p "Delete all data from kiosk_output table? (yes/no): " CONFIRM
if [[ "$CONFIRM" != "yes" ]]; then
    echo "Cancelled."
    exit 0
fi

# Execute reset using Python and psycopg2
echo -e "${YELLOW}Resetting database...${NC}"
python3 << 'EOPY'
import psycopg2
import sys
import os

try:
    # Connect to database
    conn = psycopg2.connect(
        host=os.environ.get('DB_HOST'),
        port=int(os.environ.get('DB_PORT', 5432)),
        database=os.environ.get('DB_NAME'),
        user=os.environ.get('DB_USER'),
        password=os.environ.get('DB_PASSWORD') or None,
    )
    
    cursor = conn.cursor()
    
    # Begin transaction
    cursor.execute("BEGIN TRANSACTION;")
    
    # Truncate table
    cursor.execute("TRUNCATE TABLE kiosk_output RESTART IDENTITY CASCADE;")
    
    # Get row counts
    cursor.execute("SELECT COUNT(*) as kiosk_output_rows FROM kiosk_output;")
    kiosk_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) as exhibitions_rows FROM exhibitions;")
    exhibitions_count = cursor.fetchone()[0]
    
    # Commit transaction
    conn.commit()
    
    cursor.close()
    conn.close()
    
    print(f"Kiosk output rows: {kiosk_count}")
    print(f"Exhibitions rows: {exhibitions_count}")
    
except psycopg2.Error as e:
    print(f"Database error: {e}", file=sys.stderr)
    sys.exit(1)
except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)
EOPY

echo ""
echo -e "${GREEN}✓ Database reset complete${NC}"
echo "- kiosk_output table cleared"
echo "- exhibitions table preserved"
echo "- All table structures intact"
