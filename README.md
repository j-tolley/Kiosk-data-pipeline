# Kiosk Data Pipeline

A two-week project that builds a complete data pipeline for the Liverpool Museum of Natural History (LMNH) kiosk feedback system. Visitor interactions recorded at kiosk terminals across museum exhibitions are extracted, validated, and stored in a PostgreSQL database for analysis.

## Project Overview

The project is split into two distinct phases:

| Folder | Approach | Description |
|--------|----------|-------------|
| [`week-1`](./week-1/) | Batch ETL | Extracts historical kiosk and exhibition data from S3, transforms and validates it, then loads it into a local or cloud (AWS RDS) PostgreSQL database. |
| [`week-2`](./week-2/) | Real-time streaming | Consumes live kiosk interaction messages from a Kafka topic, validates each message, deduplicates events, and inserts them into the cloud RDS database. |

## Repository Structure

```
Kiosk-data-pipeline/
├── week-1/             # Batch ETL pipeline
│   ├── extract.py      # Downloads data from S3 and produces CSV files
│   ├── transform.py    # Validates and formats CSV data
│   ├── load.py         # Creates the database schema and seeds data
│   ├── pipeline.py     # Orchestrates extract → transform → load
│   ├── schema.sql      # PostgreSQL schema definition
│   └── set_up.tf       # Terraform config for AWS RDS instance
├── week-2/             # Kafka streaming consumer
│   ├── pipeline.py     # Kafka consumer with validation and DB insertion
│   ├── test_pipeline.py# Pytest test suite for message validation
│   ├── set_up_ec2.tf   # Terraform config for EC2 instance
│   ├── reset_database.sh # Script to clear kiosk_output table
│   └── requirements.txt
├── requirements.txt    # Root-level Python dependencies
└── README.md
```

## Prerequisites

- Python 3.10+
- PostgreSQL (for local database use)
- AWS credentials (for S3 access and cloud deployment)
- Terraform (for provisioning AWS infrastructure)
- Kafka cluster credentials (for Week 2 streaming)

## Installation

Install Python dependencies from the root:

```bash
pip install -r requirements.txt
```

Or install the minimal set required for each week from within its directory:

```bash
pip install -r week-2/requirements.txt
```

## Database Schema

Both weeks share the same PostgreSQL schema (defined in [`week-1/schema.sql`](./week-1/schema.sql)):

- **`exhibitions`** — stores exhibition metadata (name, ID, site, floor, department, start date, description).
- **`kiosk_output`** — stores visitor interactions (timestamp, site, rating value, button type, and a foreign key to the relevant exhibition).

## Environment Variables

| Variable | Used in | Description |
|----------|---------|-------------|
| `ACCESS_KEY_ID` | Week 1 | AWS access key for S3 |
| `SECRET_ACCESS_KEY` | Week 1 | AWS secret key for S3 |
| `HOST` | Week 2 | RDS host address |
| `PORT` | Week 2 | RDS port (default `5432`) |
| `DATABASE` | Week 2 | Database name |
| `DB_USERNAME` | Week 2 | Database username |
| `DB_PASSWORD` | Week 2 | Database password |
| `BOOTSTRAP_SERVERS` | Week 2 | Kafka bootstrap server address |
| `SECURITY_PROTOCOL` | Week 2 | Kafka security protocol |
| `SASL_MECHANISM` | Week 2 | Kafka SASL mechanism |
| `USERNAME` | Week 2 | Kafka SASL username |
| `PASSWORD` | Week 2 | Kafka SASL password |

Store these in a `.env` file in the relevant week directory. **Never commit `.env` files to version control.**