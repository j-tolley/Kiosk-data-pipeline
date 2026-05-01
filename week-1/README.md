# Week 1 — Batch ETL Pipeline

This week implements a one-off batch **Extract → Transform → Load** pipeline. It pulls historical kiosk interaction data and exhibition metadata from an AWS S3 bucket, validates and formats it, then loads it into a PostgreSQL database (local or AWS RDS).

## Files

| File | Description |
|------|-------------|
| `pipeline.py` | CLI entry point that orchestrates all three ETL phases |
| `extract.py` | Downloads files from S3, produces `kiosk_output.csv` and `exhibitions.csv` |
| `transform.py` | Validates and formats both CSV files |
| `load.py` | Creates the database schema and loads the CSV data |
| `schema.sql` | PostgreSQL table definitions for `exhibitions` and `kiosk_output` |
| `set_up.tf` | Terraform configuration for provisioning an AWS RDS PostgreSQL instance |

## Prerequisites

- Python 3.10+
- `pip install -r ../requirements.txt`
- AWS credentials with read access to the `sigma-resources-museum` S3 bucket
- PostgreSQL installed locally **or** AWS credentials for RDS provisioning
- Terraform (only required for cloud database deployment)

## Environment Variables

Create a `.env` file in this directory:

```dotenv
ACCESS_KEY_ID=your_aws_access_key
SECRET_ACCESS_KEY=your_aws_secret_key
```

For cloud database use, connection details are generated automatically by Terraform and saved to `rds_config.json`.

## Usage

Run the full pipeline (extract → transform → load) against a local database:

```bash
python pipeline.py
```

Run against a cloud AWS RDS database:

```bash
python pipeline.py --db-type cloud
```

Run specific steps only:

```bash
# Extract only
python pipeline.py --steps extract

# Transform and load only
python pipeline.py --steps transform load
```

Additional options:

```bash
# Custom file paths
python pipeline.py --exhibitions-file /path/to/exh.csv --kiosk-file /path/to/kiosk.csv

# Custom local database name
python pipeline.py --db-name my_museum

# Custom RDS config file
python pipeline.py --db-type cloud --config-file /path/to/rds_config.json

# Verbose debug logging
python pipeline.py --log-level DEBUG

# Tear down RDS infrastructure
python pipeline.py --db-type cloud --terraform-destroy
```

## ETL Phases

### Extract (`extract.py`)

1. Connects to the `sigma-resources-museum` S3 bucket using AWS credentials from `.env`.
2. Downloads all files into a local `./data/` directory.
3. Combines `lmnh_hist_data_*.csv` files into `kiosk_output.csv`.
4. Combines `lmnh_exhibition_*.json` files into `exhibitions.csv`.

### Transform (`transform.py`)

**Exhibitions (`exhibitions.csv`):**
- Parses `start_date` to a standard date format (`dd/mm/yy`).
- Derives `site` from the numeric suffix of `exhibition_id`.
- Removes rows with `NULL` values in required fields (`exhibition_name`, `site`, `floor`, `department`, `start_date`, `exhibition_id`).
- Removes rows where `exhibition_id` does not start with `EXH_`.

**Kiosk output (`kiosk_output.csv`):**
- Parses the `at` timestamp column; sets unparseable values to `NULL`.
- Casts `type` to a nullable integer.
- Removes rows with `NULL` values in required fields (`at`, `site`, `val`).
- Removes rows where `val` is not in `{-1, 0, 1, 2, 3, 4}`.
- Sets invalid `type` values to `NULL` (must be `0`, `1`, or `NULL`).
- Enforces the constraint: `val = -1` requires a non-`NULL` `type`; all other `val` values require `type = NULL`.

### Load (`load.py`)

1. Creates the target database (local) or provisions an RDS instance via Terraform (cloud).
2. Executes `schema.sql` to create the `exhibitions` and `kiosk_output` tables.
3. Truncates existing data and bulk-loads the transformed CSV files using `COPY`.
4. Updates `exhibit_id` on `kiosk_output` rows by matching each interaction to the most recently started exhibition at the same site.

## Cloud Infrastructure (`set_up.tf`)

Terraform provisions the following AWS resources in `eu-west-2`:

- A **security group** allowing inbound PostgreSQL traffic (port 5432) from any IP.
- A **DB subnet group** using the `c23-VPC` subnets.
- An **RDS `db.t3.micro` PostgreSQL instance** (`c23-jessica-museum-db`) with public accessibility.

The RDS endpoint, port, database name, username, and password are written to `rds_config.json` via `terraform output -json`.
