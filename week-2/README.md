# Week 2 — Real-Time Kafka Streaming Consumer

This week replaces the batch pipeline with a **long-running Kafka consumer** that processes live kiosk interaction messages as they arrive. Each message is validated, deduplicated, and inserted into the cloud AWS RDS PostgreSQL database populated in Week 1.

## Files

| File | Description |
|------|-------------|
| `pipeline.py` | Kafka consumer: reads messages from the `lmnh` topic, validates them, and inserts into the database |
| `test_pipeline.py` | Pytest test suite covering all validation functions |
| `set_up_ec2.tf` | Terraform configuration for the EC2 instance that runs the consumer |
| `reset_database.sh` | Shell script to clear the `kiosk_output` table while preserving exhibitions |
| `requirements.txt` | Python dependencies for this week |

## Prerequisites

- Python 3.10+
- `pip install -r requirements.txt`
- An AWS RDS PostgreSQL database already seeded (see [Week 1](../week-1/))
- Confluent Kafka cluster credentials
- Terraform (only required for EC2 deployment)

## Environment Variables

Create a `.env` file in this directory:

```dotenv
# Database (AWS RDS)
HOST=your_rds_endpoint
PORT=5432
DATABASE=museum
DB_USERNAME=your_db_username
DB_PASSWORD=your_db_password

# Kafka
BOOTSTRAP_SERVERS=your_kafka_bootstrap_server
SECURITY_PROTOCOL=SASL_SSL
SASL_MECHANISM=PLAIN
USERNAME=your_kafka_username
PASSWORD=your_kafka_password
```

## Usage

### Run the consumer locally

```bash
python pipeline.py
```

With verbose debug logging:

```bash
python pipeline.py --log-level DEBUG
```

The consumer connects to the `lmnh` Kafka topic and runs continuously until interrupted with `Ctrl+C`.

### Run tests

```bash
pytest test_pipeline.py
```

### Reset the database

To clear all kiosk interaction data while keeping exhibitions intact:

```bash
bash reset_database.sh
```

The script prompts for confirmation before truncating the `kiosk_output` table.

## Message Validation

Each Kafka message is expected to be a JSON object. A message is accepted only if it passes all of the following checks:

| Field | Validation |
|-------|-----------|
| `at` | Present, valid ISO 8601 datetime, and between **08:45** and **18:15** (museum opening hours) |
| `site` | Present and a valid integer (or numeric string) |
| `val` | Present and one of `{-1, 0, 1, 2, 3, 4}` |
| `type` | Required (and `0` or `1`) when `val = -1`; set to `NULL` for all other `val` values |

Messages that fail any check are logged and skipped.

## Deduplication

Before inserting, the consumer checks whether an identical interaction (same `site`, `exhibit_id`, `val`, and `type`) was already recorded within the previous **10 seconds**. Duplicate events are discarded and logged.

## Database Insertion

For each valid, non-duplicate message:

1. The most recent exhibition at the message's `site` (with a `start_date` before the interaction timestamp) is looked up to determine `exhibit_id`.
2. If no matching exhibition is found the message is skipped.
3. The interaction is inserted into `kiosk_output` with the resolved `exhibit_id`.

## Cloud Infrastructure (`set_up_ec2.tf`)

Terraform provisions the following AWS resources in `eu-west-2` to host the long-running consumer:

- A **security group** (`c23_jessica_museum_ec2_sg`) with:
  - Inbound: PostgreSQL (5432), SSH (22), Kafka (9092)
  - Outbound: PostgreSQL (5432), HTTPS (443), Kafka (9092)
- An **RSA 4096-bit key pair** for SSH access; the private key is saved locally as `c23-jessica-museum-key.pem`.
- A **`t3.micro` EC2 instance** (`c23_jessica_museum_ec2`) in the `c23-public-subnet-1` subnet with a public IP.
