from os import environ
import logging
import argparse
from typing import Optional

from dotenv import load_dotenv
from confluent_kafka import Consumer
import json
from datetime import datetime
from psycopg2 import connect
from psycopg2.extensions import connection


def setup_logger(log_level: str = "INFO") -> None:
    """Configure logging with the specified log level.
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding="utf-8"
    )


def get_consumer() -> Consumer:
    """Creates and returns a Kafka consumer."""
    return Consumer({
        "bootstrap.servers": environ["BOOTSTRAP_SERVERS"],
        'security.protocol': environ["SECURITY_PROTOCOL"],
        'sasl.mechanisms': environ["SASL_MECHANISM"],
        'sasl.username': environ["USERNAME"],
        'sasl.password': environ["PASSWORD"],
        'group.id': 'jessica-consumer-group-local',
    })


def check_valid_time(at_value: datetime) -> bool:
    """Check if the 'at' value is within the valid time range."""
    if not isinstance(at_value, datetime):
        logging.error("'at' value is not a datetime object.")
        return False
    if at_value.hour < 8 or (at_value.hour == 8 and at_value.minute < 45):
        logging.error("Message is from before 8:45 AM.")
        return False
    if at_value.hour > 18 or (at_value.hour == 18 and at_value.minute >= 15):
        logging.error("Message is from after 6:15 PM.")
        return False
    return True


def check_at_key(msg: dict) -> bool:
    """Check if the 'at' key is present in the message."""
    if "at" in msg.keys():
        try:
            msg['at'] = datetime.fromisoformat(msg['at'])
            # Strip timezone info to match TIMESTAMP WITHOUT TIME ZONE column
            msg['at'] = msg['at'].replace(tzinfo=None)
            logging.info(
                "'at' key successfully converted to datetime: %s", msg['at'])
        except ValueError as e:
            logging.error("Error converting 'at' key to datetime: %s", e)
            return False
        return check_valid_time(msg['at'])
    logging.warning("Message does not contain 'at' key.")
    return False


def check_site_key(msg: dict) -> bool:
    """
    Check if the 'site' key is present in the message and is a valid number.
    Database validation will ensure the site exists in exhibitions."""
    if "site" in msg.keys():
        if isinstance(msg['site'], str) and msg['site'].isnumeric():
            msg['site'] = int(msg['site'])
        if isinstance(msg['site'], int):
            return True
        logging.error(
            "Message 'site' key is not a valid number.")
        return False
    logging.warning("Message does not contain 'site' key.")
    return False


def check_val_key(msg: dict) -> bool:
    """Check if the 'val' key is present in the message."""
    if "val" in msg.keys():
        if msg['val'] in {-1, 0, 1, 2, 3, 4}:
            return True
        logging.error(
            "Message 'val' key is invalid. Expected values: -1, 0, 1, 2, 3, 4.")
        return False
    logging.warning("Message does not contain 'val' key.")
    return False


def check_type_key(msg: dict) -> bool:
    """Check if the 'type' key is present in the message."""
    if "type" in msg.keys():
        if msg['type'] in {0, 1}:
            return True
        logging.error(
            "Message 'type' key is invalid. Expected values: 0, 1.")
        return False
    logging.warning("Message does not contain 'type' key.")
    return False


def check_message(msg: dict) -> bool:
    if not check_at_key(msg):
        logging.warning(
            "Message failed 'at' key validation. Skipping.")
        return False
    logging.info("Message passed 'at' key validation.")
    if not check_site_key(msg):
        logging.warning(
            "Message failed 'site' key validation. Skipping.")
        return False
    logging.info("Message passed 'site' key validation.")
    if not check_val_key(msg):
        logging.warning(
            "Message failed 'val' key validation. Skipping.")
        return False
    logging.info("Message passed 'val' key validation.")
    if msg['val'] == -1:
        if not check_type_key(msg):
            logging.warning(
                "Message failed 'type' key validation. Skipping.")
            return False
        logging.info("Message passed 'type' key validation.")
    else:
        msg['type'] = None
        logging.info(
            "Message 'type' key set to None for non -1 'val' value.")
    return True


def load_message(consumer: Consumer) -> dict:
    """Gets a single message using the Kafka consumer."""
    message = None
    while not message:
        message = consumer.poll(timeout=1.0)
        if message:
            if message.error():
                logging.error("Consumer error: %s", message.error())
            else:
                msg = message.value().decode('utf-8')
                logging.info("Received message: %s", msg)
                try:
                    msg = json.loads(msg)
                except json.JSONDecodeError:
                    logging.error("Received message is not valid JSON.")
                    message = None
                    continue
                logging.debug("Message keys: %s", msg.keys())
                msg = {
                    k.lower().strip(): v for k, v in msg.items()
                    if isinstance(k, str)
                }
                if check_message(msg):
                    logging.info("Message passed all validation checks.")
                    return msg
                logging.warning(
                    "Message failed validation checks. Skipped: %s", msg)
            message = None
    return {}


def find_exhibit_id_for_message(msg: dict, cursor) -> Optional[int]:
    """
    Find the exhibit_id for a message using ranked exhibitions logic.
    Returns the exhibit_id if found, None otherwise."""
    find_exhibit_id = """
    WITH ranked_exhibitions AS (
        SELECT 
            e.exhibit_id, 
            e.site,
            ROW_NUMBER() OVER (PARTITION BY e.site ORDER BY e.start_date DESC) as row_num
        FROM exhibitions e
        WHERE e.site = %s AND e.start_date < %s
    )
    SELECT exhibit_id FROM ranked_exhibitions
    WHERE row_num = 1;
    """
    cursor.execute(find_exhibit_id, (msg['site'], msg['at']))
    result = cursor.fetchone()

    if result is None:
        logging.warning(
            "No associated exhibit found for site=%s, at=%s. Message skipped.",
            msg['site'], msg['at'])
        return None

    exhibit_id = result[0]
    logging.info("Successfully found exhibit_id=%s for site=%s, at=%s",
                 exhibit_id, msg['site'], msg['at'])
    return exhibit_id


def check_for_duplicate(msg: dict, exhibit_id: int, cursor) -> bool:
    """
    Check if a duplicate interaction exists within 10 seconds.
    Returns True if duplicate found, False otherwise."""
    check_duplicate = """
    SELECT COUNT(*) FROM kiosk_output
    WHERE site = %s 
    AND exhibit_id = %s 
    AND val = %s 
    AND type = %s
    AND at > %s - INTERVAL '10 seconds'
    AND at <= %s;
    """
    cursor.execute(check_duplicate, (
        msg['site'], exhibit_id, msg['val'], msg['type'],
        msg['at'], msg['at']
    ))
    duplicate_count = cursor.fetchone()[0]

    if duplicate_count > 0:
        logging.warning(
            "Duplicate interaction detected. Skipping: site=%s, exhibit_id=%s, val=%s, at=%s",
            msg['site'], exhibit_id, msg['val'], msg['at'])
        return True

    logging.info("No duplicate found for site=%s, exhibit_id=%s, val=%s",
                 msg['site'], exhibit_id, msg['val'])
    return False


def get_cloud_connection() -> connection:
    """Connect to cloud RDS database using config file."""
    try:
        connection = connect(
            host=environ['HOST'],
            port=environ['PORT'],
            database=environ['DATABASE'],
            user=environ['DB_USERNAME'],
            password=environ['DB_PASSWORD']
        )
        return connection
    except Exception as e:
        logging.exception("Failed to connect to database: %s", e)
        raise


def load_messages(log_level: str = "INFO") -> None:
    """Continuously loads messages from the Kafka consumer and uploads them to the database."""
    setup_logger(log_level)
    logging.info("Starting continuous message loading process...")
    try:
        with get_consumer() as consumer:
            logging.info("Consumer connected successfully")
            consumer.subscribe(['lmnh'])
            logging.info("Subscribed to 'lmnh' topic. Waiting for messages...")

            successful_count = 0

            while True:
                try:
                    msg = load_message(consumer)
                    if not msg:
                        logging.warning("Received empty message. Skipping...")
                        continue

                    logging.debug("Attempting to insert message: %s", msg)
                    with get_cloud_connection() as conn:
                        with conn.cursor() as cursor:
                            # Find exhibit_id for this message
                            exhibit_id = find_exhibit_id_for_message(
                                msg, cursor)
                            if exhibit_id is None:
                                continue

                            # Check for duplicate interactions
                            if check_for_duplicate(msg, exhibit_id, cursor):
                                continue

                            # Insert message with exhibit_id
                            cursor.execute(
                                "INSERT INTO kiosk_output (exhibit_id, at, site, val, type) VALUES (%s, %s, %s, %s, %s)",
                                (exhibit_id, msg['at'], msg['site'],
                                 msg['val'], msg['type'])
                            )

                            conn.commit()
                            successful_count += 1
                            logging.info(
                                "Successfully inserted message into database: exhibit_id=%s, at=%s, site=%s, val=%s, type=%s (Total: %d)",
                                exhibit_id, msg['at'], msg['site'], msg['val'], msg['type'], successful_count)
                except KeyboardInterrupt:
                    logging.info(
                        "Consumer interrupted by user. Total successful entries: %d. Exiting...",
                        successful_count)
                    return
                except KeyError as e:
                    logging.error(
                        "Missing required key in message: %s. Message: %s", e, msg)
                except Exception as e:
                    logging.exception("Error during message loading: %s", e)
    except KeyboardInterrupt:
        logging.info("Consumer interrupted. Exiting...")
    except Exception as e:
        logging.exception("Failed to initialize consumer: %s", e)


def setup_parser() -> argparse.ArgumentParser:
    """Set up command-line argument parser."""
    parser = argparse.ArgumentParser(
        description="Kafka consumer for loading LMNH messages into database"
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)"
    )
    return parser


if __name__ == "__main__":
    parser = setup_parser()
    args = parser.parse_args()
    load_dotenv()
    load_messages(log_level=args.log_level)
