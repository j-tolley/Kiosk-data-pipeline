"""Extract module - Data extraction from S3 and CSV/JSON file processing."""
from dotenv import dotenv_values
from boto3 import client
import pandas as pd
import os
import glob
import json
import logging
import botocore


def download_data() -> bool:
    """Downloads all the data from S3 bucket and saves it to ./data.
    Returns:
        True if successful, False otherwise
    """
    try:
        logging.info("Loading AWS credentials from environment...")
        config = dotenv_values()

        if not config.get('ACCESS_KEY_ID') or not config.get('SECRET_ACCESS_KEY'):
            logging.error("AWS credentials not found in environment")
            return False

        logging.info("Connecting to S3 bucket...")
        s3 = client("s3",
                    aws_access_key_id=config['ACCESS_KEY_ID'],
                    aws_secret_access_key=config['SECRET_ACCESS_KEY'])

        logging.info("Listing objects in sigma-resources-museum bucket...")
        bucket = s3.list_objects(Bucket="sigma-resources-museum")

        if 'Contents' not in bucket:
            logging.warning("No contents found in S3 bucket")
            return False

        os.makedirs("./data", exist_ok=True)
        logging.info("Created ./data directory")

        download_count = 0
        for o in bucket["Contents"]:
            try:
                s3.download_file("sigma-resources-museum",
                                 o["Key"], f"./data/{o['Key']}")
                download_count += 1
            except Exception as e:
                logging.warning("Failed to download %s: %s", o["Key"], e)

        logging.info("Downloaded %d files from S3", download_count)
        return True
    except botocore.exceptions.NoCredentialsError:
        logging.error("AWS credentials not configured")
        return False
    except botocore.exceptions.ClientError as e:
        logging.error("S3 client error: %s", e)
        return False
    except Exception as e:
        logging.error("Unexpected error in download_data: %s", e)
        return False


def make_kiosk_csv() -> bool:
    """Combines all the csv files in ./data of the format lmnh_hist_data_*.csv into kiosk_output.csv.
    Returns:
        True if successful, False otherwise
    """
    try:
        csv_files = glob.glob("./data/lmnh_hist_data_*.csv")

        if not csv_files:
            logging.error("No kiosk CSV files found in ./data")
            return False

        logging.info("Found %d kiosk CSV files", len(csv_files))

        dfs = []
        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file)
                dfs.append(df)
                logging.info("Loaded %s", csv_file)
            except pd.errors.ParserError as e:
                logging.error("Failed to parse %s: %s", csv_file, e)
                return False
            except Exception as e:
                logging.error("Error reading %s: %s", csv_file, e)
                return False

        if not dfs:
            logging.error("No valid kiosk data to combine")
            return False

        df = pd.concat(dfs, ignore_index=True)
        logging.info("Combined %d rows from kiosk data", len(df))

        df.to_csv("kiosk_output.csv", index=False)
        logging.info(
            "Successfully created kiosk_output.csv with %d rows", len(df))
        return True
    except Exception as e:
        logging.error("Unexpected error in make_kiosk_csv: %s", e)
        return False


def make_exhibitions_csv() -> bool:
    """Combines all the json files in ./data of the format lmnh_exhibition_*.json into exhibitions.csv.
    Returns:
        True if successful, False otherwise
    """
    try:
        json_files = glob.glob("./data/lmnh_exhibition_*.json")

        if not json_files:
            logging.error("No exhibition JSON files found in ./data")
            return False

        logging.info("Found %d exhibition JSON files", len(json_files))

        rows = []
        for file_path in json_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    exhibition_data = json.load(f)
                row_df = pd.DataFrame([exhibition_data])
                rows.append(row_df)
                logging.info("Loaded %s", file_path)
            except json.JSONDecodeError as e:
                logging.error("Failed to parse JSON in %s: %s", file_path, e)
                return False
            except Exception as e:
                logging.error("Error reading %s: %s", file_path, e)
                return False

        if not rows:
            logging.error("No valid exhibition data to combine")
            return False

        combined_df = pd.concat(rows, ignore_index=True)
        logging.info("Combined %d rows from exhibition data", len(combined_df))

        combined_df.to_csv("exhibitions.csv", index=False)
        logging.info(
            "Successfully created exhibitions.csv with %d rows", len(combined_df))
        return True
    except Exception as e:
        logging.error("Unexpected error in make_exhibitions_csv: %s", e)
        return False


def extract_data_to_csv() -> bool:
    """Orchestrates all extraction steps. Logs and validates each step.
    Returns:
        True if all steps successful, False otherwise
    """
    try:
        logging.info("Starting data extraction phase...")

        logging.info("Step 1: Downloading data from S3...")
        if not download_data():
            logging.error("Failed to download data from S3")
            return False

        logging.info("Step 2: Creating kiosk CSV...")
        if not make_kiosk_csv():
            logging.error("Failed to create kiosk_output.csv")
            return False

        logging.info("Step 3: Creating exhibitions CSV...")
        if not make_exhibitions_csv():
            logging.error("Failed to create exhibitions.csv")
            return False

        logging.info("Data extraction completed successfully!")
        return True
    except Exception as e:
        logging.error("Unexpected error during data extraction: %s", e)
        return False


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding="utf-8"
    )
    success = extract_data_to_csv()
    if not success:
        logging.error("Data extraction failed!")
        exit(1)
    logging.info("All extraction steps completed successfully!")
