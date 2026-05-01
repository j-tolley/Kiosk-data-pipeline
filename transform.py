"""Transform module - Data transformation and validation functions."""
import pandas as pd
import logging
import os


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


def check_null_values(df: pd.DataFrame, null_fields: list) -> pd.DataFrame:
    """Check and remove rows with NULL values in required fields."""
    for field in null_fields:
        if field in df.columns:
            null_count = df[field].isnull().sum()
            if null_count > 0:
                logging.warning(
                    "Data quality: Removing %d rows with NULL '%s' (NOT NULL required)", null_count, field)
                df = df[df[field].notna()]
    return df


def check_exhibition_id_format(df: pd.DataFrame) -> pd.DataFrame:
    """Check exhibition_id format (must start with EXH_)."""
    if "exhibition_id" not in df.columns:
        return df

    valid_ids = df["exhibition_id"].astype(str).str.startswith("EXH_")
    invalid_count = (valid_ids is False).sum()
    if invalid_count > 0:
        invalid_ids = df[valid_ids is False]["exhibition_id"].unique()
        logging.warning(
            "Data quality: Removing %d rows with invalid exhibition_id format (must start with EXH_). Invalid IDs: %s",
            invalid_count, list(invalid_ids))
        df = df[valid_ids]
    return df


def validate_exhibitions_quality(df: pd.DataFrame) -> pd.DataFrame:
    """Validates and cleans exhibitions data. Logs issues but continues processing."""
    try:
        initial_rows = len(df)
        # Run individual validation checks
        null_fields = ["exhibition_name", "site", "floor",
                       "department", "start_date", "exhibition_id"]
        df = check_null_values(df, null_fields)
        df = check_exhibition_id_format(df)
        final_rows = len(df)
        removed_rows = initial_rows - final_rows
        logging.info(
            "Exhibitions validation: %d rows loaded, %d removed, %d rows clean", initial_rows, removed_rows, final_rows)
        return df
    except Exception as e:
        logging.error("Error during exhibitions validation: %s", e)
        return df


def format_exhibitions_csv(exhibitions_file: str = "exhibitions.csv") -> bool:
    """Formats the exhibitions.csv file to have the correct data types and formats."""
    try:
        if not os.path.exists(exhibitions_file):
            logging.error("%s not found", exhibitions_file)
            return False
        df = pd.read_csv(exhibitions_file)
        logging.info("Loaded %s successfully", exhibitions_file)
        df.columns = df.columns.str.lower()

        try:
            df["start_date"] = pd.to_datetime(
                df["start_date"], format='%d/%m/%y')
        except ValueError as e:
            logging.error("Failed to parse start_date column: %s", e)
            return False
        df = df.sort_values("start_date")

        df["site"] = pd.to_numeric(
            df["exhibition_id"].str.split('_').str[-1], errors='coerce')

        # Validate and clean data
        df = validate_exhibitions_quality(df)
        if len(df) == 0:
            logging.error("No valid exhibitions data remains after validation")
            return False
        df.to_csv(exhibitions_file, index=False)
        logging.info("Successfully formatted and saved %s", exhibitions_file)
        return True
    except pd.errors.ParserError as e:
        logging.error("Failed to parse %s: %s", exhibitions_file, e)
        return False
    except Exception as e:
        logging.error("Unexpected error in format_exhibitions_csv: %s", e)
        return False


def check_kiosk_null_values(df: pd.DataFrame, null_fields: list) -> pd.DataFrame:
    """Check and remove rows with NULL values in required fields."""
    for field in null_fields:
        if field in df.columns:
            null_count = df[field].isnull().sum()
            if null_count > 0:
                logging.warning(
                    "Data quality: Removing %d rows with NULL '%s' (NOT NULL required)", null_count, field)
                df = df[df[field].notna()]
    return df


def check_val_range(df: pd.DataFrame) -> pd.DataFrame:
    """Check that val is in allowed values (-1, 0, 1, 2, 3, 4)."""
    if "val" not in df.columns:
        return df

    valid_mask = df["val"].isin([-1, 0, 1, 2, 3, 4])
    invalid_count = (valid_mask is False).sum()
    if invalid_count > 0:
        logging.warning(
            "Data quality: Removing %d rows with invalid 'val' values (must be -1, 0, 1, 2, 3, 4)", invalid_count)
        df = df[valid_mask]
    return df


def check_type_values(df: pd.DataFrame) -> pd.DataFrame:
    """Check that type is in allowed values (0, 1) or NULL."""
    if "type" not in df.columns:
        return df

    valid_types = df["type"].isin([0, 1]) | df["type"].isna()
    invalid_count = (valid_types is False).sum()
    if invalid_count > 0:
        logging.warning(
            "Data quality: Setting %d invalid 'type' values to NULL (must be 0, 1, or NULL)", invalid_count)
        df.loc[valid_types is False, "type"] = None
    return df


def check_val_type_constraint(df: pd.DataFrame) -> pd.DataFrame:
    """Check business logic: if val=-1, type must not be NULL; if val!=-1, type must be NULL."""
    if "val" not in df.columns or "type" not in df.columns:
        return df

    # Remove rows where val=-1 but type is NULL
    keep_rows = (df["val"] != -1) | df["type"].notna()
    remove_count = (keep_rows is False).sum()
    if remove_count > 0:
        logging.warning(
            "Data quality: Removing %d rows where val=-1 but type is NULL (type required)", remove_count)
        df = df[keep_rows]

    # Fix rows where val!=-1 but type is not NULL (set type to NULL)
    fix_others = (df["val"] != -1) & df["type"].notna()
    fix_count = fix_others.sum()
    if fix_count > 0:
        logging.warning(
            "Data quality: Setting type to NULL for %d rows where val!=-1 but type was not NULL", fix_count)
        df.loc[fix_others, "type"] = None
    return df


def validate_kiosk_quality(df: pd.DataFrame) -> pd.DataFrame:
    """Validates and cleans kiosk_output data. Logs issues but continues processing."""
    try:
        initial_rows = len(df)
        # Run individual validation checks
        null_fields = ["at", "site", "val"]
        df = check_kiosk_null_values(df, null_fields)
        df = check_val_range(df)
        df = check_type_values(df)
        df = check_val_type_constraint(df)
        final_rows = len(df)
        removed_rows = initial_rows - final_rows
        logging.info(
            "Kiosk validation: %d rows loaded, %d removed, %d rows clean", initial_rows, removed_rows, final_rows)
        return df
    except Exception as e:
        logging.error("Error during kiosk validation: %s", e)
        return df


def format_kiosk_csv(kiosk_file: str = "kiosk_output.csv") -> bool:
    """Formats the kiosk_output.csv file to have the correct data types and formats."""
    try:
        if not os.path.exists(kiosk_file):
            logging.error("%s not found", kiosk_file)
            return False
        df = pd.read_csv(kiosk_file)
        logging.info("Loaded %s successfully", kiosk_file)

        # Parse 'at' column, setting unparseable values to NULL
        invalid_dates = df["at"].apply(lambda x: pd.to_datetime(
            x, errors='coerce')).isna() & df["at"].notna()
        if invalid_dates.sum() > 0:
            logging.warning(
                "Data quality: Setting %d unparseable 'at' values to NULL", invalid_dates.sum())
        df["at"] = pd.to_datetime(df["at"], errors='coerce')
        df = df.sort_values("at")

        df["type"] = pd.to_numeric(df["type"], errors='coerce').astype('Int64')

        # Validate and clean data
        df = validate_kiosk_quality(df)
        if len(df) == 0:
            logging.error("No valid kiosk data remains after validation")
            return False
        df.to_csv(kiosk_file, index=False)
        logging.info("Successfully formatted and saved %s", kiosk_file)
        return True
    except pd.errors.ParserError as e:
        logging.error("Failed to parse %s: %s", kiosk_file, e)
        return False
    except Exception as e:
        logging.error("Unexpected error in format_kiosk_csv: %s", e)
        return False


def transform_data(exhibitions_file: str = "exhibitions.csv", kiosk_file: str = "kiosk_output.csv") -> bool:
    """Orchestrates all transformation and validation steps.
    Args:
        exhibitions_file: Path to exhibitions CSV file
        kiosk_file: Path to kiosk output CSV file
    Returns:
        True if successful, False otherwise
    """
    try:
        logging.info("Starting data transformation...")

        logging.info("Transforming exhibitions data...")
        if not format_exhibitions_csv(exhibitions_file):
            logging.error("Failed to transform exhibitions data")
            return False

        logging.info("Transforming kiosk data...")
        if not format_kiosk_csv(kiosk_file):
            logging.error("Failed to transform kiosk data")
            return False

        logging.info("Data transformation completed successfully!")
        return True
    except Exception as e:
        logging.error("Unexpected error during data transformation: %s", e)
        return False


if __name__ == "__main__":
    setup_logger()
    transform_data()
