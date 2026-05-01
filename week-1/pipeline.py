"""Main pipeline orchestration module."""
import logging
import argparse
from extract import extract_data_to_csv
from transform import transform_data, setup_logger
from load import load_data, terraform_destroy


def pipeline(exhibitions_file: str = "exhibitions.csv",
             kiosk_file: str = "kiosk_output.csv",
             schema_file: str = "schema.sql",
             db_type: str = "local",
             db_name: str = "museum",
             config_file: str = "rds_config.json",
             steps: list = None) -> bool:
    """Executes the complete data pipeline with error handling.
    Args:
        exhibitions_file: Path to exhibitions CSV file
        kiosk_file: Path to kiosk output CSV file
        schema_file: Path to database schema SQL file
        db_type: "local" for local PostgreSQL or "cloud" for RDS
        db_name: Database name to create and load into
        config_file: Path to RDS config file (for cloud)
        steps: List of steps to run ['extract', 'transform', 'load']. If None, runs all.
    Returns:
        True if successful, False otherwise
    """
    if steps is None:
        steps = ['extract', 'transform', 'load']

    try:
        logging.info(
            "Starting pipeline execution with %s database...", db_type)

        if 'extract' in steps:
            logging.info("Phase 1: Extract")
            if not extract_data_to_csv():
                logging.error("Data extraction failed. Aborting pipeline.")
                return False

        if 'transform' in steps:
            logging.info("Phase 2: Transform")
            if not transform_data(exhibitions_file, kiosk_file):
                logging.error("Data transformation failed. Aborting pipeline.")
                return False

        if 'load' in steps:
            logging.info("Phase 3: Load")
            if not load_data(db_type, db_name, exhibitions_file, kiosk_file, schema_file, config_file):
                logging.error("Data loading failed. Aborting pipeline.")
                return False

        logging.info("Pipeline completed successfully!")
        return True
    except Exception as e:
        logging.error("Pipeline failed with unexpected error: %s", e)
        return False


def setup_parser() -> argparse.ArgumentParser:
    """Set up and return the command-line argument parser."""
    parser = argparse.ArgumentParser(
        description='Data pipeline: Extract, Transform, and Load data into PostgreSQL database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
    Examples:
    # Run full pipeline with local database
    python pipeline.py
    
    # Run full pipeline with cloud RDS database
    python pipeline.py --db-type cloud
    
    # Run only transform and load steps
    python pipeline.py --steps transform load
    
    # Run only extract
    python pipeline.py --steps extract
    
    # Use custom file paths
    python pipeline.py --exhibitions-file /path/to/exh.csv --kiosk-file /path/to/kiosk.csv
    
    # Use custom database name (local only)
    python pipeline.py --db-name my_museum
    
    # Use custom RDS config file location
    python pipeline.py --db-type cloud --config-file /path/to/rds_config.json
    
    # Set logging level to DEBUG
    python pipeline.py --log-level DEBUG
    
    # Destroy RDS infrastructure
    python pipeline.py --db-type cloud --terraform-destroy
            '''
    )

    parser.add_argument(
        '--steps',
        nargs='+',
        choices=['extract', 'transform', 'load'],
        default=['extract', 'transform', 'load'],
        help='Pipeline steps to run (default: all steps)'
    )

    parser.add_argument(
        '--exhibitions-file',
        default='exhibitions.csv',
        help='Path to exhibitions CSV file (default: exhibitions.csv)'
    )

    parser.add_argument(
        '--kiosk-file',
        default='kiosk_output.csv',
        help='Path to kiosk output CSV file (default: kiosk_output.csv)'
    )

    parser.add_argument(
        '--schema-file',
        default='schema.sql',
        help='Path to database schema SQL file (default: schema.sql)'
    )

    parser.add_argument(
        '--db-type',
        choices=['local', 'cloud'],
        default='local',
        help='Database type: local PostgreSQL or cloud RDS (default: local)'
    )

    parser.add_argument(
        '--db-name',
        default='museum',
        help='Database name to create and load into (default: museum, local only)'
    )

    parser.add_argument(
        '--config-file',
        default='rds_config.json',
        help='Path to RDS config file (default: rds_config.json, cloud only)'
    )

    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Set logging level (default: INFO)'
    )

    parser.add_argument(
        '--terraform-destroy',
        action='store_true',
        help='Destroy RDS infrastructure created by Terraform (requires --db-type cloud)'
    )

    return parser


def run_pipeline(args) -> bool:
    """Main entry point for pipeline or terraform operations.
    Args:
        args: Parsed command line arguments
    Returns:
        True if successful, False otherwise
    """
    # Handle terraform destroy
    if args.terraform_destroy:
        if args.db_type != "cloud":
            logging.error(
                "Terraform destroy is only available for cloud databases (--db-type cloud)")
            return False
        logging.info("Terraform destroy requested for cloud database")
        return terraform_destroy(terraform_dir="./terraform_set_up", config_file=args.config_file)

    # Run pipeline with provided arguments
    return pipeline(
        exhibitions_file=args.exhibitions_file,
        kiosk_file=args.kiosk_file,
        schema_file=args.schema_file,
        db_type=args.db_type,
        db_name=args.db_name,
        config_file=args.config_file,
        steps=args.steps
    )


if __name__ == "__main__":
    parser = setup_parser()
    args = parser.parse_args()

    # Set up logging with the specified log level
    setup_logger(args.log_level)

    # Run pipeline and exit with appropriate code
    success = run_pipeline(args)
    (exit(0) if success else exit(1))
