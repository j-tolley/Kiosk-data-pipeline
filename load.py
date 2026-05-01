"""Load module - Database connection and data loading functions."""
from psycopg2 import connect
import psycopg2
import logging
import os
import json
import subprocess


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


def terraform_apply(terraform_dir: str = "./terraform_set_up") -> bool:
    """Run terraform apply to create RDS instance."""
    try:
        logging.info("Creating RDS instance via Terraform...")
        result = subprocess.run(
            ["terraform", "apply", "-auto-approve"],
            cwd=terraform_dir,
            capture_output=True,
            text=True,
            check=True
        )
        if result.returncode != 0:
            logging.error("Terraform apply failed: %s", result.stderr)
            return False
        logging.info("RDS instance created successfully")
        return True
    except FileNotFoundError:
        logging.error("Terraform not found. Please install Terraform.")
        return False
    except Exception as e:
        logging.error("Error running terraform apply: %s", e)
        return False


def terraform_destroy(terraform_dir: str = "./terraform_set_up", config_file: str = "rds_config.json") -> bool:
    """Run terraform destroy to tear down RDS instance and remove config file.
    Args:
        terraform_dir: Path to terraform configuration directory
        config_file: Path to RDS config file to remove
    """
    try:
        logging.info("Destroying RDS infrastructure via Terraform...")
        result = subprocess.run(
            ["terraform", "destroy", "-auto-approve"],
            cwd=terraform_dir,
            capture_output=True,
            text=True,
            check = True
        )
        if result.returncode != 0:
            logging.error("Terraform destroy failed: %s", result.stderr)
            return False
        logging.info("RDS infrastructure destroyed successfully")

        # Remove config file if it exists
        if os.path.exists(config_file):
            os.remove(config_file)
            logging.info("Removed RDS configuration file: %s", config_file)

        return True
    except FileNotFoundError:
        logging.error("Terraform not found. Please install Terraform.")
        return False
    except Exception as e:
        logging.error("Error running terraform destroy: %s", e)
        return False


def generate_rds_config(terraform_dir: str = "./terraform_set_up", output_file: str = "rds_config.json") -> bool:
    """Generate rds_config.json from terraform output."""
    try:
        logging.info("Generating RDS configuration from Terraform output...")
        result = subprocess.run(
            ["terraform", "output", "-json"],
            cwd=terraform_dir,
            capture_output=True,
            text=True,
            check=True
        )
        if result.returncode != 0:
            logging.error("Failed to get terraform output: %s", result.stderr)
            return False

        config = json.loads(result.stdout)
        with open(output_file, 'w', encoding="utf-8") as f:
            json.dump(config, f, indent=2)
        logging.info("RDS configuration saved to %s", output_file)
        return True
    except json.JSONDecodeError as e:
        logging.error("Failed to parse terraform output: %s", e)
        return False
    except Exception as e:
        logging.error("Error generating RDS config: %s", e)
        return False


def check_rds_exists(config_file: str = "rds_config.json") -> bool:
    """Check if rds_config.json exists."""
    exists = os.path.exists(config_file)
    if exists:
        logging.info("RDS configuration found at %s", config_file)
    else:
        logging.info("RDS configuration not found at %s", config_file)
    return exists


def ensure_rds_infrastructure(terraform_dir: str = "./terraform_set_up", config_file: str = "rds_config.json") -> bool:
    """Ensure RDS infrastructure exists. Creates if not present."""
    if not check_rds_exists(config_file):
        logging.info("RDS infrastructure not found. Setting up...")
        if not terraform_apply(terraform_dir):
            logging.error("Failed to create RDS infrastructure")
            return False
        if not generate_rds_config(terraform_dir, config_file):
            logging.error("Failed to generate RDS configuration")
            return False
    return True


def get_cloud_connection(config_file: str = "rds_config.json") -> psycopg2.extensions.connection:
    """Connect to cloud RDS database using config file."""
    try:
        with open(config_file, 'r', encoding="utf-8") as f:
            config_data = json.load(f)
            config = config_data['db_config']['value']

        connection = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['username'],
            password=config['password']
        )
        logging.info("Connected to cloud RDS database at %s", config['host'])
        return connection
    except FileNotFoundError:
        logging.error("RDS config file %s not found", config_file)
        return None
    except json.JSONDecodeError as e:
        logging.error("Invalid RDS config file: %s", e)
        return None
    except psycopg2.OperationalError as e:
        logging.error("Failed to connect to RDS: %s", e)
        return None
    except Exception as e:
        logging.error("Unexpected error connecting to cloud database: %s", e)
        return None


def get_local_connection(db_name: str = "museum") -> psycopg2.extensions.connection:
    """Connect to local PostgreSQL database."""
    try:
        connection = connect(dbname=db_name)
        logging.info("Connected to local database '%s'", db_name)
        return connection
    except psycopg2.OperationalError as e:
        logging.error(
            "Failed to connect to local database '%s': %s", db_name, e)
        return None
    except Exception as e:
        logging.error("Unexpected error connecting to local database: %s", e)
        return None


def get_db_connection(db_type: str = "local", db_name: str = "museum", config_file: str = "rds_config.json") -> psycopg2.extensions.connection:
    """Get database connection based on type (local or cloud).
    Args:
        db_type: "local" for local PostgreSQL or "cloud" for RDS
        db_name: Database name (for local connections)
        config_file: Path to RDS config file (for cloud connections)
    Returns:
        psycopg2 connection object or None if failed
    """
    if db_type == "cloud":
        return get_cloud_connection(config_file)
    return get_local_connection(db_name)


def make_local_museum_db(db_name: str = "museum") -> bool:
    """Creates the museum database if it doesn't already exist."""
    conn = None
    try:
        conn = connect(dbname="postgres")
        conn.autocommit = True
        logging.info("Connected to postgres database")

        with conn.cursor() as cursor:
            # Check if database exists
            cursor.execute(
                "SELECT EXISTS(SELECT * FROM pg_database WHERE datname = %s LIMIT 1);", (db_name,))
            db_exists = cursor.fetchone()[0]

            if db_exists:
                logging.info("Database '%s' already exists.", db_name)
            else:
                cursor.execute("CREATE DATABASE {};".format(db_name))
                logging.info("Database '%s' created successfully.", db_name)

        return True
    except psycopg2.OperationalError as e:
        logging.error("Failed to connect to database: %s", e)
        return False
    except psycopg2.Error as e:
        logging.error("Database operation failed: %s", e)
        return False
    except Exception as e:
        logging.error("Unexpected error in make_local_museum_db: %s", e)
        return False
    finally:
        if conn:
            conn.close()
            logging.info("Disconnected from 'postgres'.")


def run_schema(db_type: str = "local", db_name: str = "museum", schema_file: str = "schema.sql", config_file: str = "rds_config.json") -> bool:
    """Connects to the database and runs the schema.sql file to create tables if they don't exist.
    Args:
        db_type: "local" or "cloud"
        db_name: Database name (for local) or used for cloud connection
        schema_file: Path to schema SQL file
        config_file: Path to RDS config file (for cloud)
    """
    # For local databases, ensure database exists
    if db_type == "local":
        if not make_local_museum_db(db_name):
            logging.error(
                "Failed to create database '%s'. Aborting schema execution.", db_name)
            return False
    else:
        # For cloud, ensure infrastructure exists
        if not ensure_rds_infrastructure(config_file=config_file):
            logging.error(
                "Failed to ensure RDS infrastructure. Aborting schema execution.")
            return False

    conn = None
    try:
        if not os.path.exists(schema_file):
            logging.error("%s file not found", schema_file)
            return False

        # Get connection based on db_type
        conn = get_db_connection(db_type, db_name, config_file)
        if conn is None:
            logging.error("Failed to connect to database")
            return False

        logging.info("Connected to %s database", db_type)
        with conn.cursor() as cursor:
            with open(schema_file, "r", encoding="utf-8") as f:
                sql_content = f.read()
                if not sql_content.strip():
                    logging.warning("%s is empty", schema_file)
                    return False
                cursor.execute(sql_content)
        conn.commit()
        logging.info("Schema executed successfully.")
        return True
    except FileNotFoundError as e:
        logging.error("Schema file not found: %s", e)
        return False
    except psycopg2.OperationalError as e:
        logging.error("Failed to connect to %s database: %s", db_type, e)
        if conn:
            conn.rollback()
        return False
    except psycopg2.Error as e:
        logging.error("Database operation failed: %s", e)
        if conn:
            conn.rollback()
        return False
    except Exception as e:
        logging.error("Unexpected error in run_schema: %s", e)
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()
            logging.info("Disconnected from database.")


def seed_database(db_type: str = "local", db_name: str = "museum", exhibitions_file: str = "exhibitions.csv", 
                kiosk_file: str = "kiosk_output.csv", config_file: str = "rds_config.json") -> bool:
    """Connects to the database and loads formatted data from CSV files.
    Args:
        db_type: "local" or "cloud"
        db_name: Database name (for local) or used for cloud connection
        exhibitions_file: Path to exhibitions CSV file
        kiosk_file: Path to kiosk output CSV file
        config_file: Path to RDS config file (for cloud)
    """
    conn = None
    try:
        if not os.path.exists(exhibitions_file):
            logging.error("%s not found", exhibitions_file)
            return False
        if not os.path.exists(kiosk_file):
            logging.error("%s not found", kiosk_file)
            return False

        # Get connection based on db_type
        conn = get_db_connection(db_type, db_name, config_file)
        if conn is None:
            logging.error("Failed to connect to database for seeding")
            return False

        logging.info("Connected to %s database for seeding", db_type)

        with conn.cursor() as cursor:
            # Truncate existing data
            cursor.execute("TRUNCATE TABLE exhibitions, kiosk_output CASCADE;")
            logging.info("Truncated existing data")

            # Load exhibitions data using copy_expert with CSV format
            with open(exhibitions_file, 'r', encoding='utf-8') as f:
                cursor.copy_expert(
                    """COPY exhibitions (exhibition_name, exhibition_id, floor, department, start_date, description, site) 
                    FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',')""",
                    f
                )
            logging.info("Loaded exhibitions data from %s", exhibitions_file)

            # Load kiosk data using copy_expert with CSV format
            with open(kiosk_file, 'r', encoding='utf-8') as f:
                cursor.copy_expert(
                    """COPY kiosk_output (at, site, val, type) 
                    FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',')""",
                    f
                )
            logging.info("Loaded kiosk data from %s", kiosk_file)

            # Update exhibit_id based on ranked exhibitions
            update_exhibit_id = """
            WITH ranked_exhibitions AS (
                SELECT 
                    e.exhibit_id, 
                    e.site, 
                    e.start_date,
                    ROW_NUMBER() OVER (PARTITION BY e.site ORDER BY e.start_date DESC) as row_num
                FROM exhibitions e
                INNER JOIN kiosk_output k ON e.site = k.site AND e.start_date < k.at
            )
            UPDATE kiosk_output
            SET exhibit_id = ranked_exhibitions.exhibit_id
            FROM ranked_exhibitions
            WHERE kiosk_output.site = ranked_exhibitions.site
            AND ranked_exhibitions.row_num = 1;
            """
            cursor.execute(update_exhibit_id)
            logging.info("Updated exhibit_id references")

        conn.commit()
        logging.info("Data seeding executed successfully.")
        return True
    except FileNotFoundError as e:
        logging.error("File not found: %s", e)
        return False
    except psycopg2.OperationalError as e:
        logging.error("Failed to connect to %s database: %s", db_type, e)
        if conn:
            conn.rollback()
        return False
    except psycopg2.Error as e:
        logging.error("Database operation failed: %s", e)
        if conn:
            conn.rollback()
        return False
    except Exception as e:
        logging.error("Unexpected error in seed_database: %s", e)
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()
            logging.info("Disconnected from database.")


def load_data(db_type: str = "local", db_name: str = "museum", exhibitions_file: str = "exhibitions.csv", kiosk_file: str = "kiosk_output.csv", schema_file: str = "schema.sql", config_file: str = "rds_config.json") -> bool:
    """Orchestrates schema creation and data loading.
    Args:
        db_type: "local" or "cloud"
        db_name: Database name (for local)
        exhibitions_file: Path to exhibitions CSV
        kiosk_file: Path to kiosk CSV
        schema_file: Path to schema SQL
        config_file: Path to RDS config (for cloud)
    Returns:
        True if successful, False otherwise
    """
    try:
        logging.info("Starting data loading phase...")

        logging.info("Creating database schema...")
        if not run_schema(db_type, db_name, schema_file, config_file):
            logging.error("Failed to run schema")
            return False

        logging.info("Seeding database with data...")
        if not seed_database(db_type, db_name, exhibitions_file, kiosk_file, config_file):
            logging.error("Failed to seed database")
            return False

        logging.info("Data loading completed successfully!")
        return True
    except Exception as e:
        logging.error("Unexpected error during data loading: %s", e)
        return False


if __name__ == "__main__":
    setup_logger()
    success = load_data("cloud")
    if not success:
        logging.error("Data loading failed!")
