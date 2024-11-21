import boto3
import json
import logging
from botocore.exceptions import ClientError
from pg8000.native import Connection


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def retrieve_db_credentials(secret_name, region_name):
    try:
        secrets_manager_client = boto3.client(
            "secretsmanager", region_name=region_name
        )
        secret_value = secrets_manager_client.get_secret_value(
            SecretId=secret_name
        )
        secret = json.loads(secret_value["SecretString"])
        return secret
    except ClientError as e:
        logger.error(
            f"Error accessing Secrets Manager: {e}",
            exc_info=True,
        )
        raise
    except Exception as err:
        logger.error(f"Error retrieving DB credentials: {err}", exc_info=True)
        raise


def connect_to_db(secret_name, region_name):
    try:
        creds = retrieve_db_credentials(secret_name, region_name)
        conn = Connection(
            user=creds["USER"],
            password=creds["PASSWORD"],
            database=creds["DATABASE"],
            host=creds["HOST"],
            port=int(creds["PORT"]),
        )
        logger.info("Successfully connected to the database.")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to the database: {e}", exc_info=True)
        raise


def load_data_into_warehouse(conn, tables_data_frames):
    DIM_PRIMARY_KEYS = {
        "dim_date": "date_id",
        "dim_staff": "staff_id",
        "dim_location": "location_id",
        "dim_currency": "currency_id",
        "dim_design": "design_id",
        "dim_counterparty": "counterparty_id",
    }
    results = {
        "successfully_loaded": [],
        "failed_to_load": [],
        "skipped_empty": [],
    }

    for table_name, df in tables_data_frames.items():
        try:
            if df.empty:
                logger.info(f"Skipping {table_name}: DataFrame is empty.")
                results["skipped_empty"].append(table_name)
                continue

            columns = (", ").join([f'"{col}"' for col in df.columns])
            placeholders = (", ").join(["%s"] * len(df.columns))
            records = df.to_records(index=False).tolist()

            if table_name.startswith("dim_"):
                primary_key = DIM_PRIMARY_KEYS[table_name]
                set_clause = (", ").join(
                    [f'"{col}" = EXCLUDED."{col}"' for col in df.columns]
                )
                query = f"""
                    INSERT INTO "{table_name}" ({columns})
                    VALUES ({placeholders})
                    ON CONFLICT ("{primary_key}") DO UPDATE
                    SET {set_clause};
                """
            else:
                query = f"""
                    INSERT INTO "{table_name}" ({columns})
                    VALUES ({placeholders});
                """

            conn.run(sql=query, params=records)
            logger.info(f"Successfully loaded data into '{table_name}'.")
            results["successfully_loaded"].append(table_name)

        except Exception as e:
            logger.error(
                f"Error loading data into '{table_name}': {e}", exc_info=True
            )
            results["failed_to_load"].append(table_name)
            continue

    return results
