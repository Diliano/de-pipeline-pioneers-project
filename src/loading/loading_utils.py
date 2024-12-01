import json
import logging
from botocore.exceptions import ClientError
import pandas as pd
from io import BytesIO
import re
import boto3
from pg8000.native import Connection


logger = logging.getLogger()
logger.setLevel(logging.INFO)

DIM_PRIMARY_KEYS = {
    "dim_date": "date_id",
    "dim_staff": "staff_id",
    "dim_location": "location_id",
    "dim_currency": "currency_id",
    "dim_design": "design_id",
    "dim_counterparty": "counterparty_id",
}


def read_file_list(s3_client, bucket_name, key):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        content = response["Body"].read()
        json_content = json.loads(content)
        file_paths = json_content.get("files", [])
        logger.info(f"Read file list: {file_paths}")
        return file_paths
    except ClientError as e:
        logger.error(f"Error reading file list from S3: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Error reading file list: {e}", exc_info=True)
        raise


def process_parquet_files(s3_client, file_paths):
    tables_data_frames = {}
    for file_path in file_paths:
        try:
            match = re.match(r"s3://([^/]+)/(.+)", file_path)
            if not match:
                logger.error(f"Invalid S3 URI: {file_path}")
                continue
            bucket_name, key = match.groups()

            table_name = key.split("/")[1]

            obj = s3_client.get_object(Bucket=bucket_name, Key=key)
            parquet_content = obj["Body"].read()
            buffer = BytesIO(parquet_content)

            df = pd.read_parquet(buffer)
            tables_data_frames[table_name] = df

            logger.info(f"Processed Parquet file for table: {table_name}")
        except ClientError as e:
            logger.error(
                f"Error accessing Parquet file from S3: {file_path}: {e}",
                exc_info=True,
            )
            continue
        except Exception as e:
            logger.error(
                f"Error processing file: {file_path}: {e}", exc_info=True
            )
            continue
    return tables_data_frames


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
                set_clause = ", ".join(
                    [
                        f'"{col}" = EXCLUDED."{col}"'
                        for col in df.columns
                        if col != primary_key
                    ]
                )
                query = f"""
                    INSERT INTO "{table_name}" ({columns})
                    VALUES ({placeholders})
                    ON CONFLICT ("{primary_key}") DO UPDATE
                    SET {set_clause};
                """
                conn.run(sql=query, params=records)
                logger.info(f"Successfully loaded data into '{table_name}'.")
            else:
                fact_query = f"""
                    SELECT {columns}
                    FROM "{table_name}";
                """
                existing_rows = conn.run(sql=fact_query)

                existing_records = set(existing_rows)

                new_records = [
                    record
                    for record in records
                    if tuple(record) not in existing_records
                ]

                if new_records:
                    query = f"""
                        INSERT INTO "{table_name}" ({columns})
                        VALUES ({placeholders});
                    """
                    conn.run(sql=query, params=new_records)
                    logger.info(
                        f"Successfully loaded {len(new_records)} "
                        f"row(s) into '{table_name}'."
                    )
                else:
                    logger.info(f"No new rows to insert into '{table_name}'.")

            results["successfully_loaded"].append(table_name)

        except Exception as e:
            logger.error(
                f"Error loading data into '{table_name}': {e}", exc_info=True
            )
            results["failed_to_load"].append(table_name)
            continue

    return results
