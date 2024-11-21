from src.ingestion import (
    logger,
)
from botocore.exceptions import ClientError
from pg8000.exceptions import DatabaseError
from pg8000.native import Connection
from datetime import datetime
import pandas as pd
import json

from src.ingestion import (
    s3_client,
    secrets_manager_client,
    SECRET_NAME,
    TABLES,
    TIMESTAMP_FILE_KEY,
    S3_INGESTION_BUCKET,
)


def retrieve_db_credentials(secrets_manager_client):
    """
    Retrieve database credentials from AWS Secrets Manager.
    Args:
        secrets_manager_client (boto3.client): A boto3 Secrets Manager client.
    Returns:
        dict: A dictionary containing database credentials with keys:
            "USER", "PASSWORD", "DATABASE", "HOST", and "PORT".
    Logs:
        Exception: If there is an issue retrieving the secret from Secrets
        Manager.
    """
    try:
        secret = secrets_manager_client.get_secret_value(SecretId=SECRET_NAME)
        secret = json.loads(secret["SecretString"])
        return secret
    except Exception as err:
        logger.error(f"Unexpected error occurred {err}", exc_info=True)
        # raise err


def connect_to_db():
    """
    Establish a connection to the database using credentials from AWS Secrets
    Manager.
    Returns:
        pg8000.native.Connection: An active database connection object.
    Logs:
        Exception: If the database connection fails.
    """
    try:
        creds = retrieve_db_credentials(secrets_manager_client)
        USER = creds["USER"]
        PASSWORD = creds["PASSWORD"]
        DATABASE = creds["DATABASE"]
        HOST = creds["HOST"]
        PORT = creds["PORT"]

        return Connection(
            user=USER,
            database=DATABASE,
            password=PASSWORD,
            host=HOST,
            port=PORT,
        )

    except Exception as e:
        logger.error(f"Database connection failed: {e}", exc_info=True)
        # raise e


def get_last_ingestion_timestamp():
    """
    Retrieve the last ingestion timestamp from an S3 bucket.
    This function reads a JSON object stored in an S3 bucket at a specified
    key.
    It extracts the `timestamp` field and converts it into a `datetime`
    object.
    Returns:
        datetime.datetime: The timestamp of the last ingestion if available.
        str: The default timestamp string "1970-01-01 00:00:00" if no
            timestamp is found or there is a botocore ClientError.
    Logs:
        Exception: For any unexpected errors during the process.
    """
    try:
        response = s3_client.get_object(
            Bucket=S3_INGESTION_BUCKET, Key=TIMESTAMP_FILE_KEY
        )
        # Reading content only if 'Body' exists and is not None
        body = response.get("Body", "")
        if body:
            last_ingestion_data = json.loads(body.read().decode("utf-8"))

            # Ensuring the 'timestamp' key exists in the json data
            timestamp_str = last_ingestion_data.get("timestamp", "")
            if timestamp_str:
                return datetime.fromisoformat(timestamp_str)

        return "1970-01-01 00:00:00"
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchBucket":
            return "1970-01-01 00:00:00"
        elif e.response["Error"]["Code"] == "NoSuchKey":
            return "1970-01-01 00:00:00"
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
        # raise


def update_last_ingestion_timestamp():
    """
    Update the last ingestion timestamp in an S3 bucket.
    This function generates the current timestamp in ISO 8601 format
    and uploads it as a JSON object to the specified S3 bucket and key.
    The S3 object will have the structure:
        {
            "timestamp": "<current ISO 8601 timestamp>"
        }
    """
    current_timestamp = datetime.now().isoformat()
    print(S3_INGESTION_BUCKET)
    s3_client.put_object(
        Bucket=S3_INGESTION_BUCKET,
        Key=TIMESTAMP_FILE_KEY,
        Body=json.dumps({"timestamp": current_timestamp}),
    )


def fetch_tables(tables: list = TABLES):
    """
    Fetch data from specified tables in the database updated since last
    ingestion.
    Args:
        tables (list): List of table names to fetch data from. Defaults to the
            `TABLES` constant.
    Returns:
        dict: A dictionary where keys are table names and values are lists of
            row data as dictionaries.
    Logs:
        Info: Every time a table is fetched succesfully.
        Exception: If database connection or query execution fails.
    """
    tables_data = {}
    try:
        last_ingestion_timestamp = get_last_ingestion_timestamp()
        with connect_to_db() as db:
            for table_name in tables:
                query = (
                    f"SELECT * FROM {table_name}"  # nosec B608
                    + " WHERE last_updated > :s;"  # nosec B608
                )
                logger.debug(f"Query for {table_name}: {query}")
                try:
                    rows = db.run(
                        query,
                        s=last_ingestion_timestamp,
                    )
                    if rows:
                        column = [col["name"] for col in db.columns]
                        tables_data[table_name] = [
                            dict(zip(column, row)) for row in rows
                        ]
                        logger.info(
                            f"Fetched new data from {table_name} successfully."
                        )
                    else:
                        logger.info(f"No new data in {table_name}")
                except DatabaseError:
                    logger.error(
                        f"Database error, fetching data {table_name}",
                        exc_info=True,
                    )
                except Exception:
                    logger.error(
                        f"Failed to fetch data from {table_name}",
                        exc_info=True,
                    )
        update_last_ingestion_timestamp()
        return tables_data

    except Exception as err:
        logger.error(f"Database connection failed: {err}", exc_info=True)
        # raise err


# Transformation helper functions
def dim_date(*datasets):
    """
    Create a comprehensive dim_date table with a date_id column from multiple datasets.
    
    Args:
        *datasets (list[pd.DataFrame]): List of datasets containing date columns.
    
    Returns:
        pd.DataFrame: dim_date table with a date_id column.
    
    Logs:
        If no valid date columns are found in the datasets.
    """
    try:
        if not datasets:
            logger.warning(f"Invalid dates {datasets} data")
            return None
        
        # Extracting and combining all unique dates from relevant columns
        all_dates = []
        for dataset in datasets:
            if not isinstance(dataset, pd.DataFrame):
                logger.error(f"Expected a DataFrame, but got {type(dataset)}")
                return # not sure what to do in this case
            
            date_columns = [col for col in dataset.columns if 'date' in col or 'created_at' in col or 'last_updated' in col]
            if not date_columns:
                logger.warning(f"Skipping dataset without date columns: {dataset}")
                continue 

            for column in date_columns:
                try:
                    all_dates.append(pd.to_datetime(dataset[column], format="mixed").dropna().dt.date)
                except Exception as err:
                    logger.error(f"Error parsing column '{column}' in dataset: {err}")
                    return # not sure what to do in this case
                
        if not all_dates:
            logger.error("No valid date columns found in the provided datasets")
            return
        
        # Flattening the list and getting unique dates
        unique_dates = pd.Series(pd.concat(all_dates).unique())
        unique_dates = unique_dates.sort_values().reset_index(drop=True)

        # Generating a continuous date range (using the min and max dates from unique_dates)
        # freq='D' helps with missing date data
        date_range = pd.date_range(start=unique_dates.min(), end=unique_dates.max(), freq='D')

        # Creating the dim_date table
        dim_date = pd.DataFrame({'date': date_range})
        dim_date['date_id'] = dim_date['date'].dt.strftime('%Y%m%d').astype(int)
        dim_date['year'] = dim_date['date'].dt.year
        dim_date['month'] = dim_date['date'].dt.month
        dim_date['day'] = dim_date['date'].dt.day
        dim_date['day_of_week'] = dim_date['date'].dt.dayofweek
        dim_date['day_name'] = dim_date['date'].dt.day_name()
        dim_date['month_name'] = dim_date['date'].dt.month_name()
        dim_date['quarter'] = dim_date['date'].dt.quarter
        # Not needed, just thought it was interesting to add
        # dim_date['day_of_week'] = dim_date['date'].dt.dayofweek
        # dim_date['is_weekend'] = dim_date['day_of_week'].isin([5, 6])

        dim_date = dim_date[[
            'date_id', 'date', 'year', 'month', 'day', 
            'day_of_week','day_name', 'month_name', 
            'quarter'
        ]]
        return dim_date
    except Exception as err:
        logger.error(f"Unexpected exception has occurred: {err}")


def transform_dim_counterparty(counterparty_data, address_data):
    """
    Merges counterparty data and address data and transforms
    the merged data into dim_counterparty.

    Args:
        counterparty_data (list[dict] or pd.DataFrame): Counterparty dataset.
        address_data (list[dict] or pd.DataFrame): Address dataset.

    Returns:
        pd.DataFrame: Transformed dim_counterparty table.

    Logs:
        If required columns are missing or if inputs are invalid.
    """
    try:
        dim_counterparty = (
            pd.DataFrame(counterparty_data)
            if not isinstance(counterparty_data, pd.DataFrame)
            else counterparty_data.copy()
        )
        dim_address = (
            pd.DataFrame(address_data)
            if not isinstance(address_data, pd.DataFrame)
            else address_data.copy()
        )
        # Assuming this columns must exist for now
        dim_address.drop(columns=["created_at", "last_updated"], inplace=True)
        dim_counterparty.drop(
            columns=[
                "created_at",
                "last_updated",
                "commercial_contact",
                "delivery_contact",
            ],
            inplace=True,
        )
        dim_address = dim_address.rename(
            columns={
                "address_line_1": "counterparty_legal_address_line_1",
                "address_line_2": "counterparty_legal_address_line_2",
                "district": "counterparty_legal_district",
                "city": "counterparty_legal_city",
                "postal_code": "counterparty_legal_postal_code",
                "country": "counterparty_legal_country",
                "phone": "counterparty_legal_phone_number",
            }
        )
        merged_df = pd.merge(
            dim_counterparty,
            dim_address,
            left_on="legal_address_id",
            right_on="address_id",
            how="inner",
        )
        merged_df.drop(columns=["legal_address_id", "address_id"], inplace=True)

        return merged_df
    except Exception as err:
        logger.error(f"Unexpected error occurred in transform_dim_counterparty: {err}")
