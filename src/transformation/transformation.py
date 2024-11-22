from datetime import datetime
from io import StringIO
import logging
import boto3
import src.transformation.transformationutil as util
import pandas as pd
import json
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")

S3_INGESTION_BUCKET = os.getenv(
    "S3_INGESTION_BUCKET",
)
S3_PROCESSED_BUCKET = os.getenv("S3_PROCESSED_BUCKET")
HISTORY_FOLDER = "history"
PROCESSED_FOLDER = "processed"


def save_transformed_data(table_name, data):
    """
    Save transformed DataFrames as
    Parquet files to the processed S3 bucket.
    """
    try:
        now = datetime.utcnow()
        timestamp = now.strftime("%Y%m%d%H%M%S")

        # Defining S3 paths
        processed_key = f"{PROCESSED_FOLDER}/{table_name}/{timestamp}.parquet"
        history_key = f"{HISTORY_FOLDER}/{table_name}/{timestamp}.parquet"

        parquet_buffer = StringIO()
        data.to_parquet(parquet_buffer, index=False, engine="pyarrow")

        s3_client.put_object(
            Bucket=S3_PROCESSED_BUCKET,
            Key=processed_key,
            Body=parquet_buffer.getvalue(),
        )
        logger.info(f"Saved latest data to: {processed_key}")

        # If it's a fact 'sales_order' table, append to the history folder
        if table_name.startswith("fact"):
            s3_client.put_object(
                Bucket=S3_PROCESSED_BUCKET,
                Key=history_key,
                Body=parquet_buffer.getvalue(),
            )
            logger.info(f"Saved historical data to: {history_key}")

    except Exception as err:
        logger.error(
            f"Error saving transformed data for table {table_name}: {err}"
        )


# Predefined functions for ease of lookup
TRANSFORMATION_FUNCTIONS = {
    "sales_order": util.transform_fact_sales_order,
    "staff": util.transform_dim_staff,
    "address": util.transform_dim_location,
    "design": util.transform_dim_design,
    "currency": util.transform_dim_currency,
    "counterparty": util.transform_dim_counterparty,
}


def lambda_handler(event, context):
    """Lambda handler function."""
    logger.info("Received event: %s", json.dumps(event))

    try:
        # event contains the S3 object key of the ingested data
        # from being invoked by s3 ingestion bucket
        for record in event["Records"]:
            s3_key = record["s3"]["object"]["key"]
            logger.info(f"Processing file: {s3_key}")
            try:
                # Fetching data from key
                table_name = util.extract_table_name(s3_key=s3_key)
                transform_function = TRANSFORMATION_FUNCTIONS.get(
                    table_name, None
                )

                if not transform_function:
                    logger.warning(
                        f"No transformation logic exists, table: {table_name}"
                    )
                    continue

                # Load data from s3
                data = util.load_data_from_s3_ingestion(key=s3_key)
                transformed_data = util.process_table(
                    table_name, transform_function, data
                )

                # Save transformed data
                if transformed_data is not None:
                    save_transformed_data(table_name, transformed_data)

                # Handle dim date if 'sales_order'
                if table_name == "sales_order":
                    transformed_data = transform_function(data)
                    dim_date = util.dim_date(pd.DataFrame(data))
                    save_transformed_data("dim_date", dim_date)

            except Exception as record_error:
                logger.error(f"Error parsing record {s3_key}: {record_error}")
                continue

        logger.info("Transformation process completed")
        return {"statusCode": 200, "body": "Transformation complete"}
    except Exception as err:
        logger.error(f"Error in transformation lambda: {err}")
