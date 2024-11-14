from datetime import datetime
from io import BytesIO
import logging
import boto3
import pandas as pd
import json
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")

S3_INGESTION_BUCKET = os.getenv("S3_INGESTION_BUCKET", )
S3_PROCESSED_BUCKET = os.getenv("S3_PROCESSED_BUCKET")


def load_data_from_s3(key):
    response = s3_client.get_object(
        Bucket=S3_INGESTION_BUCKET,
        Key=key
    )
    logger.info(f"s3 replied with response {response}")
    if 'Body' in response:
        data = json.loads(response['Body'].read().decode('utf-8'))
    return data


def transform_data(data):
    """"""
    df = pd.DataFrame(data=data)
    # return {
    #     "fact_sales_order": fact_sales_order,
    #     "dim_date": dim_date,
    #     "dim_counterparty": dim_counterparty,
    #     "dim_staff": dim_staff,
    #     "dim_location": dim_location,
    #     "dim_design": dim_design,
    #     "dim_currency": dim_currency
    # }
    pass


def save_transformed_data(dataframes):
    """Save transformed DataFrames as Parquet files to the processed S3 bucket."""
    for table_name, df in dataframes.items():
        if not df.empty:
            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            s3_key = f"processed/{table_name}/{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
            s3_client.put_object(Bucket=S3_PROCESSED_BUCKET, Key=s3_key, Body=buffer.getvalue())
            logger.info(f"Saved {table_name} to s3://{S3_PROCESSED_BUCKET}/{s3_key}")


def lambda_handler(event, context):
    """Lambda handler function."""
    logger.info("Starting transformation process")
    
    # Assuming event contains the S3 object key of the ingested data
    for record in event['Records']:
        s3_key = record['s3']['object']['key']
        logger.info(f"Processing file: {s3_key}")
        
        raw_data = load_data_from_s3(s3_key)
        
        logger.info(f"Transforming raw data from {s3_key}")
        transformed_data = transform_data(raw_data)
        
        # Save transformed data back to S3 in processed format
        logger.info("Saving transformed data from into processed bucket")
        save_transformed_data(transformed_data)
    
    logger.info("Transformation process completed")
    return {
        "statusCode": 200,
        "body": "Transformation complete"
    }