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

S3_INGESTION_BUCKET = os.getenv(
    "S3_INGESTION_BUCKET",
)
S3_PROCESSED_BUCKET = os.getenv("S3_PROCESSED_BUCKET")


def load_data_from_s3_ingestion(key):
    """
    Loads data from the ingestion bucket
    using a given key

    ARGS:
        key: s3 key file
    
    RETURNS:
        data from the ingestion bucket
    """
    response = s3_client.get_object(Bucket=S3_INGESTION_BUCKET, Key=key)
    logger.info(f"s3 replied with response {response}")
    if "Body" in response:
        data = json.loads(response["Body"].read().decode("utf-8"))
    return data


def transform_dim_location(address_data):
    """
    Transforms address data into dim_location.
    """
    dim_address = pd.DataFrame(address_data)
    dim_address.drop(columns=["created_at", "last_updated"], inplace=True)
    dim_address = dim_address.rename(
        columns={
            "address_id": "location_id",
            "address_line_1": "address_line_1",
            "address_line_2": "address_line_2",
            "district": "district",
            "city": "city",
            "postal_code": "postal_code",
            "country": "country",
            "phone": "phone",
        }
    )
    return dim_address        



def transform_dim_transaction(transaction_data):
    """
    Transforms transaction data into dim_transaction.
    """
    pass


def transform_dim_payment_type(payment_type_data):
    """Transforms payment type data into dim_payment_type."""
    pass


def transform_dim_department(department_data):
    """ """
    pass

    
def transform_fact_purchase_orders(transactions, dim_date):
    """
    Transforms purchase transactions into fact_purchase_orders.
    """
    pass


def transform_fact_payment(payments_data, dim_date, payment_types_data):
    """
    Transforms payment data into fact_payment.
    """
    pass


def transform_data(data):
    """
    
    """
    
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
    """
    Save transformed DataFrames as
    Parquet files to the processed S3 bucket.
    """
    for table_name, df in dataframes.items():
        if not df.empty:
            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)
            file_path = f"processed/{table_name}/{table_name}"
            s3_key = (
                file_path
                + f"_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
            )
            s3_client.put_object(
                Bucket=S3_PROCESSED_BUCKET,
                Key=s3_key,
                Body=buffer.getvalue(),
            )
            logger.info(
                f"Saved {table_name} to s3://{S3_PROCESSED_BUCKET}/{s3_key}"
            )

# Commented for now
# TRANSFORMATION_FUNCTIONS = {
#     "counterparty": transform_dim_counterparty,
#     "currency": transform_dim_currency,
#     "department": transform_dim_department,
#     "design": transform_dim_design,
#     "staff": transform_dim_staff,
#     "payment": transform_dim_payment_type,
#     "transaction": transform_dim_transaction,
# }


def lambda_handler(event, context):
    """Lambda handler function."""
    logger.info("Received event: %s", json.dumps(event))

    # table_data = {}

    # Assuming event contains the S3 object key of the ingested data
    for record in event["Records"]:
        s3_key = record["s3"]["object"]["key"]
        logger.info(f"Processing file: {s3_key}")

        # table_name = s3_key.split("/")[1]
        # if table_name in TRANSFORMATION_FUNCTIONS:
        #     # print(TRANSFORMATION_FUNCTIONS[table_name])
        #     logger.warning(f"Skipping file with invalid format: {s3_key}")
        #     continue

    # Creating dimensions and fact tables independently
    # dim_counterparty
    # dim_currency
    # dim_staff
    # dim_design
    # dim_location
    # dim_payment_type

    # fact_sales_order based on (transactions, dim_date)

    # NOT NEEDED FOR THE MVP
    # fact_purchase_orders based on (transactions, dim_date)
    # fact_payment based on (payments, dim_date, payment_types)
    logger.info("Transformation process completed")
    return {"statusCode": 200, "body": "Transformation complete"}
