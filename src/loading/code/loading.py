import boto3
import logging


logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3", region_name="eu-west-2")

TABLES = {
    "dim_date": ["date_id"],
    "dim_staff": ["staff_id"],
    "dim_location": ["location_id"],
    "dim_currency": ["currency_id"],
    "dim_design": ["design_id"],
    "dim_counterparty": ["counterparty_id"],
    "fact_sales_order": ["sales_record_id"],
}


def lambda_handler(event, context):
    pass
