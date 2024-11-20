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


def load_data_from_s3(key):
    """ """
    response = s3_client.get_object(Bucket=S3_INGESTION_BUCKET, Key=key)
    logger.info(f"s3 replied with response {response}")
    if "Body" in response:
        data = json.loads(response["Body"].read().decode("utf-8"))
    return data


def create_dim_date(dates):
    """
    Creates the dim_date table from a list of datetime strings.

    ARGS:
    a list of dates
    ['2022-11-03T14:20:49.962000',]

    RETURNS:
    a dataframe reformatted dates
    """

    if not dates:
        logger.warning(f"Invalid dates {dates} data")
        return None

    dates = pd.to_datetime(pd.Series(dates).drop_duplicates())
    dim_date = pd.DataFrame(
        {"date_id": range(1, len(dates) + 1), "date": dates}
    )
    dim_date["year"] = dim_date["date"].dt.year
    dim_date["month"] = dim_date["date"].dt.month
    dim_date["day"] = dim_date["date"].dt.day
    dim_date["day_of_week"] = dim_date["date"].dt.dayofweek
    dim_date["day_name"] = dim_date["date"].dt.day_name()
    dim_date["month_name"] = dim_date["date"].dt.month_name()
    dim_date["quarter"] = dim_date["date"].dt.quarter
    return dim_date


def transform_dim_location(address_data):
    """
    Transforms address data into dim_location.
    """
    dim_address = pd.DataFrame(address_data)
    dim_address.drop(columns=["created_at", "last_updated"], inplace=True)
    dim_address = dim_address.rename(
        columns={
            "address_id": "id",
            "address_line_1": "line_1",
            "address_line_2": "line_2",
            "district": "district",
            "city": "city",
            "postal_code": "post_code",
            "country": "country",
            "phone": "phone",
        }
    )
    return dim_address


def transform_dim_counterparty(counterparty_data, address_data):
    """
    Merges counterparty data and address data and transforms
    the merged data into dim_counterparty
    """
    dim_counterparty = pd.DataFrame(counterparty_data)
    dim_address = pd.DataFrame(address_data)
    dim_address.drop(columns=["created_at", "last_updated"], inplace=True)
    dim_counterparty.drop(
        columns=[
            "created_at",
            "last_updated",
            "commercial_contact",
            "delivery_contact"
        ],
        inplace=True
    )
    dim_address = dim_address.rename(
        columns={
            "address_line_1": "counterparty_legal_address_line_1",
            "address_line_2": "counterparty_legal_address_line_2",
            "district": "counterparty_legal_district",
            "city": "counterparty_legal_city",
            "postal_code": "counterparty_legal_postal_code",
            "country": "counterparty_legal_country",
            "phone": "counterparty_legal_phone_number"
        }
    )
    merged_df = pd.merge(
        dim_counterparty,
        dim_address,
        left_on="legal_address_id",
        right_on="address_id",
        how="inner"
    )
    merged_df.drop(columns=["legal_address_id", "address_id"], inplace=True)

    return merged_df


def transform_dim_currency(currency_data):
    """
    Transforms currency data into dim_currency.
    """
    pass


def transform_dim_staff(staff_data):
    """
    Transform records into the required format for dim_staff.
    """
    return pd.DataFrame(staff_data).rename(
        columns={
            "staff_id": "id",
            "first_name": "first_name",
            "last_name": "last_name",
            "department_id": "department_id",
            "email_address": "email",
        }
    )


def transform_dim_design(design_data):
    """
    Transforms design data into dim_design.
    """
    pass


def transform_dim_transaction(transaction_data):
    """
    Transforms transaction data into dim_transaction.
    """
    pass


def transform_dim_payment_type(payment_type_data):
    """Transforms payment type data into dim_payment_type."""
    pass


def transform_dim_department():
    """ """
    pass


def transform_fact_sales_order(sales_order):
    """
    Transforms sales transactions into fact_sales_order.
    """
    fact_sales_order = pd.DataFrame(sales_order)
    fact_sales_order = fact_sales_order.rename(
        columns={
            "sales_order_id": "sales_order_id",
            "created_at": "created_date",
            "last_updated": "last_updated_date",
            "staff_id": "sales_staff_id",
            "counterparty_id": "counterparty_id",
            "design_id": "design_id",
            "units_sold": "units_sold",
            "unit_price": "unit_price",
            "currency_id": "currency_id",
            "agreed_payment_date": "agreed_payment_date",
            "agreed_delivery_date": "agreed_delivery_date",
            "agreed_delivery_location_id": "agreed_delivery_location_id",
        }
    )

    # Converting to pd.datetime first
    fact_sales_order["created_date"] = pd.to_datetime(
        fact_sales_order["created_date"],
        format="mixed",
    )
    fact_sales_order["last_updated_date"] = pd.to_datetime(
        fact_sales_order["last_updated_date"],
        format="mixed",
    )

    # Extracting time and date separately
    fact_sales_order["created_time"] = fact_sales_order["created_date"].dt.time
    fact_sales_order["created_date"] = fact_sales_order["created_date"].dt.date

    # Extracting time and date separately
    fact_sales_order["last_updated_time"] = fact_sales_order[
        "last_updated_date"
    ].dt.time
    fact_sales_order["last_updated_date"] = fact_sales_order[
        "last_updated_date"
    ].dt.date

    fact_sales_order = fact_sales_order[
        [
            "sales_order_id",
            "created_date",
            "created_time",
            "last_updated_date",
            "last_updated_time",
            "sales_staff_id",
            "counterparty_id",
            "units_sold",
            "unit_price",
            "currency_id",
            "design_id",
            "agreed_payment_date",
            "agreed_delivery_date",
            "agreed_delivery_location_id",
        ]
    ]

    return fact_sales_order


def transform_fact_purchase_orders(transactions, dim_date):
    """
    Transforms purchase transactions into fact_purchase_orders.
    """
    pass


def transform_fact_payment(payments, dim_date, payment_types):
    """
    Transforms payment data into fact_payment.
    """
    pass


def transform_data(data):
    """"""
    # df = pd.DataFrame(data=data)

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


TRANSFORMATION_FUNCTIONS = {
    "counterparty": transform_dim_counterparty,
    "currency": transform_dim_currency,
    "department": transform_dim_department,
    "design": transform_dim_design,
    "staff": transform_dim_staff,
    "payment": transform_dim_payment_type,
    "transaction": transform_dim_transaction,
}


def lambda_handler(event, context):
    """Lambda handler function."""
    logger.info("Received event: %s", json.dumps(event))

    # table_data = {}

    # Assuming event contains the S3 object key of the ingested data
    for record in event["Records"]:
        s3_key = record["s3"]["object"]["key"]
        logger.info(f"Processing file: {s3_key}")

        table_name = s3_key.split("/")[1]
        if table_name in TRANSFORMATION_FUNCTIONS:
            # print(TRANSFORMATION_FUNCTIONS[table_name])
            logger.warning(f"Skipping file with invalid format: {s3_key}")
            continue

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


# For testing purposes
if __name__ == "__main__":
    # with open("src/event_payload.json") as f:
    #     event = json.load(f)
    # lambda_handler(event, None)
    with open("sales_order.json") as f:
        data = json.loads(f.read())

    # print(address_data[0]['created_at'], address_data[0]['last_updated'])
    # dates = list(pd.DataFrame(address_data)['created_at'])
    # + list(pd.DataFrame(address_data)['last_updated'])
    # dim_date = create_dim_date(dates)
    # print(dim_date)

    # dim_address = transform_dim_location(address_data)
    # print(dim_address)
    fact_sales_order = transform_fact_sales_order(data)
    print(fact_sales_order.head())
    with open("sales_order.txt", mode="w") as f:
        f.write(str(fact_sales_order.head()))
