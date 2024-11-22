import pandas as pd
import json
from botocore.exceptions import ClientError
from src.transformation.transformation import (
    logger,
    s3_client,
    S3_INGESTION_BUCKET,
)


# Transformation helper functions
def load_data_from_s3_ingestion(key):
    """
    Loads data from the ingestion bucket
    using a given key

    ARGS:
        key: string - s3 key file

    RETURNS:
        data from the ingestion bucket
    """
    try:
        if not key or not isinstance(key, str):
            raise ValueError("Invalid s3 key. Key must be non empty string.")

        logger.info(f"Attempting to load data from s3: {key}")
        response = s3_client.get_object(Bucket=S3_INGESTION_BUCKET, Key=key)
        logger.debug(f"s3 response {response['ResponseMetadata']}")

        if "Body" not in response:
            raise ValueError(
                f"No 'Body' content in s3 response for key: {key}"
            )

        content = response["Body"].read().decode("utf-8")
        logger.info(f"Successfully loaded data from s3 key: {key}")
        data = json.loads(content)

        return data
    except ClientError as ce:
        if ce.response["Error"]["Code"] == "NoSuchBucket":
            logger.error(
                f"The specified key {key} does not exist in bucket: {ce}"
            )
        logger.error(f"Client error occurred: {ce}")
    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
    except Exception as err:
        logger.error(
            f"Unexpected error occurred fetching data from s3 ingestion: {err}"
        )


def dim_date(*datasets):
    """
    Create a comprehensive dim_date table with
    a date_id column from multiple datasets.

    Args:
        *datasets (list[pd.DataFrame]): List of datasets
        containing date columns.

    Returns:
        pd.DataFrame: dim_date table with a date_id column.

    Logs:
        If no valid date columns are found in the datasets.
    """
    try:
        if not datasets:
            logger.warning(f"Datasets can't be empty: {datasets}")
            return None

        # Extracting and combining all unique dates from relevant columns
        all_dates = []
        for dataset in datasets:
            if not isinstance(dataset, pd.DataFrame):
                logger.error(f"Expected a DataFrame, but got {type(dataset)}")
                return  # not sure what to do in this case

            date_columns = [
                col
                for col in dataset.columns
                if "date" in col
                or "created_at" in col
                or "last_updated" in col
            ]
            if not date_columns:
                logger.warning(
                    f"Skipping dataset without date columns: {dataset}"
                )
                continue

            for column in date_columns:
                try:
                    all_dates.append(
                        pd.to_datetime(dataset[column], format="mixed")
                        .dropna()
                        .dt.date
                    )
                except Exception as err:
                    logger.error(
                        f"Error parsing column '{column}' in dataset: {err}"
                    )
                    return  # not sure what to do in this case

        if not all_dates:
            logger.error(
                "No valid date columns found in the provided datasets"
            )
            return

        # Flattening the list and getting unique dates
        unique_dates = pd.Series(pd.concat(all_dates).unique())
        unique_dates = unique_dates.sort_values().reset_index(drop=True)

        # Generating a continuous date range
        # (using the min and max dates from unique_dates)
        # freq='D' helps with missing date data
        date_range = pd.date_range(
            start=unique_dates.min(), end=unique_dates.max(), freq="D"
        )

        # Creating the dim_date table
        dim_date = pd.DataFrame({"date": date_range})
        dim_date["date_id"] = (
            dim_date["date"].dt.strftime("%Y%m%d").astype(int)
        )
        dim_date["year"] = dim_date["date"].dt.year.astype("int64")
        dim_date["month"] = dim_date["date"].dt.month.astype("int64")
        dim_date["day"] = dim_date["date"].dt.day.astype("int64")
        dim_date["day_of_week"] = dim_date["date"].dt.dayofweek.astype("int64")
        dim_date["day_name"] = dim_date["date"].dt.day_name()
        dim_date["month_name"] = dim_date["date"].dt.month_name()
        dim_date["quarter"] = dim_date["date"].dt.quarter.astype("int64")
        # Not needed, just thought it was interesting to add
        # dim_date['day_of_week'] = dim_date['date'].dt.dayofweek
        # dim_date['is_weekend'] = dim_date['day_of_week'].isin([5, 6])

        dim_date = dim_date[
            [
                "date_id",
                "date",
                "year",
                "month",
                "day",
                "day_of_week",
                "day_name",
                "month_name",
                "quarter",
            ]
        ]
        return dim_date
    except Exception as err:
        logger.error(f"Unexpected exception has occurred: {err}")


def transform_dim_counterparty(counterparty_data, address_data):
    """
    Merges counterparty data and address data and transforms
    the merged data into dim_counterparty.

    Args:
        counterparty_data (list[dict]): Counterparty dataset.
        address_data (list[dict]): Address dataset.

    Returns:
        pd.DataFrame: Transformed dim_counterparty table.

    Logs:
        If required columns are missing or if inputs are invalid.
    """
    try:
        dim_counterparty = pd.DataFrame(counterparty_data)
        dim_address = pd.DataFrame(address_data)

        required_counterparty_columns = {
            "counterparty_id",
            "counterparty_legal_name",
            "legal_address_id",
        }
        required_address_columns = {
            "address_id",
            "address_line_1",
            "address_line_2",
            "district",
            "city",
            "postal_code",
            "country",
            "phone",
        }
        final_columns = [
            "counterparty_id",
            "counterparty_legal_name",
            "counterparty_legal_address_line_1",
            "counterparty_legal_address_line_2",
            "counterparty_legal_district",
            "counterparty_legal_city",
            "counterparty_legal_postal_code",
            "counterparty_legal_country",
            "counterparty_legal_phone_number",
        ]

        missing_cp_cols = required_counterparty_columns - set(
            dim_counterparty.columns
        )
        missing_a_columns = required_address_columns - set(dim_address.columns)

        if missing_cp_cols or missing_a_columns:
            missing_info = []
            missing_cp_cols = required_counterparty_columns - set(
                dim_counterparty.columns
            )
            missing_a_columns = required_address_columns - set(
                dim_address.columns
            )
            if missing_cp_cols:
                missing_info.append(
                    f"counterparty_data is missing columns: {missing_cp_cols}"
                )
            if missing_a_columns:
                missing_info.append(
                    f"address_data is missing columns: {missing_a_columns}"
                )
            logger.error(f"{missing_info}")
            return pd.DataFrame(columns=final_columns)

        dim_address.drop(
            columns=["created_at", "last_updated"],
            inplace=True,
            errors="ignore",
        )

        dim_counterparty.drop(
            columns=[
                "created_at",
                "last_updated",
                "commercial_contact",
                "delivery_contact",
            ],
            inplace=True,
            errors="ignore",
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

        merged_df.drop(
            columns=["legal_address_id", "address_id"],
            inplace=True,
            errors="ignore",
        )

        return merged_df[final_columns]
    except Exception as err:
        logger.error(
            f"Unexpected error occurred in transform_dim_counterparty: {err}"
        )


def transform_fact_sales_order(sales_order):
    """
    Transforms sales transactions into
    fact_sales_order with error handling and validation.

    Args:
        sales_order (list[dict]):
        Raw sales order data.

    Returns:
        pd.DataFrame: Transformed fact_sales_order DataFrame.

    Logs:
        If required columns are missing or invalid data is provided.
    """
    try:
        fact_sales_order = (
            pd.DataFrame(sales_order)
            if not isinstance(sales_order, pd.DataFrame)
            else sales_order.copy()
        )
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

        # Convert data types
        # Assuming the data we get is already of the type we want
        # fact_sales_order["sales_order_id"] = fact_sales_order[
        #     "sales_order_id"
        # ].astype(int)
        # fact_sales_order["sales_staff_id"] = fact_sales_order[
        #     "sales_staff_id"
        # ].astype(int)
        # fact_sales_order["counterparty_id"] = fact_sales_order[
        #     "counterparty_id"
        # ].astype(int)
        # fact_sales_order["units_sold"] = fact_sales_order[
        #   "units_sold"
        # ].astype(int)
        # fact_sales_order["unit_price"] = fact_sales_order[
        #   "unit_price"
        # ].astype(
        #     float
        # )
        # fact_sales_order["currency_id"] = fact_sales_order[
        #   "currency_id"
        # ].astype(
        #     int
        # )
        # fact_sales_order["design_id"] = fact_sales_order[
        #   "design_id"
        # ].astype(int)
        # fact_sales_order[
        #   "agreed_delivery_location_id"
        # ] = fact_sales_order[
        #     "agreed_delivery_location_id"
        # ].astype(int)

        # Converting to pd.datetime first
        datetime_columns = ["created_date", "last_updated_date"]
        for col in datetime_columns:
            try:
                fact_sales_order[col] = pd.to_datetime(
                    fact_sales_order[col], format="mixed"
                )
            except Exception as err:
                logger.error(
                    ValueError(f"Error parsing datetime column '{col}': {err}")
                )
        # fact_sales_order["created_date"] = pd.to_datetime(
        #     fact_sales_order["created_date"],
        #     format="mixed",
        # )
        # fact_sales_order["last_updated_date"] = pd.to_datetime(
        #     fact_sales_order["last_updated_date"],
        #     format="mixed",
        # )

        # Extracting time and date separately
        fact_sales_order["created_time"] = fact_sales_order[
            "created_date"
        ].dt.time
        fact_sales_order["created_date"] = fact_sales_order[
            "created_date"
        ].dt.date

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
    except Exception as err:
        logger.error(
            f"Unexpected error occurred in transform_fact_sales_order: {err} "
        )
        # return None


def transform_dim_design(design_data):
    """
    Transforms design data into dim_design with error handling and logging.

    Args:
        design_data (list[dict]):
            Raw design data.

    Returns:
        pd.DataFrame: Transformed dim_design DataFrame.

    Logs:
        If required columns are missing or if inputs are invalid.
    """
    try:
        if not design_data:
            logger.warning(f"Design data is empty: {design_data}")
            return None

        dim_design = (
            pd.DataFrame(design_data)
            if not isinstance(design_data, pd.DataFrame)
            else design_data.copy()
        )
        dim_design.drop(columns=["created_at", "last_updated"], inplace=True)
        # dim_design = dim_design.rename(
        #     columns={
        #         "design_id": "design_id",
        #         "design_name": "design_name",
        #         "file_location": "file_location",
        #         "file_name": "file_name",
        #     }
        # )
        dim_design = dim_design[
            [
                "design_id",
                "design_name",
                "file_location",
                "file_name",
            ]
        ]
        # Validation and converting data types
        # dim_design['design_id'] = dim_design['design_id'].astype(int)
        # dim_design['design_name'] = dim_design[
        #   'design_name'
        # ].astype('string')
        # dim_design['file_location'] = (
        #     dim_design['file_location'].astype('string')
        # )
        # dim_design['file_name'] = dim_design['file_name'].astype('string')
        dim_design.drop_duplicates(inplace=True)
        return dim_design
    except Exception as err:
        logger.error(
            f"Unexpected error occurred with transform_dim_design: {err}"
        )


def transform_dim_staff(staff_data, department_data):
    """
    Transforms staff and department records into the
    required format for dim_staff.

    Args:
        staff_data (list[dict]):
            Raw staff data.
        department_data (list[dict]):
            Raw department data.

    Returns:
        pd.DataFrame: Transformed dim_staff DataFrame.

    Logs:
        If required columns are missing or if inputs are invalid.
    """
    try:
        staff = (
            pd.DataFrame(staff_data)
            if not isinstance(staff_data, pd.DataFrame)
            else staff_data.copy()
        )
        department = (
            pd.DataFrame(department_data)
            if not isinstance(department_data, pd.DataFrame)
            else department_data.copy()
        )
        # Dropping unnecessary columns
        department.drop(
            columns=["manager", "created_at", "last_updated"], inplace=True
        )
        staff.drop(columns=["created_at", "last_updated"], inplace=True)

        dim_staff = pd.merge(
            staff, department, on="department_id", how="inner"
        )
        # Why is department_id twice here?
        dim_staff.drop(
            columns=["department_id", "department_id"], inplace=True
        )

        # Converting datatypes, not sure if its necessary
        # dim_staff["staff_id"] = dim_staff["staff_id"].astype(int)
        # dim_staff["first_name"] = dim_staff["first_name"].astype(str)
        # dim_staff["last_name"] = dim_staff["last_name"].astype(str)
        # dim_staff["department_name"] = dim_staff[
        #   "department_name"
        #  ].astype(str)
        # dim_staff["location"] = dim_staff[
        #   "department_location"
        # ].astype(str)

        # Reordering columns
        dim_staff = dim_staff[
            [
                "staff_id",
                "first_name",
                "last_name",
                "department_name",
                "location",
                "email_address",
            ]
        ]

        return dim_staff
    except Exception as err:
        logger.error(
            f"Unexpected error occurred with transform_dim_design: {err}"
        )


def transform_dim_currency(currency_data):
    """
    Transforms raw currency data into dim_currency.

    Args:
    currency_data = [{
            "currency_id": 1,
            "currency_code": "GBP",
            "created_at": "2022-11-03 14:20:49.962000",
            "last_updated": "2022-11-03 14:20:49.962000"
        }
    ]

    Returns:
        DataFrame for dim_currency.
    Logs:
        an unexpected error during transform
    """
    try:

        currency_mapping = {
            "USD": "US Dollar",
            "EUR": "Euro",
            "GBP": "British Pound",
            "JPY": "Japanese Yen",
        }

        if not currency_data:
            logger.warning("No currency data provided.")
            return None

        # dim_currency = (
        #     pd.DataFrame(currency_data)
        #     if not isinstance(currency_data, pd.DataFrame)
        #     else currency_data.copy()
        # )

        dim_currency = pd.DataFrame(
            currency_data,
            columns=[
                "currency_id",
                "currency_code",
                "created_at",
                "last_updated",
            ],
        )

        dim_currency["currency_name"] = (
            dim_currency["currency_code"]
            .map(currency_mapping)
            .fillna("Unknown Currency")
        )

        dim_currency = dim_currency[
            [
                "currency_id",
                "currency_code",
                "currency_name",
            ]
        ]

        dim_currency.drop_duplicates(inplace=True)

        return dim_currency
    except Exception as err:
        logger.error(
            f"Unexpected error occurred with transform_dim_currency: {err}"
        )


def transform_dim_location(address_data):
    """
    Transforms address data into dim_location with
    error handling and validation.

    Args:
        address_data (list[dict]):
            Raw address data.

    Returns:
        pd.DataFrame: Transformed dim_location
        DataFrame or None if an error occurs.

    Logs:
        Unexpected error
    """
    try:
        # not sure if we want to raise the errors
        if not isinstance(address_data, list):
            raise ValueError("Input must be a list of dictionaries.")

        if not address_data:
            raise ValueError("Input must be populated.")

        dim_address = (
            pd.DataFrame(address_data)
            if not isinstance(address_data, pd.DataFrame)
            else address_data.copy()
        )

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
        dim_address.drop_duplicates(inplace=True)
        return dim_address
    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
    except Exception as err:
        logger.error(
            f"Unexpected error occurred with transform_dim_location: {err}"
        )


def transform_dim_transaction(transaction_data):
    """
    Transforms raw transaction data into dim_transaction
    with error handling and validation.

    Args:
        transaction_data (list[dict]):
            Raw transaction data.

    Returns:
        pd.DataFrame: Transformed dim_transaction DataFrame
        or None if an error occurs.
    """
    try:
        if not isinstance(transaction_data, list):
            raise ValueError("Input must be a list of dictionaries.")

        if not transaction_data:
            raise ValueError("transaction_data must be populated")

        dim_transaction = (
            pd.DataFrame(transaction_data)
            if not isinstance(transaction_data, pd.DataFrame)
            else transaction_data.copy()
        )

        dim_transaction.drop(
            columns=["created_at", "last_updated"], inplace=True
        )
        dim_transaction.drop_duplicates(inplace=True)
        return dim_transaction
    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
    except Exception as err:
        logger.error(f"Unexpected error occurred with dim_transaction: {err}")
        # return None


def transform_dim_payment_types(
    payment_types_data,
):
    """
    Transforms raw payment types data into dim_payment_type
    with error handling and validation.

    Args:
        payment_types_data (list[dict]):
            Raw payment types data.

    Returns:
        pd.DataFrame: Transformed dim_payment_type DataFrame
        or None if an error occurs.
    """
    try:
        dim_payment_type = (
            pd.DataFrame(payment_types_data)
            if not isinstance(payment_types_data, pd.DataFrame)
            else payment_types_data.copy()
        )
        dim_payment_type.drop(
            columns=["created_at", "last_updated"], inplace=True
        )
        return dim_payment_type
    except Exception as err:
        logger.error(f"Unexpected error occurred with dim_payment_type: {err}")
        # return None


def transform_fact_payment(
    payments_data, transactions_data, payment_type_data
):
    """
    Transforms raw payments, transactions, and payment
    type data into fact_payment.

    Args:
        payments_data (list[dict]):
            Raw payments data.
        transactions_data (list[dict]):
            Raw transactions data.
        payment_type_data (list[dict]):
            Raw payment type data.

    Returns:
        pd.DataFrame: Transformed fact_payment DataFrame
        or None if an error occurs.
    """
    try:
        # Converting inputs to DataFrame if necessary
        payments_df = (
            pd.DataFrame(payments_data)
            if not isinstance(payments_data, pd.DataFrame)
            else payments_data.copy()
        )
        transactions_df = (
            pd.DataFrame(transactions_data)
            if not isinstance(transactions_data, pd.DataFrame)
            else transactions_data.copy()
        )
        payment_type_df = (
            pd.DataFrame(payment_type_data)
            if not isinstance(payment_type_data, pd.DataFrame)
            else payment_type_data.copy()
        )

        # Merging payments with transactions on `transaction_id`
        fact_payment = pd.merge(
            payments_df,
            transactions_df[["transaction_id", "transaction_type"]],
            on="transaction_id",
            how="left",
        )

        # Merging with payment_type data on `payment_type_id`
        fact_payment = pd.merge(
            fact_payment,
            payment_type_df[["payment_type_id", "payment_type_name"]],
            on="payment_type_id",
            how="left",
        )

        # Can also merge with sales_order on 'sales_order_id'

        # Converting 'created_at' and 'last_updated'
        # into date and time components
        fact_payment["created_at"] = pd.to_datetime(
            fact_payment["created_at"], format="mixed"
        )
        fact_payment["created_date"] = fact_payment["created_at"].dt.date
        fact_payment["created_time"] = fact_payment["created_at"].dt.time

        fact_payment["last_updated"] = pd.to_datetime(
            fact_payment["last_updated"], format="mixed"
        )
        fact_payment["last_updated_date"] = fact_payment[
            "last_updated"
        ].dt.date
        fact_payment["last_updated_time"] = fact_payment[
            "last_updated"
        ].dt.time

        # Rename columns to match the schema
        fact_payment = fact_payment.rename(
            columns={
                "payment_id": "payment_id",
                "counterparty_id": "counterparty_id",
                "payment_amount": "payment_amount",
                "currency_id": "currency_id",
                "payment_type_id": "payment_type_id",
                "payment_type_name": "payment_type_name",
                "paid": "paid",
                "payment_date": "payment_date",
            }
        )

        # Ensuring columns are in the correct order
        fact_payment = fact_payment[
            [
                "payment_id",
                "created_date",
                "created_time",
                "last_updated_date",
                "last_updated_time",
                "transaction_id",
                "counterparty_id",
                "payment_amount",
                "currency_id",
                "payment_type_id",
                "payment_type_name",
                "paid",
                "payment_date",
            ]
        ]

        # Converting data types
        fact_payment["payment_id"] = fact_payment["payment_id"].astype(int)
        fact_payment["transaction_id"] = fact_payment["transaction_id"].astype(
            int
        )
        fact_payment["counterparty_id"] = fact_payment[
            "counterparty_id"
        ].astype(int)
        fact_payment["payment_amount"] = fact_payment["payment_amount"].astype(
            float
        )
        fact_payment["currency_id"] = fact_payment["currency_id"].astype(int)
        fact_payment["payment_type_id"] = fact_payment[
            "payment_type_id"
        ].astype(int)
        fact_payment["paid"] = fact_payment["paid"].astype(bool)
        fact_payment["payment_date"] = pd.to_datetime(
            fact_payment["payment_date"]
        ).dt.date

        return fact_payment
    except Exception as err:
        logger.error(f"Unexpected error occurred with fact_payment: {err}")


def transform_dim_department(department_data):
    """
    Transforms raw department data into dim_department
    with error handling and validation.

    Args:
        department_data (list[dict]):
            Raw department data.

    Returns:
        pd.DataFrame: Transformed dim_department
            DataFrame or None if an error occurs.
    """
    try:
        if isinstance(department_data, list) and not department_data:
            logger.info("Received empty list. Returning an empty DataFrame.")
            return pd.DataFrame(
                columns=[
                    "department_id",
                    "department_name",
                    "location",
                    "manager",
                ]
            )

        dim_department = (
            pd.DataFrame(department_data)
            if not isinstance(department_data, pd.DataFrame)
            else department_data.copy()
        )

        # Only selecting relevant columns
        dim_department = dim_department[
            ["department_id", "department_name", "location", "manager"]
        ]

        # Renaming columns, i dont think its needed
        # dim_department = dim_department.rename(columns={
        #     "department_id": "department_id",
        #     "department_name": "department_name",
        #     "location": "location",
        #     "manager": "manager",
        # })

        # Ensure data types might not be necessary
        # but can be added here

        dim_department.drop_duplicates(inplace=True)

        return dim_department
    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
    except Exception as e:
        logger.error(f"Error in transform_dim_department: {e}")
