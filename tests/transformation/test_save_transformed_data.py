import pandas as pd
from src.transformation.transformationutil import save_transformed_data
import logging

# from unittest.mock import patch


S3_BUCKET = "test-processed-bucket"
PROCESSED_FOLDER = "processed"
HISTORY_FOLDER = "history"


# @patch(
#     "src.transformation.transformation.S3_PROCESSED_BUCKET", "test_bucket"
# )
def test_save_transformed_data_valid_input(mock_s3_client):
    """
    Test saving valid data to S3.
    """
    table_name = "fact_sales_order"
    data = pd.DataFrame(
        {
            "id": [1, 2],
            "value": [100, 200],
        }
    )

    mock_s3_client.create_bucket(
        Bucket=S3_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    save_transformed_data(table_name, data, S3_BUCKET)

    # Validate processed file exists
    processed_key_prefix = f"{PROCESSED_FOLDER}/{table_name}/"
    response = mock_s3_client.list_objects_v2(
        Bucket=S3_BUCKET, Prefix=processed_key_prefix
    )
    assert "Contents" in response
    assert len(response["Contents"]) == 1

    # Validate history file exists
    history_key_prefix = f"{HISTORY_FOLDER}/{table_name}/"
    response = mock_s3_client.list_objects_v2(
        Bucket=S3_BUCKET, Prefix=history_key_prefix
    )
    assert "Contents" in response
    assert len(response["Contents"]) == 1


def test_save_transformed_data_empty_dataframe(mock_s3_client, caplog):
    """
    Test saving an empty DataFrame to S3.
    """
    caplog.set_level(logging.ERROR)
    table_name = "fact_sales_order"
    data = pd.DataFrame()

    mock_s3_client.create_bucket(
        Bucket=S3_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    save_transformed_data(table_name, data, S3_BUCKET)

    assert (
        f"Validation error in saving data for table: {table_name}"
        in caplog.text
    )


def test_save_transformed_data_invalid_input(mock_s3_client, caplog):
    """
    Test saving invalid data (not a DataFrame) to S3.
    """
    table_name = "fact_sales_order"
    # Invalid input
    data = {"id": [1, 2], "value": [100, 200]}

    mock_s3_client.create_bucket(
        Bucket=S3_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    save_transformed_data(table_name, data, S3_BUCKET)

    assert (
        f"Invalid or empty DataFrame provided for table: {table_name}"
        in caplog.text
    )


def test_save_transformed_data_no_history(mock_s3_client):
    """
    Test saving data for a table without historical tracking.
    """
    table_name = "dim_staff"
    data = pd.DataFrame(
        {
            "id": [1, 2],
            "name": ["Alice", "Bob"],
        }
    )
    mock_s3_client.create_bucket(
        Bucket=S3_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    save_transformed_data(table_name, data, S3_BUCKET)

    # Validate processed file exists
    processed_key_prefix = f"{PROCESSED_FOLDER}/{table_name}/"
    response = mock_s3_client.list_objects_v2(
        Bucket=S3_BUCKET, Prefix=processed_key_prefix
    )
    assert "Contents" in response
    assert len(response["Contents"]) == 1

    # Validate no history file exists
    history_key_prefix = f"{HISTORY_FOLDER}/{table_name}/"
    response = mock_s3_client.list_objects_v2(
        Bucket=S3_BUCKET, Prefix=history_key_prefix
    )
    assert "Contents" not in response
