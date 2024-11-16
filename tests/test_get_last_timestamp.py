from unittest.mock import patch
from moto import mock_aws
from datetime import datetime
from src.ingestion import (
    get_last_ingestion_timestamp,
    S3_INGESTION_BUCKET,
    TIMESTAMP_FILE_KEY
)
import logging
import pytest
import json


# @pytest.mark.xfail
@mock_aws
def test_get_last_ingestion_timestamp_valid_timestamp(mock_s3_client):

    # s3_client = boto3.client("s3", region_name="eu-west-2")
    mock_s3_client.create_bucket(
        Bucket=S3_INGESTION_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    valid_timestamp = datetime(2023, 1, 1, 12, 0, 0).isoformat()

    # Put a valid timestamp in the S3 object
    mock_s3_client.put_object(
        Bucket=S3_INGESTION_BUCKET,
        Key=TIMESTAMP_FILE_KEY,
        Body=json.dumps({"timestamp": valid_timestamp}),
    )

    with patch("src.ingestion.s3_client", mock_s3_client):
        result = get_last_ingestion_timestamp()

        assert result == datetime.fromisoformat(valid_timestamp)


# @pytest.mark.xfail
@mock_aws
def test_get_last_ingestion_timestamp_no_file(mock_s3_client):

    # Initialize the mock S3 environment
    # s3_client = boto3.client("s3", region_name="eu-west-2")

    mock_s3_client.create_bucket(
        Bucket=S3_INGESTION_BUCKET,
        CreateBucketConfiguration={
            "LocationConstraint": "eu-west-2"
            },)

    with patch("src.ingestion.s3_client", mock_s3_client):
        result = get_last_ingestion_timestamp()

        expected_default_timestamp = "1970-01-01 00:00:00"
        assert result == expected_default_timestamp


# @pytest.mark.xfail
@mock_aws
def test_get_last_ingestion_timestamp_missing_timestamp_key(mock_s3_client):
    # s3_client = boto3.client("s3", region_name="eu-west-2")
    mock_s3_client.create_bucket(
        Bucket=S3_INGESTION_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    mock_s3_client.put_object(
        Bucket=S3_INGESTION_BUCKET, Key=TIMESTAMP_FILE_KEY, Body=json.dumps({})
    )

    with patch("src.ingestion.s3_client", mock_s3_client):
        result = get_last_ingestion_timestamp()
        assert result == "1970-01-01 00:00:00"


# @pytest.mark.xfail
@mock_aws
def test_get_last_ingestion_timestamp_unexpected_error(
    mock_s3_client,
    caplog
):
    caplog.set_level(logging.INFO)

    # s3_client = boto3.client("s3", region_name="eu-west-2")
    mock_s3_client.create_bucket(
        Bucket=S3_INGESTION_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    with patch(
        "src.ingestion.s3_client.get_object",
        side_effect=Exception("Unexpected error"),
    ), patch("src.ingestion.logger") as mock_logger:

        with pytest.raises(Exception, match="Unexpected error"):
            get_last_ingestion_timestamp()

        # Confirming that the error was logged
        mock_logger.error.assert_called_once_with(
            "Unexpected error occurred: Unexpected error"
        )