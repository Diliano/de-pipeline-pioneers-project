from unittest.mock import patch
from src.transformation.transformationutil import load_data_from_s3_ingestion
import logging
import json


VALID_KEY = "valid-key.json"
INVALID_JSON_KEY = "invalid-json.json"
MISSING_BODY_KEY = "missing-body.json"


@patch(
    "src.transformation.transformation.S3_INGESTION_BUCKET", "test_bucket"
)
def test_load_data_from_s3_ingestion_empty_key(mock_s3_client, caplog):
    # Input: Empty String
    result = load_data_from_s3_ingestion("")
    assert result is None
    assert "Invalid s3 key. Key must be non empty string." in caplog.text


@patch(
    "src.transformation.transformation.S3_INGESTION_BUCKET", "test_bucket"
)
def test_load_data_from_s3_ingestion_non_string_key(mock_s3_client, caplog):
    # Input: Non String Key
    result = load_data_from_s3_ingestion(123)
    assert result is None
    assert "Invalid s3 key. Key must be non empty string." in caplog.text


@patch(
    "src.transformation.transformation.S3_INGESTION_BUCKET", "test_bucket"
)
def test_load_data_from_s3_ingestion_valid_key(mock_s3_client, caplog):
    # Input: Valid Key
    mock_s3_client.create_bucket(
        Bucket="test_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    valid_data = {"key1": "value1", "key2": "value2"}
    mock_s3_client.put_object(
        Bucket="test_bucket",
        Key=VALID_KEY,
        Body=json.dumps(valid_data),
    )

    data = load_data_from_s3_ingestion(VALID_KEY)

    # Asserting returned data matches
    assert data == {"key1": "value1", "key2": "value2"}
    assert f"Successfully loaded data from s3 key: {VALID_KEY}" in caplog.text


@patch(
    "src.transformation.transformation.S3_INGESTION_BUCKET", "test_bucket"
)
def test_load_data_from_s3_ingestion_invalid_json(mock_s3_client, caplog):
    # Input: Invalid JSON Data
    caplog.set_level(logging.ERROR)
    mock_s3_client.create_bucket(
        Bucket="test_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    mock_s3_client.put_object(
        Bucket="test_bucket",
        Key=INVALID_JSON_KEY,
        Body="invalid json content",
    )
    result = load_data_from_s3_ingestion(INVALID_JSON_KEY)

    # Asserting 'Validation error' in captured logs
    assert result is None
    assert "Validation error" in caplog.text


@patch(
    "src.transformation.transformation.S3_INGESTION_BUCKET", "test_bucket"
)
def test_load_data_from_s3_ingestion_missing_body(mock_s3_client, caplog):
    # Simulate missing body
    caplog.set_level(logging.ERROR)
    mock_s3_client.create_bucket(
        Bucket="test_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    mock_s3_client.put_object(
        Bucket="test_bucket",
        Key=MISSING_BODY_KEY,
    )
    result = load_data_from_s3_ingestion(MISSING_BODY_KEY)
    assert result is None
    assert "Validation error" in caplog.text


@patch(
    "src.transformation.transformation.S3_INGESTION_BUCKET", "test_bucket"
)
def test_load_data_from_s3_ingestion_no_such_key(mock_s3_client, caplog):
    # Non existent key
    invalid_key = "invalid"
    load_data_from_s3_ingestion(invalid_key)
    assert (
        f"The specified key {invalid_key} does not exist in bucket"
        in caplog.text
    )
