from unittest.mock import Mock, patch
from moto import mock_aws
from datetime import (
    datetime,
    timedelta,
)
from unittest.mock import MagicMock
import boto3
import pytest
import json
import logging

#
from src.ingestion import (
    retrieve_db_credentials,
    connect_to_db,
    fetch_tables,
    lambda_handler,
    get_last_ingestion_timestamp,
    update_last_ingestion_timestamp,
    TIMESTAMP_FILE_KEY,
    BUCKET_NAME
)

# Sample data
SAMPLE_TABLES_DATA = {
    "table1": [{"id": 1, "value": "test1"}],
    "table2": [{"id": 2, "value": "test2"}],
}


# Defining a fixture
@pytest.fixture
def mock_secrets_manager():
    with mock_aws():
        secrets_client = boto3.client(
            "secretsmanager",
            region_name="eu-west-2"
            )

        # Create a mock secret
        secrets_client.create_secret(
            Name="nc-totesys-db-credentials",
            SecretString=json.dumps(
                {
                    "HOST": "test-host",
                    "USER": "test-user",
                    "PASSWORD": "test-password",
                    "DATABASE": "test-db",
                    "PORT": "5432",
                }
            ),
        )

        yield secrets_client


def test_retrieve_db_credentials_success(mock_secrets_manager, caplog):
    result = retrieve_db_credentials(mock_secrets_manager)

    expected_secret = {
        "HOST": "test-host",
        "USER": "test-user",
        "PASSWORD": "test-password",
        "DATABASE": "test-db",
        "PORT": "5432",
    }
    assert result == expected_secret
    assert (
        "Unexpected error occurred" not in caplog.text
    )  # Ensuring no error was logged


def test_retrieve_db_credentials_error(caplog):
    with mock_aws():
        # Create the Secrets Manager client without creating the secret
        secrets_client = boto3.client(
            "secretsmanager",
            region_name="eu-west-2"
            )

        with pytest.raises(Exception):
            retrieve_db_credentials(secrets_client)

        assert "Unexpected error occurred" in caplog.text


@patch("src.ingestion.retrieve_db_credentials")
@patch("src.ingestion.Connection")
def test_connect_to_db_success(
    mock_connection,
    mock_retrieve_credentials,
    caplog
):
    # Mock the credentials returned by retrieve_db_credentials
    mock_retrieve_credentials.return_value = {
        "USER": "test-user",
        "PASSWORD": "test-password",
        "DATABASE": "test-db",
        "HOST": "test-host",
        "PORT": "5432",
    }

    connect_to_db()

    # Ensuring credentials were retrieved once
    mock_retrieve_credentials.assert_called_once()
    mock_connection.assert_called_once_with(
        user="test-user",
        database="test-db",
        password="test-password",
        host="test-host",
        port="5432",
    )
    assert (
        "Database connection failed" not in caplog.text
    )  # Ensuring no error was logged


@patch("src.ingestion.retrieve_db_credentials")
@patch("src.ingestion.Connection")
def test_connect_to_db_failure(
    mock_connection,
    mock_retrieve_credentials,
    caplog
):
    # Mock credentials retrieval to return valid credentials
    mock_retrieve_credentials.return_value = {
        "USER": "test-user",
        "PASSWORD": "test-password",
        "DATABASE": "test-db",
        "HOST": "test-host",
        "PORT": "5432",
    }

    mock_connection.side_effect = Exception("Connection error")

    with pytest.raises(Exception, match="Connection error"):
        connect_to_db()

    # Check that an error was logged
    assert "Database connection failed" in caplog.text
    mock_retrieve_credentials.assert_called_once()
    mock_connection.assert_called_once_with(
        user="test-user",
        database="test-db",
        password="test-password",
        host="test-host",
        port="5432",
    )

# @pytest.mark.skip
@patch("src.ingestion.connect_to_db")
def test_fetch_tables_success(mock_connect_to_db, caplog):
    mock_db = Mock()
    mock_connect_to_db.return_value.__enter__.return_value = mock_db

    caplog.set_level(logging.INFO)

    # Define the data each table query should return
    sample_data = {"id": 1, "name": "Sample"}
    mock_db.run.return_value = [sample_data]

    result = fetch_tables()

    # Check that each table name has the sample data as its value in the result
    for table_name in [
        "counterparty",
        "currency",
        "department",
        "design",
        "staff",
        "sales_order",
        "address",
        "payment",
        "purchase_order",
        "payment_type",
        "transaction",
    ]:
        assert result[table_name] == [sample_data]
        assert f"Fetched new data from {table_name} successfully." in caplog.text

    # Ensuring that run was called for each table
    mock_db.run.assert_called()
    # Ensuring no errors were logged
    assert "Failed to fetch data" not in caplog.text


@pytest.mark.skip
@patch("src.ingestion.connect_to_db")
def test_fetch_tables_table_query_failure(mock_connect_to_db, caplog):
    caplog.set_level(logging.INFO)

    mock_db = Mock()
    mock_connect_to_db.return_value.__enter__.return_value = mock_db

    # Define a side effect function that
    # raises an exception only for the "staff" table
    def side_effect(query):
        if "staff" in query:
            raise Exception("Query failed")
        return [{"id": 1, "name": "Sample"}]

    # Apply the side effect to the mock database's run method
    mock_db.run.side_effect = side_effect

    with pytest.raises(Exception, match="Query failed"):
        fetch_tables()

    # Check that an error was logged for the "staff" table
    assert "Failed to fetch data from staff" in caplog.text
    mock_db.run.assert_called()  # Ensuring that run was called at least once


@pytest.mark.skip
@patch("src.ingestion.connect_to_db")
def test_fetch_tables_connection_failure(mock_connect_to_db, caplog):
    caplog.set_level(logging.INFO)

    # Simulate an exception when attempting to connect to the database
    mock_connect_to_db.side_effect = Exception("Connection error")

    with pytest.raises(Exception, match="Connection error"):
        fetch_tables()

    # Check that an error was logged for the connection failure
    assert "Database connection failed" in caplog.text


@patch(
    "src.ingestion.BUCKET_NAME", "test_bucket"
)  # Mock BUCKET_NAME directly in the module
@patch("src.ingestion.fetch_tables")
@patch("src.ingestion.s3_client")
@patch("src.ingestion.datetime")
def test_lambda_handler_success(
    mock_datetime, mock_s3_client, mock_fetch_tables, caplog
):
    caplog.set_level(logging.INFO)

    mock_fetch_tables.return_value = SAMPLE_TABLES_DATA
    mock_datetime.now.return_value = datetime(
        2023, 1, 1, 0, 0, 0
    )  # Fixed timestamp for testing
    timestamp = "2023-01-01-00-00-00"

    result = lambda_handler({}, {})

    for table_name in SAMPLE_TABLES_DATA.keys():
        expected_key = f"{table_name}/{table_name}_{timestamp}.json"
        mock_s3_client.put_object.assert_any_call(
            Bucket="test_bucket",
            Key=expected_key,
            Body=json.dumps(SAMPLE_TABLES_DATA[table_name]),
        )
    assert result == {
        "status": "Success",
        "message": "All data ingested successfully"
        }
    assertion_key = "table1/table1_2023-01-01-00-00-00.json"
    assert (
        f"Successfully wrote table1 data to S3 key: {assertion_key}"
        in caplog.text
    )
    assertion_key = "table2/table2_2023-01-01-00-00-00.json"
    assert (
        f"Successfully wrote table2 data to S3 key: {assertion_key}"
        in caplog.text
    )


@patch("src.ingestion.BUCKET_NAME", "test_bucket")
@patch("src.ingestion.fetch_tables")
@patch("src.ingestion.s3_client")
@patch("src.ingestion.datetime")
def test_lambda_handler_partial_failure(
    mock_datetime, mock_s3_client, mock_fetch_tables, caplog
):
    caplog.set_level(logging.INFO)

    mock_fetch_tables.return_value = SAMPLE_TABLES_DATA
    mock_datetime.now.return_value = datetime(2023, 1, 1, 0, 0, 0)

    def put_object_side_effect(Bucket, Key, Body):
        if "table1" in Key:
            raise Exception("S3 upload error for table1")
        return {}

    mock_s3_client.put_object.side_effect = put_object_side_effect

    result = lambda_handler({}, {})

    # Check the return value indicates partial failure
    assert result == {
        "status": "Partial Failure",
        "message": "Some tables failed to ingest",
    }

    # Ensuring that an error was logged for the table that failed
    assert "Failed to write data to S3" in caplog.text
    assertion_key = "table2/table2_2023-01-01-00-00-00.json"
    assert (
        f"Successfully wrote table2 data to S3 key: {assertion_key}"
        in caplog.text
    )

@mock_aws
def test_get_last_ingestion_timestamp_valid_timestamp():

    s3_client = boto3.client("s3", region_name="eu-west-2")
    s3_client.create_bucket(
        Bucket=BUCKET_NAME, 
        CreateBucketConfiguration={
            "LocationConstraint": "eu-west-2"
            })
    valid_timestamp = datetime(2023, 1, 1, 12, 0, 0).isoformat()
    
    # Put a valid timestamp in the S3 object
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=TIMESTAMP_FILE_KEY,
        Body=json.dumps({"timestamp": valid_timestamp})
    )

    with patch("src.ingestion.s3_client", s3_client):
        result = get_last_ingestion_timestamp()

        assert result == datetime.fromisoformat(valid_timestamp)

# @pytest.mark.skip
@mock_aws
def test_get_last_ingestion_timestamp_no_file():
    BUCKET_NAME = 'test_bucket'

    # Initialize the mock S3 environment
    s3_client = boto3.client("s3", region_name="eu-west-2")
    
    s3_client.create_bucket(
        Bucket=BUCKET_NAME,
        CreateBucketConfiguration={
            "LocationConstraint": "eu-west-2"
        }
    )
    
    with patch("src.ingestion.s3_client", s3_client):
        result = get_last_ingestion_timestamp()

        expected_default_timestamp = datetime.now() - timedelta(days=1)
        # Check if result is within a reasonable delta of the expected default timestamp
        assert abs((result - expected_default_timestamp).total_seconds()) < 5

@mock_aws
def test_get_last_ingestion_timestamp_missing_timestamp_key():
    BUCKET_NAME = "test_bucket"
    s3_client = boto3.client('s3', region_name='us-east-1')
    s3_client.create_bucket(
        Bucket=BUCKET_NAME,
        CreateBucketConfiguration={
            "LocationConstraint": "eu-west-2"
            }
        )

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=TIMESTAMP_FILE_KEY,
        Body=json.dumps({})
    )

    result = get_last_ingestion_timestamp()

    expected_default_timestamp = datetime.now() - timedelta(days=1)
    assert abs((result - expected_default_timestamp).total_seconds()) < 5

@mock_aws
def test_get_last_ingestion_timestamp_unexpected_error(caplog):
    s3_client = MagicMock()
    s3_client.get_object.side_effect = Exception("Unexpected error")

    result = get_last_ingestion_timestamp()

    expected_default_timestamp = datetime.now() - timedelta(days=1)
    assert abs((result - expected_default_timestamp).total_seconds()) < 5
    assert "Unexpected error occurred:" in caplog.text