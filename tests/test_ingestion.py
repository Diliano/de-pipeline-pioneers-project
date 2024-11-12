from unittest.mock import Mock, patch
from moto import mock_aws
import boto3
import unittest
import pytest
import json
import logging

# 
from  src.ingestion import retrieve_db_credentials ,connect_to_db, fetch_tables

# Defining a fixture
@pytest.fixture
def mock_secrets_manager():
    with mock_aws():
        secrets_client = boto3.client("secretsmanager", region_name="eu-west-2")

        # Create a mock secret
        secrets_client.create_secret(
            Name="nc-totesys-db-credentials",
            SecretString=json.dumps({
                "HOST": "test-host",
                "USER": "test-user",
                "PASSWORD": "test-password",
                "DATABASE": "test-db",
                "PORT": "5432"
            })
        )

        yield secrets_client

def test_retrieve_db_credentials_success(mock_secrets_manager, caplog):
    result = retrieve_db_credentials(mock_secrets_manager)

    expected_secret = {
                "HOST": "test-host",
                "USER": "test-user",
                "PASSWORD": "test-password",
                "DATABASE": "test-db",
                "PORT": "5432"
    }
    assert result == expected_secret
    assert "Unexpected error occurred" not in caplog.text  # Ensuring no error was logged

def test_retrieve_db_credentials_error(caplog):
    with mock_aws():
        # Create the Secrets Manager client without creating the secret
        secrets_client = boto3.client("secretsmanager", region_name="eu-west-2")

        with pytest.raises(Exception) as excinfo:
            retrieve_db_credentials(secrets_client)

        assert "Unexpected error occurred" in caplog.text

@patch("src.ingestion.retrieve_db_credentials")
@patch("src.ingestion.Connection") 
def test_connect_to_db_success(mock_connection, mock_retrieve_credentials, caplog):
    # Mock the credentials returned by retrieve_db_credentials
    mock_retrieve_credentials.return_value = {
        "USER": "test-user",
        "PASSWORD": "test-password",
        "DATABASE": "test-db",
        "HOST": "test-host",
        "PORT": "5432"
    }

    connect_to_db()

    mock_retrieve_credentials.assert_called_once()  # Ensuring credentials were retrieved once
    mock_connection.assert_called_once_with(
        user="test-user",
        database="test-db",
        password="test-password",
        host="test-host",
        port="5432"
    )
    assert "Database connection failed" not in caplog.text  # Ensuring no error was logged


@patch("src.ingestion.retrieve_db_credentials")
@patch("src.ingestion.Connection") 
def test_connect_to_db_failure(mock_connection, mock_retrieve_credentials, caplog):
    # Mock credentials retrieval to return valid credentials
    mock_retrieve_credentials.return_value = {
        "USER": "test-user",
        "PASSWORD": "test-password",
        "DATABASE": "test-db",
        "HOST": "test-host",
        "PORT": "5432"
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
        port="5432"
    )

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
        "counterparty", "currency", "department", "design",
        "staff", "sales_order", "address", "payment",
        "purchase_order", "payment_type", "transaction"
    ]:
        assert result[table_name] == [sample_data]
        assert f"Fetched data from {table_name} successfully." in caplog.text

    mock_db.run.assert_called()  # Ensuring that run was called for each table
    assert "Failed to fetch data" not in caplog.text  # Ensuring no errors were logged

# @pytest.mark.skip
@patch("src.ingestion.connect_to_db")
def test_fetch_tables_table_query_failure(mock_connect_to_db, caplog):
    caplog.set_level(logging.INFO)

    mock_db = Mock()
    mock_connect_to_db.return_value.__enter__.return_value = mock_db

    # Define a side effect function that raises an exception only for the "staff" table
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

# @pytest.mark.skip
@patch("src.ingestion.connect_to_db")
def test_fetch_tables_connection_failure(mock_connect_to_db, caplog):
    caplog.set_level(logging.INFO)

    # Simulate an exception when attempting to connect to the database
    mock_connect_to_db.side_effect = Exception("Connection error")

    with pytest.raises(Exception, match="Connection error"):
        fetch_tables()

    # Check that an error was logged for the connection failure
    assert "Database connection failed" in caplog.text