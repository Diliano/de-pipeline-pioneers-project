from unittest.mock import Mock, patch
from moto import mock_aws
import boto3
import unittest
import pytest
import json

# 
from  src.ingestion import retrieve_db_credentials ,connect_to_db

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