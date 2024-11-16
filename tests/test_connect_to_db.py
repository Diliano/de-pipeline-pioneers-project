from unittest.mock import patch
from src.ingestion import connect_to_db
import pytest


# @pytest.mark.xfail
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


# @pytest.mark.xfail
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