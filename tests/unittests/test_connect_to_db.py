from unittest.mock import patch
from src.utils.utils import connect_to_db
import logging
# import pytest


# @pytest.mark.xfail
@patch("src.utils.utils.retrieve_db_credentials")
@patch("src.utils.utils.Connection")
def test_connect_to_db_success(
    mock_connection, mock_retrieve_credentials, caplog
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
@patch("src.utils.utils.retrieve_db_credentials")
@patch("src.utils.utils.Connection")
def test_connect_to_db_failure(
    mock_connection, mock_retrieve_credentials, caplog
):

    caplog.set_level(logging.ERROR)
    # Mock credentials retrieval to return valid credentials
    mock_retrieve_credentials.return_value = {
        "USER": "test-user",
        "PASSWORD": "test-password",
        "DATABASE": "test-db",
        "HOST": "test-host",
        "PORT": "5432",
    }

    mock_connection.side_effect = Exception("Connection error")

    # with pytest.raises(Exception, match="Connection error"):
    #     connect_to_db()

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
