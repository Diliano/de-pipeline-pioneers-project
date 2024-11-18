from unittest.mock import MagicMock, patch
from src.ingestion import (
    fetch_tables,
)
import logging
# import pytest


@patch("src.ingestion.get_last_ingestion_timestamp")
@patch("src.ingestion.update_last_ingestion_timestamp")
@patch("src.ingestion.connect_to_db")
def test_fetch_tables_success(
    mock_connect_to_db,
    mock_update_timestamp,
    mock_get_timestamp,
    expected_table_data,
    mock_columns,
    mock_tables,
    mock_rows,
):
    # Mocking the last ingestion timestamp
    mock_get_timestamp.return_value = "2023-01-01 00:00:00"

    # Mocking the database connection and query
    mock_db = MagicMock()
    mock_db.run.return_value = mock_rows
    mock_db.columns = mock_columns
    mock_connect_to_db.return_value.__enter__.return_value = mock_db

    result = fetch_tables(mock_tables)

    assert result == expected_table_data

    expected_query = "SELECT * FROM table1 WHERE last_updated > :s;"
    mock_db.run.assert_any_call(expected_query, s="2023-01-01 00:00:00")

    expected_query = "SELECT * FROM table2 WHERE last_updated > :s;"
    mock_db.run.assert_any_call(expected_query, s="2023-01-01 00:00:00")

    mock_update_timestamp.assert_called_once()


@patch("src.ingestion.get_last_ingestion_timestamp")
@patch("src.ingestion.update_last_ingestion_timestamp")
@patch("src.ingestion.connect_to_db")
def test_fetch_tables_query_logging(
    mock_connect_to_db,
    mock_update_timestamp,
    mock_get_timestamp,
    mock_columns,
    mock_tables,
    caplog,
):
    caplog.set_level(logging.ERROR)
    mock_get_timestamp.return_value = "2023-01-01 00:00:00"

    mock_db = MagicMock()
    mock_connect_to_db.return_value.__enter__.return_value = mock_db

    # Simulating a query failure for the first table
    mock_db.columns = mock_columns
    mock_db.run.side_effect = [
        Exception("Query failed for table1"),
        [[1, "mock_data"]],
    ]

    result = fetch_tables(mock_tables)

    assert result == {"table2": [{"id": 1, "data": "mock_data"}]}

    assert "Failed to fetch data from table1" in caplog.text

    # Ensuring the query was attempted for both tables
    expected_query_table1 = "SELECT * FROM table1 WHERE last_updated > :s;"
    mock_db.run.assert_any_call(expected_query_table1, s="2023-01-01 00:00:00")

    expected_query_table2 = "SELECT * FROM table2 WHERE last_updated > :s;"
    mock_db.run.assert_any_call(expected_query_table2, s="2023-01-01 00:00:00")

    mock_update_timestamp.assert_called_once()


# @pytest.mark.xfail
@patch("src.ingestion.S3_INGESTION_BUCKET", "test_bucket")
@patch("src.ingestion.connect_to_db")
def test_fetch_tables_connection_failure(mock_connect_to_db, caplog):
    """ """
    caplog.set_level(logging.INFO)

    # Simulate an exception when attempting to connect to the database
    mock_connect_to_db.side_effect = Exception("Connection error")

    # with pytest.raises(Exception, match="Connection error"):
    #     fetch_tables()
    fetch_tables()

    # Check that an error was logged for the connection failure
    assert "Database connection failed" in caplog.text
