from unittest.mock import Mock, patch
import logging
from src.ingestion import (
    fetch_tables,
    TABLES
)
import pytest


# @pytest.mark.skip
@patch("src.ingestion.update_last_ingestion_timestamp")
@patch("src.ingestion.get_last_ingestion_timestamp")
@patch("src.ingestion.connect_to_db")
def test_fetch_tables_success(
    mock_connect_to_db, mock_last_timestamp, mock_update_timestamp, caplog
):
    caplog.set_level(logging.INFO)

    mock_db = Mock()
    mock_connect_to_db.return_value.__enter__.return_value = mock_db

    # Define the data each table query should return
    mock_last_timestamp.return_value = "2024-11-13 15:48:34.623971"
    mock_db.run.return_value = [{1, "Sample data"}]
    mock_db.columns = [{"name": "id"}, {"name": "data"}]

    print("Mock db.run.return_value:", mock_db.run.return_value)
    print("Mock db.columns:", mock_db.columns)

    result = fetch_tables()

    print("fetch tables result:", result)
    expected_data = {
        table_name: [{"id": 1, "data": "Sample data"}] for table_name in TABLES
    }

    print("Expected data ", expected_data)
    assert result == expected_data

    for table_name in TABLES:
        assert (
            f"Fetched new data from {table_name} successfully." in caplog.text
        )
    mock_update_timestamp.assert_called_once()


# @pytest.mark.skip
@patch("src.ingestion.update_last_ingestion_timestamp")
@patch("src.ingestion.get_last_ingestion_timestamp")
@patch("src.ingestion.connect_to_db")
def test_fetch_tables_table_query_failure(
    mock_connect_to_db, mock_last_timestamp, mock_update_timestamp, caplog
):
    caplog.set_level(logging.ERROR)

    mock_db = Mock()
    mock_connect_to_db.return_value.__enter__.return_value = mock_db
    mock_last_timestamp.return_value = "2024-11-13 15:48:34.623971"

    # Define a side effect for mock_db.run
    # to raise an error for one specific table
    def side_effect(query, s):
        if "staff" in query:
            raise Exception("Query failed for staff table")

        # Successful query result for other tables
        return [[1, "Sample data"]]

    mock_db.run.side_effect = side_effect
    mock_db.columns = [{"name": "id"}, {"name": "data"}]

    with pytest.raises(Exception, match="Query failed for staff table"):
        fetch_tables()

    # Assert that the error was logged for the failing table
    # and update_last_ingestion_timestamp was not called
    assert "Failed to fetch data from staff" in caplog.text
    mock_update_timestamp.assert_not_called()


# @pytest.mark.xfail
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