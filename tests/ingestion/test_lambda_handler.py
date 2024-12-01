from unittest.mock import patch
from datetime import datetime
from src.ingestion.ingestion import (
    lambda_handler,
)
# import pytest


# @pytest.mark.xfail
@patch("src.ingestion.utils.fetch_tables")
@patch("src.ingestion.ingestion.s3_client.put_object")
@patch("src.ingestion.ingestion.S3_INGESTION_BUCKET", "test_bucket")
@patch("src.ingestion.ingestion.datetime")
def test_lambda_handler_success(
    mock_datetime,
    mock_put_object,
    mock_fetch_tables,
    caplog,
    sample_table_data,
):
    mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)
    timestamp = "2023-01-01T12:00:00Z"

    mock_fetch_tables.return_value = sample_table_data

    # Call the lambda_handler function
    result = lambda_handler({}, {})

    assert result == {
        "status": "Success",
        "message": "All data ingested successfully",
    }

    # Verifying S3 uploads
    mock_put_object.assert_any_call(
        Bucket="test_bucket",
        Key=f"ingestion/table1/2023/01/01/table1_{timestamp}.json",
        Body='[{"id": 1, "value": "test1"}]',
    )
    mock_put_object.assert_any_call(
        Bucket="test_bucket",
        Key=f"ingestion/table2/2023/01/01/table2_{timestamp}.json",
        Body='[{"id": 2, "value": "test1"}]',
    )

    assert "Successfully wrote table1 data to S3 key" in caplog.text
    assert "Successfully wrote table2 data to S3 key" in caplog.text


# @pytest.mark.xfail
@patch("src.ingestion.utils.fetch_tables")
@patch("src.ingestion.ingestion.s3_client.put_object")
@patch("src.ingestion.ingestion.datetime")
def test_lambda_handler_partial_failure(
    mock_datetime,
    mock_put_object,
    mock_fetch_tables,
    caplog,
    sample_table_data,
):
    # Mock the current timestamp
    mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)
    # timestamp = "2023-01-01T12:00:00Z"

    # Mocking fetch_tables to return data for all tables
    mock_fetch_tables.return_value = sample_table_data

    # Simulating S3 upload failure for table1
    mock_put_object.side_effect = [
        Exception("S3 upload failed for table1"),
        None,  # Success for table2
    ]

    result = lambda_handler({}, {})

    assert result == {
        "status": "Partial Failure",
        "message": "Some tables failed to ingest",
        "failed_tables": ["table1"],
    }

    assert "Failed to write table1 data to S3" in caplog.text
    assert "Successfully wrote table2 data to S3 key" in caplog.text


# @pytest.mark.xfail
@patch("src.ingestion.utils.fetch_tables")
@patch("src.ingestion.ingestion.s3_client.put_object")
@patch("src.ingestion.ingestion.S3_INGESTION_BUCKET", "test_bucket")
@patch("src.ingestion.ingestion.datetime")
def test_lambda_handler_empty_table(
    mock_datetime, mock_put_object, mock_fetch_tables, caplog
):
    mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)
    timestamp = "2023-01-01T12:00:00Z"

    # Mocking fetch_tables to return an empty table1
    # and valid data for table2
    mock_fetch_tables.return_value = {
        "table1": [],
        "table2": [{"id": 2, "value": "data2"}],
    }

    result = lambda_handler({}, {})

    assert result == {
        "status": "Success",
        "message": "All data ingested successfully",
    }

    assert "Table table1 has not been updated" in caplog.text

    # Verifying S3 upload for table2 only
    mock_put_object.assert_called_once_with(
        Bucket="test_bucket",
        Key=f"ingestion/table2/2023/01/01/table2_{timestamp}.json",
        Body='[{"id": 2, "value": "data2"}]',
    )
