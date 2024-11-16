from unittest.mock import patch
from datetime import datetime
from src.ingestion import (
    lambda_handler,
)
import logging
import json

# Samples data
SAMPLE_TABLES_DATA = {
    "table1": [{"id": 1, "value": "test1"}],
    "table2": [{"id": 2, "value": "test2"}],
}


# @pytest.mark.xfail
@patch(
    "src.ingestion.S3_INGESTION_BUCKET", "test_bucket"
)  # Mock S3_INGESTION_BUCKET directly in the module
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
        "message": "All data ingested successfully",
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


# @pytest.mark.xfail
@patch("src.ingestion.S3_INGESTION_BUCKET", "test_bucket")
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
    assertion_key = "ingestion/table2/table2_2023-01-01-00-00-00.json"
    assert (
        f"Successfully wrote table2 data to S3 key: {assertion_key}"
        in caplog.text
    )