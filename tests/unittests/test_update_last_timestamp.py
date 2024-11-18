from unittest.mock import patch
from datetime import datetime
from src.ingestion import update_last_ingestion_timestamp, TIMESTAMP_FILE_KEY
import json


# @pytest.mark.xfail
@patch("src.ingestion.S3_INGESTION_BUCKET", "test_bucket")
@patch("src.ingestion.s3_client")
def test_update_last_ingestion_timestamp(mock_s3_client):
    current_timestamp = datetime.now().isoformat()

    with patch("src.ingestion.datetime") as mock_datetime:
        mock_datetime.now.return_value = datetime.fromisoformat(
            current_timestamp
        )
        update_last_ingestion_timestamp()

    mock_s3_client.put_object.assert_called_once_with(
        Bucket="test_bucket",
        Key=TIMESTAMP_FILE_KEY,
        Body=json.dumps({"timestamp": current_timestamp}),
    )
