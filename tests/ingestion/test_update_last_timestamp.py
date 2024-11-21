from unittest.mock import patch
from datetime import datetime
from src.ingestion.ingestion import TIMESTAMP_FILE_KEY
import json

from src.ingestion.utils import (
    update_last_ingestion_timestamp,
)


# @pytest.mark.xfail
@patch("src.ingestion.utils.S3_INGESTION_BUCKET", "test_bucket")
@patch("src.ingestion.ingestion.s3_client.put_object")
def test_update_last_ingestion_timestamp(mock_put_object):
    current_timestamp = datetime.now().isoformat()

    with patch("src.ingestion.utils.datetime") as mock_datetime:
        mock_datetime.now.return_value = datetime.fromisoformat(
            current_timestamp
        )
        update_last_ingestion_timestamp()

    mock_put_object.assert_called_once_with(
        Bucket="test_bucket",
        Key=TIMESTAMP_FILE_KEY,
        Body=json.dumps({"timestamp": current_timestamp}),
    )
