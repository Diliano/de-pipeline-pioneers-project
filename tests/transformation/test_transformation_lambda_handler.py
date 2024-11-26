from src.transformation.transformation import lambda_handler
import pandas as pd

# from unittest.mock import


VALID_KEY_SALES_ORDER = "ingestion/sales_order/sample_data.json"
VALID_KEY_STAFF = "ingestion/staff/sample_data.json"
INVALID_KEY = "invalid/sample_data.json"


def test_lambda_handler_valid_sales_order(
    mock_s3_client, mock_s3_event, mocker
):
    """Test Lambda with a valid sales_order key."""
    event = mock_s3_event(VALID_KEY_SALES_ORDER)

    # Mock dependencies
    mocker.patch(
        "src.transformation.transformationutil.load_data_from_s3_ingestion",
        return_value=[
            {
                "sales_order_id": 1,
                "created_at": "2023-01-01",
                "last_updated": "2023-01-02",
            }
        ],
    )
    mocker.patch(
        "src.transformation.transformationutil.dim_date",
        return_value=pd.DataFrame(
            {
                "date_id": [20230101],
                "date": ["2023-01-01"],
                "year": [2023],
                "month": [1],
                "day": [1],
            }
        ),
    )
    mocker.patch("src.transformation.transformationutil.save_transformed_data")
    mocker.patch(
        "src.transformation.transformationutil.process_table",
        return_value=pd.DataFrame(),
    )

    response = lambda_handler(event, None)
    assert response["statusCode"] == 200


def test_lambda_handler_valid_staff(mock_s3_client, mock_s3_event, mocker):
    """Test Lambda with a valid staff key."""
    event = mock_s3_event(VALID_KEY_STAFF)

    # Mock dependencies
    mocker.patch(
        "src.transformation.transformationutil.load_data_from_s3_ingestion",
        return_value=[
            {"staff_id": 1, "first_name": "John", "last_name": "Doe"}
        ],
    )
    mocker.patch("src.transformation.transformationutil.save_transformed_data")
    mocker.patch(
        "src.transformation.transformationutil.process_table",
        return_value=pd.DataFrame(),
    )

    response = lambda_handler(event, None)
    assert response["statusCode"] == 200


def test_lambda_handler_invalid_key(mock_s3_client, mock_s3_event, mocker):
    """Test Lambda with an invalid key."""
    event = mock_s3_event(INVALID_KEY)

    # Mock dependencies
    mocker.patch(
        "src.transformation.transformationutil.load_data_from_s3_ingestion",
        side_effect=Exception("Invalid Key"),
    )
    mocker.patch("src.transformation.transformationutil.save_transformed_data")

    response = lambda_handler(event, None)
    # Lambda should continue, even with an invalid key
    assert response["statusCode"] == 200


def test_lambda_handler_missing_transformation_logic(mock_s3_event, mocker):
    """Test Lambda with a key that has no transformation logic."""
    event = mock_s3_event("ingestion/unknown_table/sample_data.json")

    # Mock dependencies
    mocker.patch(
        "src.transformation.transformationutil.load_data_from_s3_ingestion"
    )
    mocker.patch("src.transformation.transformationutil.save_transformed_data")

    response = lambda_handler(event, None)
    assert response["statusCode"] == 200


def test_lambda_handler_critical_error(mock_s3_event, mocker, caplog):
    """Test Lambda with a critical error."""
    event = mock_s3_event(VALID_KEY_SALES_ORDER)

    # Mock an unhandled exception
    mocker.patch(
        "src.transformation.transformationutil.load_data_from_s3_ingestion",
        side_effect=Exception("Critical Error"),
    )

    response = lambda_handler(event, None)  # noqa: F841
    assert f"Error parsing record {VALID_KEY_SALES_ORDER}" in caplog.text
    # assert response["statusCode"] == 500 # ?
