from src.transformation.transformationutil import transform_dim_transaction
from unittest.mock import patch
import pandas as pd


@patch("src.transformation.transformation.logger")
def test_transform_dim_transaction_valid_list(mock_logger):
    # Input: List of dictionaries
    transaction_data = [
        {
            "transaction_id": 1,
            "transaction_type": "PURCHASE",
            "sales_order_id": None,
            "purchase_order_id": 2,
            "created_at": "2022-11-03 14:20:52.186000",
            "last_updated": "2022-11-03 14:20:52.186000",
        },
        {
            "transaction_id": 2,
            "transaction_type": "PURCHASE",
            "sales_order_id": None,
            "purchase_order_id": 3,
            "created_at": "2022-11-03 14:20:52.187000",
            "last_updated": "2022-11-03 14:20:52.187000",
        },
    ]

    expected_output = pd.DataFrame(
        {
            "transaction_id": [1, 2],
            "transaction_type": ["PURCHASE", "PURCHASE"],
            "sales_order_id": [None, None],
            "purchase_order_id": [2, 3],
        }
    )

    result = transform_dim_transaction(transaction_data)

    # Asserting the result matches the expected DataFrame
    pd.testing.assert_frame_equal(result, expected_output)
    mock_logger.error.assert_not_called()


@patch("src.transformation.transformation.logger")
def test_transform_dim_transaction_empty_list(mock_logger):
    # Input: Empty list
    transaction_data = []

    result = transform_dim_transaction(transaction_data)

    # Asserting the function returns None
    assert result is None
    mock_logger.error.assert_called_once_with(
        "Validation error: transaction_data must be populated"
    )


@patch("src.transformation.transformation.logger")
def test_transform_dim_transaction_invalid_input_type(mock_logger):
    # Input: Invalid type (string)
    transaction_data = "invalid_data"

    result = transform_dim_transaction(transaction_data)

    # Asserting the function returns None :D
    assert result is None
    mock_logger.error.assert_called_once_with(
        "Validation error: Input must be a list of dictionaries."
    )


@patch("src.transformation.transformation.logger")
def test_transform_dim_transaction_duplicates(mock_logger):
    # Input: List with duplicate rows
    transaction_data = [
        {
            "transaction_id": 1,
            "transaction_type": "PURCHASE",
            "sales_order_id": None,
            "purchase_order_id": 2,
            "created_at": "2022-11-03 14:20:52.186000",
            "last_updated": "2022-11-03 14:20:52.186000",
        },
        {
            "transaction_id": 1,
            "transaction_type": "PURCHASE",
            "sales_order_id": None,
            "purchase_order_id": 2,
            "created_at": "2022-11-03 14:20:52.186000",
            "last_updated": "2022-11-03 14:20:52.186000",
        },
    ]

    expected_output = pd.DataFrame(
        {
            "transaction_id": [1],
            "transaction_type": ["PURCHASE"],
            "sales_order_id": [None],
            "purchase_order_id": [2],
        }
    )

    result = transform_dim_transaction(transaction_data)

    # Asserting the result matches the expected DataFrame :)
    pd.testing.assert_frame_equal(result, expected_output)
    mock_logger.error.assert_not_called()
