from src.transformation.transformationutil import transform_fact_sales_order
import pandas as pd
import pytest


def test_transform_fact_sales_order_invalid_type(caplog):
    """Test transform_fact_sales_order with invalid input type."""
    invalid_data = "invalid input"
    transform_fact_sales_order(invalid_data)

    assert "Validation error:" in caplog.text


def test_transform_fact_sales_order_empty_data(caplog):
    """Test transform_fact_sales_order with empty input."""
    result = transform_fact_sales_order([])

    # Ensure the function logs the Validation error
    assert result is None
    assert "Validation error:" in caplog.text


@pytest.mark.skip("Not necessary to check for data integrity as of now")
def test_transform_fact_sales_order_invalid_data(invalid_sales_order_data):
    """Test transform_fact_sales_order with invalid data."""
    result = transform_fact_sales_order(invalid_sales_order_data)

    # Ensure the function doesn't crash and returns a DataFrame with NaNs
    assert pd.isna(result["created_date"].iloc[0])
    assert pd.isna(result["created_time"].iloc[0])
    assert pd.isna(result["sales_staff_id"].iloc[0])


def test_transform_fact_sales_order_valid_data(valid_sales_order_data):
    """Test transform_fact_sales_order with valid data."""
    result = transform_fact_sales_order(valid_sales_order_data)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert set(result.columns) == {
        "sales_order_id",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time",
        "sales_staff_id",
        "counterparty_id",
        "units_sold",
        "unit_price",
        "currency_id",
        "design_id",
        "agreed_payment_date",
        "agreed_delivery_date",
        "agreed_delivery_location_id",
    }
    assert result["sales_order_id"].iloc[0] == 1
    assert result["created_date"].iloc[0] == pd.Timestamp("2023-01-01").date()
    assert (
        result["created_time"].iloc[0]
        == pd.Timestamp("2023-01-01 12:34:56").time()
    )
