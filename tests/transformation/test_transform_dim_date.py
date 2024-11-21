import pandas as pd

from src.transformation.transformationutil import dim_date


def test_dim_date_empty_input(caplog):
    result = dim_date()
    assert "Datasets can't be empty" in caplog.text
    assert result is None


def test_dim_date_missing_date_columns(caplog):
    dataset = pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie"],
        "value": [1, 2, 3]
    })

    result = dim_date(dataset)
    assert "Skipping dataset without date columns" in caplog.text
    assert result is None


def test_dim_date_non_dataframe_input(caplog):
    dataset = [{"created_at": "2023-01-01", "last_updated": "2023-01-02"}]

    result = dim_date(dataset)
    assert "Expected a DataFrame, but got" in caplog.text
    assert result is None


def test_dim_date_invalid_date_format(caplog):
    dataset = pd.DataFrame({
        "created_at": ["invalid_date", "2023-01-02", "2023-01-03"]
    })

    result = dim_date(dataset)
    assert "Error parsing column 'created_at'" in caplog.text 
    assert result is None


def test_dim_date_valid_input():
    dataset1 = pd.DataFrame({
        "created_at": ["2023-01-01", "2023-01-02", "2023-01-03"],
        "last_updated": ["2023-01-04", "2023-01-05", "2023-01-06"]
    })
    dataset2 = pd.DataFrame({
        "event_date": ["2023-01-07", "2023-01-08", "2023-01-09"]
    })

    result = dim_date(dataset1, dataset2)
    dates = pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03",
                            "2023-01-04", "2023-01-05", "2023-01-06",
                            "2023-01-07", "2023-01-08", "2023-01-09"])
    expected = pd.DataFrame({
        "date_id": [20230101, 20230102, 20230103,
                    20230104, 20230105, 20230106, 
                    20230107, 20230108, 20230109],
        "date": dates,
        "year": [2023] * 9,
        "month": [1] * 9,
        "day": [1, 2, 3, 4, 5, 6, 7, 8, 9],
        "day_of_week": [6, 0, 1, 2, 3, 4, 5, 6, 0],
        "day_name": ["Sunday", "Monday", "Tuesday",
                     "Wednesday", "Thursday", "Friday",
                     "Saturday", "Sunday", "Monday"],
        "month_name": ["January"] * 9,
        "quarter": [1] * 9
    })

    pd.testing.assert_frame_equal(result, expected)


def test_dim_date_duplicate_dates():
    dataset = pd.DataFrame({
        "created_at": ["2023-01-01", "2023-01-01", "2023-01-02"]
    })

    result = dim_date(dataset)
    expected = pd.DataFrame({
        "date_id": [20230101, 20230102],
        "date": pd.to_datetime(["2023-01-01", "2023-01-02"]),
        "year": [2023, 2023],
        "month": [1, 1],
        "day": [1, 2],
        "day_of_week": [6, 0],
        "day_name": ["Sunday", "Monday"],
        "month_name": ["January", "January"],
        "quarter": [1, 1]
    })

    pd.testing.assert_frame_equal(result, expected)