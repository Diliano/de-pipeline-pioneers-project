import pandas as pd
from src.transformation.transformationutil import (
    transform_dim_design
)


def test_transform_dim_design_empty_data(caplog):
    # caplog.set_level(logging.INFO)
    design_data = []
    result = transform_dim_design(design_data)
    assert "Design data is empty" in caplog.text
    assert result is None


def test_transform_dim_design_valid_data():
    design_data = [
        {
            "design_id": 1,
            "design_name": "Wooden",
            "file_location": "/usr",
            "file_name": "wooden-20220717.json",
            "created_at": "2023-01-01",
            "last_updated": "2023-01-02",
        },
        {
            "design_id": 2,
            "design_name": "Metallic",
            "file_location": "/opt",
            "file_name": "metallic-20220717.json",
            "created_at": "2023-01-03",
            "last_updated": "2023-01-04",
        },
    ]

    result = transform_dim_design(design_data)
    expected = pd.DataFrame(
        {
            "design_id": [1, 2],
            "design_name": ["Wooden", "Metallic"],
            "file_location": ["/usr", "/opt"],
            "file_name": [
                "wooden-20220717.json",
                "metallic-20220717.json"
            ],
        }
    )

    pd.testing.assert_frame_equal(result, expected)


def test_transform_dim_design_duplicates():
    design_data = [
        {
            "design_id": 1,
            "design_name": "Wooden",
            "file_location": "/usr",
            "file_name": "wooden-20220717.json",
            "created_at": "2023-01-01",
            "last_updated": "2023-01-02",
        },
        {
            "design_id": 1,
            "design_name": "Wooden",
            "file_location": "/usr",
            "file_name": "wooden-20220717.json",
            "created_at": "2023-01-01",
            "last_updated": "2023-01-02",
        },
    ]

    result = transform_dim_design(design_data)
    expected = pd.DataFrame(
        {
            "design_id": [1],
            "design_name": ["Wooden"],
            "file_location": ["/usr"],
            "file_name": ["wooden-20220717.json"],
        }
    ).drop_duplicates()
    pd.testing.assert_frame_equal(result, expected)


def test_transform_dim_design_missing_columns(caplog):
    design_data = [
        {
            "design_id": 1,
            "design_name": "Wooden",
            "file_location": "/usr",
            # "file_name": "wooden-20220717.json", # Missing file_name
            "created_at": "2023-01-01",
            "last_updated": "2023-01-02",
        }
    ]

    transform_dim_design(design_data)
    assert "Unexpected error occurred with transform_dim_design" in caplog.text
