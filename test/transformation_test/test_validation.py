import pandas as pd
import pytest

from src.transformation.validation import (
    validate_schema,
    validate_not_null,
    validate_row_count,
    validate_row_drop,
    TransformValidationError,
)

def test_validate_schema_passes_when_schema_is_correct():
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "name": ["A", "B"],
        }
    )

    expected_columns = ["id", "name"]

    # не має впасти
    validate_schema(df, expected_columns, "test_table")

def test_validate_schema_fails_when_column_missing():
    df = pd.DataFrame(
        {
            "id": [1, 2],
        }
    )

    expected_columns = ["id", "name"]

    with pytest.raises(TransformValidationError):
        validate_schema(df, expected_columns, "test_table")

def test_validate_schema_fails_when_extra_column_present():
    df = pd.DataFrame(
        {
            "id": [1],
            "name": ["A"],
            "extra": ["X"],
        }
    )

    expected_columns = ["id", "name"]

    with pytest.raises(TransformValidationError):
        validate_schema(df, expected_columns, "test_table")

def test_validate_not_null_fails_when_null_present():
    df = pd.DataFrame(
        {
            "id": [1, None],
            "name": ["A", "B"],
        }
    )

    not_null_columns = ["id", "name"]

    with pytest.raises(TransformValidationError):
        validate_not_null(df, not_null_columns, "test_table")

def test_validate_not_null_passes_when_no_nulls():
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "name": ["A", "B"],
        }
    )

    not_null_columns = ["id", "name"]

    validate_not_null(df, not_null_columns, "test_table")

def test_validate_row_count_fails_when_df_empty():
    df = pd.DataFrame(columns=["id", "name"])

    with pytest.raises(TransformValidationError):
        validate_row_count(df, "test_table")

def test_validate_row_drop_passes_when_drop_within_threshold():
    source_count = 100
    result_count = 95  # 95% залишилось

    validate_row_drop(
        source_count,
        result_count,
        table_name="test_table",
        threshold=0.9,
    )

def test_validate_row_drop_fails_when_drop_exceeds_threshold():
    source_count = 100
    result_count = 50  # 50% залишилось

    with pytest.raises(TransformValidationError):
        validate_row_drop(
            source_count,
            result_count,
            table_name="test_table",
            threshold=0.9,
        )