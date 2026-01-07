import pandas as pd


class TransformValidationError(Exception):
    """Raised when transform validation fails."""
    pass


def validate_schema(
    df: pd.DataFrame,
    expected_columns: list[str],
    table_name: str,
):
    missing = set(expected_columns) - set(df.columns)
    extra = set(df.columns) - set(expected_columns)

    if missing or extra:
        raise TransformValidationError(
            f"[{table_name}] Schema mismatch | "
            f"Missing: {sorted(missing)} | "
            f"Extra: {sorted(extra)}"
        )


def validate_not_null(
    df: pd.DataFrame,
    not_null_columns: list[str],
    table_name: str,
):
    null_counts = df[not_null_columns].isnull().sum()
    failed = null_counts[null_counts > 0]

    if not failed.empty:
        raise TransformValidationError(
            f"[{table_name}] NULL values detected:\n{failed.to_string()}"
        )


def validate_row_count(
    df: pd.DataFrame,
    table_name: str,
):
    if df.empty:
        raise TransformValidationError(
            f"[{table_name}] Transform produced 0 rows"
        )


def validate_row_drop(
    source_count: int,
    result_count: int,
    table_name: str,
    threshold: float = 0.9,
):
    if source_count == 0:
        return  # nothing to compare

    if result_count < source_count * threshold:
        raise TransformValidationError(
            f"[{table_name}] Row count dropped too much "
            f"(source={source_count}, result={result_count})"
        )