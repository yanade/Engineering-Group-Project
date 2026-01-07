# import pandas as pd
# import pytest

# from src.transformation.S3T_transform_client import S3TransformationClient
# from src.transformation.validation import TransformValidationError


# @pytest.fixture
# def sales_order_df():
#     return pd.DataFrame(
#         {
#             "sales_order_id": [1, 2],
#             "created_at": [
#                 "2024-01-01 10:00:00",
#                 "2024-01-02 11:30:00",
#             ],
#             "last_updated": [
#                 "2024-01-01 12:00:00",
#                 "2024-01-02 12:45:00",
#             ],
#             "staff_id": [10, 20],
#             "counterparty_id": [100, 200],
#             "units_sold": [5, 10],
#             "unit_price": [2.5, 3.0],
#             "currency_id": [1, 1],
#             "design_id": [50, 60],
#             "agreed_payment_date": [
#                 "2024-01-10",
#                 "2024-01-15",
#             ],
#             "agreed_delivery_date": [
#                 "2024-01-12",
#                 "2024-01-18",
#             ],
#             "agreed_delivery_location_id": [1000, 2000],
#         }
#     )

# @pytest.fixture
# def transform_service():
#     return S3TransformationClient(
#         ingest_bucket="fake-ingest",
#         processed_bucket="fake-processed",
#     )


# def test_make_fact_sales_order_success(
#     mocker,
#     transform_service,
#     sales_order_df,
# ):
#     mocker.patch.object(
#         S3TransformationClient,
#         "_get_ingest_table",
#         return_value=sales_order_df,
#     )

#     fact_sales_order = transform_service.make_fact_sales_order()

#     assert isinstance(fact_sales_order, pd.DataFrame)

#     expected_columns = [
#         "sales_order_id",
#         "created_date",
#         "created_time",
#         "last_updated_date",
#         "last_updated_time",
#         "sales_staff_id",
#         "counterparty_id",
#         "units_sold",
#         "unit_price",
#         "currency_id",
#         "design_id",
#         "agreed_payment_date",
#         "agreed_delivery_date",
#         "agreed_delivery_location_id",
#     ]
#     assert list(fact_sales_order.columns) == expected_columns
#     assert fact_sales_order.loc[0, "sales_order_id"] == 1
#     assert fact_sales_order.loc[0, "sales_staff_id"] == 10
#     assert fact_sales_order.loc[0, "units_sold"] == 5
#     assert fact_sales_order.loc[0, "unit_price"] == 2.5
#     assert isinstance(fact_sales_order.loc[0, "created_date"], type(pd.Timestamp("2024-01-01").date()))
#     assert isinstance(fact_sales_order.loc[0, "created_time"], type(pd.Timestamp("10:00:00").time()))

# def test_make_fact_sales_order_fails_on_null(
#     mocker,
#     transform_service,
#     sales_order_df,
# ):
#     bad_df = sales_order_df.copy()
#     bad_df.loc[0, "units_sold"] = None

#     mocker.patch.object(
#         S3TransformationClient,
#         "_get_ingest_table",
#         return_value=bad_df,
#     )

#     with pytest.raises(TransformValidationError):
#         transform_service.make_fact_sales_order()