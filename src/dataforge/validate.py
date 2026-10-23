
import pandas as pd


REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "product",
    "quantity",
    "price",
    "order_date",
    "status",
]


def validate_orders(df: pd.DataFrame) -> None:
    """Validate the structure and essential values of order data."""

    missing_columns = set(REQUIRED_COLUMNS) - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Missing columns: {sorted(missing_columns)}"
        )

    if df.empty:
        raise ValueError("Orders dataset is empty.")

    if df["order_id"].isna().any():
        raise ValueError("Some orders have a missing order_id.")

    if df["order_id"].duplicated().any():
        raise ValueError("Duplicate order_id values found.")

    if df["customer_id"].isna().any():
        raise ValueError("Some orders have a missing customer_id.")

    if df["product"].isna().any():
        raise ValueError("Some orders have a missing product.")

    if df["quantity"].isna().any() or (df["quantity"] <= 0).any():
        raise ValueError("Quantity must be present and greater than zero.")

    if df["price"].isna().any() or (df["price"] < 0).any():
        raise ValueError("Price must be present and non-negative.")

    if df["order_date"].isna().any():
        raise ValueError("Some orders have a missing order_date.")

    if df["status"].isna().any():
        raise ValueError("Some orders have a missing status.")

    print("Validation passed!")
