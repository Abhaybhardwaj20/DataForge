
import pandas as pd


def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Clean order data and prepare it for the Silver layer."""
    df = df.copy()

    # Clean text columns.
    for column in ["status", "product", "customer_id"]:
        df[column] = df[column].str.strip()

    df["status"] = df["status"].str.title()

    # Convert dates and calculate order totals.
    df["order_date"] = pd.to_datetime(
        df["order_date"], errors="coerce"
    )
    df["total_amount"] = df["quantity"] * df["price"]

    # Remove rows with missing required values.
    required_columns = [
        "order_id",
        "customer_id",
        "product",
        "quantity",
        "price",
        "order_date",
        "status",
    ]
    df = df.dropna(subset=required_columns)

    # Remove invalid quantities and prices.
    df = df[(df["quantity"] > 0) & (df["price"] >= 0)]

    # Keep only the first occurrence of each order ID.
    df = df.drop_duplicates(subset=["order_id"], keep="first")

    return df.reset_index(drop=True)
