
from pathlib import Path

import pandas as pd


def split_valid_and_invalid_orders(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Separate valid orders from invalid orders and explain each rejection."""

    required_columns = [
        "order_id",
        "customer_id",
        "product",
        "quantity",
        "price",
        "order_date",
        "status",
    ]

    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing columns: {sorted(missing_columns)}")

    data = df.copy()
    reasons = pd.Series("", index=data.index, dtype="object")

    def add_reason(mask: pd.Series, message: str) -> None:
        nonlocal reasons
        mask = mask.fillna(True)
        reasons.loc[mask] = reasons.loc[mask].apply(
            lambda current: f"{current}; {message}" if current else message
        )

    add_reason(data["order_id"].isna(), "Missing order_id")
    add_reason(data["customer_id"].isna(), "Missing customer_id")
    add_reason(data["product"].isna(), "Missing product")
    add_reason(data["status"].isna(), "Missing status")

    quantity = pd.to_numeric(data["quantity"], errors="coerce")
    price = pd.to_numeric(data["price"], errors="coerce")
    dates = pd.to_datetime(data["order_date"], errors="coerce")

    add_reason(quantity.isna(), "Invalid or missing quantity")
    add_reason(quantity.le(0), "Quantity must be greater than zero")
    add_reason(price.isna(), "Invalid or missing price")
    add_reason(price.lt(0), "Price must be non-negative")
    add_reason(dates.isna(), "Invalid or missing order_date")

    duplicate_ids = data["order_id"].notna() & data["order_id"].duplicated(
        keep="first"
    )
    add_reason(duplicate_ids, "Duplicate order_id")

    data["quantity"] = quantity
    data["price"] = price
    data["order_date"] = dates

    invalid_mask = reasons.ne("")
    valid = data.loc[~invalid_mask].copy().reset_index(drop=True)
    invalid = data.loc[invalid_mask].copy()
    invalid["rejection_reason"] = reasons.loc[invalid_mask]
    invalid = invalid.reset_index(drop=True)

    return valid, invalid


def save_quarantine(
    invalid_orders: pd.DataFrame,
    output_path: str | Path,
) -> None:
    """Save rejected orders as a Parquet file."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    invalid_orders.to_parquet(output_path, index=False)
    print(f"Quarantined {len(invalid_orders)} rows: {output_path}")
