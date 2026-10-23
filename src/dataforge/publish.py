
from pathlib import Path

import pandas as pd


def publish_orders(df: pd.DataFrame, output_path: str | Path) -> None:
    """Save processed orders as a Parquet file."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(output_path, index=False)

    print(f"Published {len(df)} rows to: {output_path}")


if __name__ == "__main__":
    from ingest import load_orders
    from transform import transform_orders

    project_root = Path(__file__).resolve().parents[2]
    raw_file = project_root / "data" / "raw" / "orders.csv"
    silver_file = project_root / "data" / "silver" / "orders.parquet"

    orders = load_orders(raw_file)
    silver_orders = transform_orders(orders)
    publish_orders(silver_orders, silver_file)
