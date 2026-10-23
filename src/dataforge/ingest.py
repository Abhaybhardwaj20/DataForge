from pathlib import Path

import pandas as pd


def load_orders(file_path: str | Path) -> pd.DataFrame:
    """Load order records from a CSV file."""
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"Orders file not found: {file_path}")

    return pd.read_csv(file_path)


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parents[2]
    orders_file = project_root / "data" / "raw" / "orders.csv"

    orders = load_orders(orders_file)

    print("DataForge — Order Ingestion")
    print(f"Rows loaded: {len(orders)}")
    print(orders.head())