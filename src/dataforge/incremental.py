
import json
from pathlib import Path
from typing import Any

import pandas as pd


def load_processed_ids(state_path: str | Path) -> set[str]:
    """Load previously processed order IDs from the state file."""
    state_path = Path(state_path)

    if not state_path.exists():
        return set()

    with state_path.open("r", encoding="utf-8") as file:
        state: dict[str, Any] = json.load(file)

    return {str(order_id) for order_id in state.get("processed_ids", [])}


def split_new_orders(
    orders: pd.DataFrame,
    processed_ids: set[str],
    key: str = "order_id",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split incoming orders into new records and previously processed records."""
    if key not in orders.columns:
        raise ValueError(f"Missing incremental key: {key}")

    if orders[key].isna().any():
        raise ValueError(f"Incremental key '{key}' cannot contain missing values")

    ids = orders[key].astype(str)
    already_processed = ids.isin(processed_ids)

    new_orders = orders.loc[~already_processed].copy().reset_index(drop=True)
    skipped_orders = orders.loc[already_processed].copy().reset_index(drop=True)

    return new_orders, skipped_orders


def save_processed_ids(
    state_path: str | Path,
    processed_ids: set[str],
) -> None:
    """Persist processed order IDs in a JSON state file."""
    state_path = Path(state_path)
    state_path.parent.mkdir(parents=True, exist_ok=True)

    temporary_path = state_path.with_suffix(state_path.suffix + ".tmp")

    with temporary_path.open("w", encoding="utf-8") as file:
        json.dump(
            {"processed_ids": sorted(processed_ids)},
            file,
            indent=2,
        )

    temporary_path.replace(state_path)
