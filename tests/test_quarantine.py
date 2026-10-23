
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dataforge.quarantine import (
    split_valid_and_invalid_orders,
    save_quarantine,
)


def sample_orders():
    return pd.DataFrame({
        "order_id": [1001, 1002, 1003],
        "customer_id": ["C101", "C102", "C103"],
        "product": ["Laptop", "Mouse", "Keyboard"],
        "quantity": [1, 0, 2],
        "price": [55000, 500, 1500],
        "order_date": ["2026-10-20", "2026-10-21", "2026-10-22"],
        "status": ["Completed", "Pending", "Completed"],
    })


def test_invalid_quantity_is_quarantined():
    valid, invalid = split_valid_and_invalid_orders(sample_orders())
    assert len(valid) == 2
    assert len(invalid) == 1
    assert "Quantity must be greater than zero" in invalid.loc[0, "rejection_reason"]


def test_valid_orders_are_preserved():
    valid, invalid = split_valid_and_invalid_orders(sample_orders())
    assert valid["order_id"].tolist() == [1001, 1003]
    assert len(invalid) == 1


def test_duplicate_order_is_quarantined():
    df = pd.concat([sample_orders().iloc[[0]], sample_orders().iloc[[0]]],
                   ignore_index=True)
    valid, invalid = split_valid_and_invalid_orders(df)
    assert len(valid) == 1
    assert len(invalid) == 1
    assert "Duplicate order_id" in invalid.loc[0, "rejection_reason"]


def test_missing_columns_raise_error():
    with pytest.raises(ValueError, match="Missing columns"):
        split_valid_and_invalid_orders(pd.DataFrame({"order_id": [1001]}))


def test_quarantine_file_is_saved(tmp_path):
    _, invalid = split_valid_and_invalid_orders(sample_orders())
    output_file = tmp_path / "quarantine" / "invalid.parquet"

    save_quarantine(invalid, output_file)

    assert output_file.is_file()
    saved = pd.read_parquet(output_file)
    assert len(saved) == 1
    assert "rejection_reason" in saved.columns


def test_all_valid_orders_produce_empty_quarantine():
    df = sample_orders().iloc[[0, 2]].copy()
    valid, invalid = split_valid_and_invalid_orders(df)
    assert len(valid) == 2
    assert invalid.empty
