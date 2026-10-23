
import json
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dataforge.incremental import (
    load_processed_ids,
    save_processed_ids,
    split_new_orders,
)


def sample_orders():
    return pd.DataFrame({
        "order_id": [1001, 1002, 1003],
        "product": ["Laptop", "Mouse", "Keyboard"],
    })


def test_missing_state_returns_empty_set(tmp_path):
    assert load_processed_ids(tmp_path / "state.json") == set()


def test_state_round_trip(tmp_path):
    state_file = tmp_path / "state.json"
    expected = {"1001", "1002"}

    save_processed_ids(state_file, expected)

    assert load_processed_ids(state_file) == expected


def test_new_and_processed_orders_are_separated():
    new, skipped = split_new_orders(
        sample_orders(),
        processed_ids={"1001", "1003"},
    )

    assert new["order_id"].tolist() == [1002]
    assert skipped["order_id"].tolist() == [1001, 1003]


def test_no_processed_orders_means_all_are_new():
    new, skipped = split_new_orders(sample_orders(), set())

    assert len(new) == 3
    assert skipped.empty


def test_all_processed_orders_are_skipped():
    new, skipped = split_new_orders(
        sample_orders(),
        processed_ids={"1001", "1002", "1003"},
    )

    assert new.empty
    assert len(skipped) == 3


def test_missing_incremental_key_raises_error():
    with pytest.raises(ValueError, match="Missing incremental key"):
        split_new_orders(pd.DataFrame({"product": ["Laptop"]}), set())


def test_missing_order_id_raises_error():
    df = pd.DataFrame({"order_id": [1001, None]})

    with pytest.raises(ValueError, match="cannot contain missing values"):
        split_new_orders(df, set())


def test_state_file_contains_processed_ids(tmp_path):
    state_file = tmp_path / "nested" / "state.json"
    save_processed_ids(state_file, {"1001"})

    with state_file.open("r", encoding="utf-8") as file:
        data = json.load(file)

    assert data["processed_ids"] == ["1001"]
