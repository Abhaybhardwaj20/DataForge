
import json
import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATAFORGE_SRC = PROJECT_ROOT / "src" / "dataforge"

sys.path.insert(0, str(DATAFORGE_SRC))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

import dataforge.cli as cli


def test_successful_pipeline_records_run(monkeypatch, tmp_path):
    """A no-new-records run should be recorded as successful."""
    project_root = tmp_path
    config_dir = project_root / "config"
    config_dir.mkdir()

    config_file = config_dir / "config.yaml"
    config_file.write_text("placeholder", encoding="utf-8")

    raw_file = project_root / "orders.csv"
    raw_file.write_text("placeholder", encoding="utf-8")

    silver_file = project_root / "data" / "silver" / "orders.parquet"
    silver_file.parent.mkdir(parents=True)
    silver_file.touch()

    config = {
        "project": {"name": "DataForge"},
        "data": {
            "raw_path": "orders.csv",
            "bronze_path": "data/bronze/orders.parquet",
            "silver_path": "data/silver/orders.parquet",
            "gold_path": "data/gold/product_revenue.parquet",
            "quarantine_path": "data/quarantine/invalid_orders.parquet",
            "state_path": "data/state/processed_orders.json",
        },
        "pipeline": {
            "incremental_processing": True,
            "incremental_key": "order_id",
        },
    }

    monkeypatch.setattr(cli, "setup_logging", lambda _: None)
    monkeypatch.setattr(cli.yaml, "safe_load", lambda _: config)

    orders = pd.DataFrame(
        [{
            "order_id": "1001",
            "customer_id": "C101",
            "product": "Mouse",
            "quantity": 1,
            "price": 500,
            "order_date": "2026-10-23",
            "status": "Completed",
        }]
    )

    monkeypatch.setattr(cli, "load_orders", lambda _: orders)
    monkeypatch.setattr(cli, "publish_orders", lambda *_: None)
    monkeypatch.setattr(
        cli,
        "split_valid_and_invalid_orders",
        lambda df: (df.copy(), df.iloc[0:0].copy()),
    )
    monkeypatch.setattr(cli, "save_quarantine", lambda *_: None)
    monkeypatch.setattr(cli, "load_processed_ids", lambda _: {"1001"})
    monkeypatch.setattr(
        cli,
        "save_quality_report",
        lambda *_: None,
    )
    monkeypatch.setattr(
        cli,
        "build_quality_report",
        lambda *_: {"raw_rows": 1, "silver_rows": 1},
    )

    existing_silver = pd.DataFrame(
        [{
            "order_id": "1001",
            "customer_id": "C101",
            "product": "Mouse",
            "quantity": 1,
            "price": 500,
            "order_date": pd.Timestamp("2026-10-23"),
            "status": "Completed",
            "total_amount": 500,
        }]
    )
    monkeypatch.setattr(cli.pd, "read_parquet", lambda _: existing_silver)

    captured = []
    monkeypatch.setattr(
        cli,
        "save_run_record",
        lambda record, path: captured.append((record.copy(), path)),
    )

    # Make the CLI resolve its project root to this test's temporary directory.
    original_file = cli.__file__
    monkeypatch.setattr(
        cli,
        "__file__",
        str(project_root / "src" / "dataforge" / "cli.py"),
    )

    try:
        cli.run_pipeline()
    finally:
        monkeypatch.setattr(cli, "__file__", original_file)

    assert len(captured) == 1
    record, history_path = captured[0]

    assert record["status"] == "success"
    assert record["raw_rows"] == 1
    assert record["new_rows"] == 0
    assert record["skipped_rows"] == 1
    assert record["silver_rows"] == 1
    assert history_path == project_root / "reports" / "run_history.json"


def test_failed_pipeline_records_error(monkeypatch, tmp_path):
    """A failed run should be recorded before the error is re-raised."""
    project_root = tmp_path
    config_dir = project_root / "config"
    config_dir.mkdir()

    config_file = config_dir / "config.yaml"
    config_file.write_text("placeholder", encoding="utf-8")

    config = {
        "project": {"name": "DataForge"},
        "data": {
            "raw_path": "missing.csv",
            "bronze_path": "data/bronze/orders.parquet",
            "silver_path": "data/silver/orders.parquet",
            "gold_path": "data/gold/product_revenue.parquet",
            "quarantine_path": "data/quarantine/invalid_orders.parquet",
            "state_path": "data/state/processed_orders.json",
        },
        "pipeline": {
            "incremental_processing": True,
            "incremental_key": "order_id",
        },
    }

    monkeypatch.setattr(cli, "setup_logging", lambda _: None)
    monkeypatch.setattr(cli.yaml, "safe_load", lambda _: config)
    monkeypatch.setattr(
        cli,
        "load_orders",
        lambda _: (_ for _ in ()).throw(
            FileNotFoundError("missing input")
        ),
    )

    captured = []
    monkeypatch.setattr(
        cli,
        "save_run_record",
        lambda record, path: captured.append((record.copy(), path)),
    )

    original_file = cli.__file__
    monkeypatch.setattr(
        cli,
        "__file__",
        str(project_root / "src" / "dataforge" / "cli.py"),
    )

    try:
        with pytest.raises(FileNotFoundError, match="missing input"):
            cli.run_pipeline()
    finally:
        monkeypatch.setattr(cli, "__file__", original_file)

    assert len(captured) == 1
    record, history_path = captured[0]

    assert record["status"] == "failed"
    assert "missing input" in record["error"]
    assert record["finished_at"] is not None
    assert history_path == project_root / "reports" / "run_history.json"
