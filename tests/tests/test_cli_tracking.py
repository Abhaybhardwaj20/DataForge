
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
from dataforge.incremental import load_processed_ids, save_processed_ids
from dataforge.storage import StorageUploadError


@pytest.mark.parametrize("storage_enabled", [False, True])
def test_successful_pipeline_records_run(
    monkeypatch, tmp_path, storage_enabled
):
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
    if storage_enabled:
        config["storage"] = {
            "enabled": True,
            "endpoint_url": "http://object-storage:9000",
            "bucket": "dataforge-learning-bucket",
        }

    monkeypatch.setattr(cli, "setup_logging", lambda _: None)
    monkeypatch.setattr(cli.yaml, "safe_load", lambda _: config)
    upload_calls = []
    monkeypatch.delenv("DATAFORGE_S3_ENDPOINT", raising=False)
    monkeypatch.setattr(
        cli,
        "upload_pipeline_outputs",
        lambda *args, **kwargs: upload_calls.append((args, kwargs)),
    )

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
    if storage_enabled:
        assert len(upload_calls) == 1
        paths, options = upload_calls[0]
        assert paths == (
            project_root / "data" / "bronze" / "orders.parquet",
            silver_file,
            project_root / "data" / "gold" / "product_revenue.parquet",
            project_root / "data" / "quarantine" / "invalid_orders.parquet",
        )
        assert options == {
            "bucket": "dataforge-learning-bucket",
            "endpoint_url": "http://object-storage:9000",
        }
    else:
        assert upload_calls == []


def prepare_new_order_pipeline(monkeypatch, project_root):
    config_dir = project_root / "config"
    config_dir.mkdir()
    (config_dir / "config.yaml").write_text("placeholder", encoding="utf-8")

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
        "storage": {
            "enabled": True,
            "endpoint_url": "http://object-storage:9000",
            "bucket": "dataforge-learning-bucket",
        },
    }
    monkeypatch.setattr(cli, "setup_logging", lambda _: None)
    monkeypatch.setattr(cli.yaml, "safe_load", lambda _: config)
    monkeypatch.delenv("DATAFORGE_S3_ENDPOINT", raising=False)
    monkeypatch.setattr(
        cli,
        "load_orders",
        lambda _: pd.DataFrame(
            [{
                "order_id": 1002,
                "customer_id": "C102",
                "product": "Mouse",
                "quantity": 2,
                "price": 500,
                "order_date": "2026-10-23",
                "status": "Completed",
            }]
        ),
    )
    state_file = project_root / "data" / "state" / "processed_orders.json"
    save_processed_ids(state_file, {"1001"})
    monkeypatch.setattr(
        cli,
        "__file__",
        str(project_root / "src" / "dataforge" / "cli.py"),
    )
    return state_file


def test_upload_failure_preserves_state_and_records_failed_run(
    monkeypatch, tmp_path
):
    state_file = prepare_new_order_pipeline(monkeypatch, tmp_path)

    def fail_upload(*args, **kwargs):
        raise StorageUploadError("simulated local S3 outage")

    monkeypatch.setattr(cli, "upload_pipeline_outputs", fail_upload)

    with pytest.raises(StorageUploadError, match="simulated local S3 outage"):
        cli.run_pipeline()

    assert load_processed_ids(state_file) == {"1001"}
    assert (tmp_path / "data" / "bronze" / "orders.parquet").is_file()
    assert (tmp_path / "data" / "silver" / "orders.parquet").is_file()
    assert (tmp_path / "data" / "gold" / "product_revenue.parquet").is_file()
    assert (tmp_path / "data" / "quarantine" / "invalid_orders.parquet").is_file()

    history = json.loads(
        (tmp_path / "reports" / "run_history.json").read_text(
            encoding="utf-8"
        )
    )
    assert history[-1]["status"] == "failed"
    assert "simulated local S3 outage" in history[-1]["error"]


def test_successful_upload_happens_before_state_persistence(
    monkeypatch, tmp_path
):
    state_file = prepare_new_order_pipeline(monkeypatch, tmp_path)
    upload_observations = []

    def verify_upload_before_state(*args, **kwargs):
        assert load_processed_ids(state_file) == {"1001"}
        assert all(Path(path).is_file() for path in args)
        upload_observations.append((args, kwargs))

    monkeypatch.setattr(
        cli,
        "upload_pipeline_outputs",
        verify_upload_before_state,
    )

    cli.run_pipeline()

    assert len(upload_observations) == 1
    assert load_processed_ids(state_file) == {"1001", "1002"}
    history = json.loads(
        (tmp_path / "reports" / "run_history.json").read_text(
            encoding="utf-8"
        )
    )
    assert history[-1]["status"] == "success"


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
