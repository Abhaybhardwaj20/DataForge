
import json
from datetime import datetime
from pathlib import Path

import pytest

from src.dataforge.run_tracking import (
    create_run_record,
    finish_run_record,
    save_run_record,
)


def test_create_run_record_has_required_fields():
    record = create_run_record()

    assert record["run_id"]
    assert record["status"] == "running"
    assert record["finished_at"] is None
    assert record["error"] is None


def test_create_run_record_uses_unique_ids():
    first = create_run_record()
    second = create_run_record()

    assert first["run_id"] != second["run_id"]


def test_create_run_record_has_valid_utc_timestamp():
    record = create_run_record()

    started_at = datetime.fromisoformat(record["started_at"])

    assert started_at.tzinfo is not None
    assert started_at.utcoffset().total_seconds() == 0


def test_create_run_record_initializes_metrics():
    record = create_run_record()

    expected_metrics = [
        "raw_rows",
        "valid_rows",
        "new_rows",
        "skipped_rows",
        "quarantined_rows",
        "silver_rows",
    ]

    for metric in expected_metrics:
        assert record[metric] == 0


def test_finish_run_record_success():
    record = create_run_record()

    result = finish_run_record(record, "success")

    assert result["status"] == "success"
    assert result["finished_at"] is not None
    assert result["error"] is None


def test_finish_run_record_failure():
    record = create_run_record()

    result = finish_run_record(
        record,
        "failed",
        error="Input file not found",
    )

    assert result["status"] == "failed"
    assert result["finished_at"] is not None
    assert result["error"] == "Input file not found"


def test_finish_run_record_rejects_invalid_status():
    record = create_run_record()

    with pytest.raises(ValueError, match="Status must be"):
        finish_run_record(record, "unknown")


def test_save_run_record_creates_file_and_parent_directories(tmp_path):
    output_path = tmp_path / "reports" / "history" / "runs.json"
    record = finish_run_record(create_run_record(), "success")

    save_run_record(record, output_path)

    assert output_path.exists()

    history = json.loads(output_path.read_text(encoding="utf-8"))
    assert len(history) == 1
    assert history[0]["run_id"] == record["run_id"]


def test_save_run_record_appends_history(tmp_path):
    output_path = tmp_path / "runs.json"

    first = finish_run_record(create_run_record(), "success")
    second = finish_run_record(create_run_record(), "failed", error="Test error")

    save_run_record(first, output_path)
    save_run_record(second, output_path)

    history = json.loads(output_path.read_text(encoding="utf-8"))

    assert len(history) == 2
    assert history[0]["run_id"] == first["run_id"]
    assert history[1]["run_id"] == second["run_id"]


def test_save_run_record_rejects_non_list_history(tmp_path):
    output_path = tmp_path / "runs.json"
    output_path.write_text('{"run_id": "invalid"}', encoding="utf-8")

    with pytest.raises(ValueError, match="JSON list"):
        save_run_record(create_run_record(), output_path)


def test_save_run_record_preserves_existing_history_on_invalid_file(tmp_path):
    output_path = tmp_path / "runs.json"
    original_content = '{"run_id": "invalid"}'
    output_path.write_text(original_content, encoding="utf-8")

    with pytest.raises(ValueError):
        save_run_record(create_run_record(), output_path)

    assert output_path.read_text(encoding="utf-8") == original_content
