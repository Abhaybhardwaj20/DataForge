
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def create_run_record() -> dict[str, Any]:
    """Create metadata for a new DataForge pipeline run."""
    return {
        "run_id": str(uuid.uuid4()),
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
        "status": "running",
        "raw_rows": 0,
        "valid_rows": 0,
        "new_rows": 0,
        "skipped_rows": 0,
        "quarantined_rows": 0,
        "silver_rows": 0,
        "error": None,
    }


def finish_run_record(
    record: dict[str, Any],
    status: str,
    *,
    error: str | None = None,
) -> dict[str, Any]:
    """Mark a pipeline run as successful or failed."""
    if status not in {"success", "failed"}:
        raise ValueError("Status must be 'success' or 'failed'")

    record["finished_at"] = datetime.now(timezone.utc).isoformat()
    record["status"] = status
    record["error"] = error
    return record


def save_run_record(
    record: dict[str, Any],
    output_path: str | Path,
) -> None:
    """Append a pipeline run record to a JSON history file."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists():
        with output_path.open("r", encoding="utf-8") as file:
            history = json.load(file)

        if not isinstance(history, list):
            raise ValueError("Run history must contain a JSON list")
    else:
        history = []

    history.append(record)

    temporary_path = output_path.with_suffix(
        output_path.suffix + ".tmp"
    )

    with temporary_path.open("w", encoding="utf-8") as file:
        json.dump(history, file, indent=4)

    temporary_path.replace(output_path)
