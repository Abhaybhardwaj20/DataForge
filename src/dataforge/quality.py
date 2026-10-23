
import json
from pathlib import Path

import pandas as pd


def check_pipeline_files(project_root: Path) -> dict:
    """Check whether each pipeline layer has its expected Parquet file."""
    expected_files = {
        "Bronze": project_root / "data" / "bronze" / "orders.parquet",
        "Silver": project_root / "data" / "silver" / "orders.parquet",
        "Gold": project_root / "data" / "gold" / "product_revenue.parquet",
    }

    return {
        layer: file_path.exists()
        for layer, file_path in expected_files.items()
    }


def build_quality_report(
    raw_df: pd.DataFrame,
    silver_df: pd.DataFrame,
) -> dict:
    """Build a data-quality summary for the pipeline run."""
    return {
        "raw_rows": int(len(raw_df)),
        "silver_rows": int(len(silver_df)),
        "rows_removed": int(len(raw_df) - len(silver_df)),
        "unique_orders": int(silver_df["order_id"].nunique()),
        "unique_customers": int(silver_df["customer_id"].nunique()),
        "unique_products": int(silver_df["product"].nunique()),
        "missing_values_silver": int(silver_df.isna().sum().sum()),
        "duplicate_order_ids_silver": int(
            silver_df["order_id"].duplicated().sum()
        ),
    }


def save_quality_report(report: dict, output_path: str | Path) -> None:
    """Save the quality report as readable JSON."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(report, file, indent=4)

    print(f"Quality report saved to: {output_path}")
