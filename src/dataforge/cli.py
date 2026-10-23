
import logging
from pathlib import Path

import duckdb
import yaml

from ingest import load_orders
from transform import transform_orders
from validate import validate_orders
from publish import publish_orders
from quality import (
    check_pipeline_files,
    build_quality_report,
    save_quality_report,
)
from quarantine import split_valid_and_invalid_orders, save_quarantine
from incremental import load_processed_ids, split_new_orders, save_processed_ids


def setup_logging(project_root: Path) -> None:
    log_dir = project_root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_dir / "dataforge.log", encoding="utf-8"),
            logging.StreamHandler(),
        ],
        force=True,
    )


def run_pipeline() -> None:
    project_root = Path(__file__).resolve().parents[2]
    setup_logging(project_root)
    logger = logging.getLogger("dataforge")

    try:
        config_file = project_root / "config" / "config.yaml"
        with config_file.open("r", encoding="utf-8") as file:
            config = yaml.safe_load(file)

        raw_file = project_root / config["data"]["raw_path"]
        bronze_file = project_root / config["data"]["bronze_path"]
        silver_file = project_root / config["data"]["silver_path"]
        gold_file = project_root / config["data"]["gold_path"]
        quarantine_file = project_root / config["data"]["quarantine_path"]
        state_file = project_root / config["data"]["state_path"]

        incremental_enabled = config["pipeline"].get(
            "incremental_processing", False
        )
        incremental_key = config["pipeline"].get(
            "incremental_key", "order_id"
        )

        logger.info("Starting %s pipeline", config["project"]["name"])

        orders = load_orders(raw_file)
        logger.info("Loaded %s raw rows", len(orders))

        # Preserve the original input in Bronze.
        publish_orders(orders, bronze_file)

        # Separate invalid records before incremental processing.
        valid_orders, invalid_orders = split_valid_and_invalid_orders(orders)
        save_quarantine(invalid_orders, quarantine_file)

        logger.info(
            "Validation split: %s valid, %s quarantined",
            len(valid_orders),
            len(invalid_orders),
        )

        if incremental_enabled:
            processed_ids = load_processed_ids(state_file)
            new_orders, skipped_orders = split_new_orders(
                valid_orders,
                processed_ids,
                key=incremental_key,
            )
            logger.info(
                "Incremental split: %s new, %s previously processed",
                len(new_orders),
                len(skipped_orders),
            )
        else:
            processed_ids = set()
            new_orders = valid_orders.copy()
            skipped_orders = valid_orders.iloc[0:0].copy()

        # If there are no new records, preserve existing Silver and Gold outputs.
        if new_orders.empty:
            logger.info("No new valid orders to process")

            quality_summary = build_quality_report(orders, valid_orders)
            quality_summary["quarantined_rows"] = int(len(invalid_orders))
            quality_summary["new_rows"] = 0
            quality_summary["skipped_rows"] = int(len(skipped_orders))

            report_file = project_root / "reports" / "quality_report.json"
            save_quality_report(quality_summary, report_file)
            logger.info("DataForge pipeline completed with no new records")
            return

        validate_orders(new_orders)

        new_silver = transform_orders(new_orders)

        # Merge new transformed records with the existing Silver dataset.
        if silver_file.exists():
            existing_silver = __import__("pandas").read_parquet(silver_file)
            combined_silver = __import__("pandas").concat(
                [existing_silver, new_silver],
                ignore_index=True,
            )
            combined_silver = combined_silver.drop_duplicates(
                subset=[incremental_key],
                keep="first",
            )
        else:
            combined_silver = new_silver

        publish_orders(combined_silver, silver_file)

        # Generate Gold from the complete Silver dataset.
        gold_file.parent.mkdir(parents=True, exist_ok=True)
        connection = duckdb.connect()

        try:
            silver_path = silver_file.as_posix().replace("'", "''")
            gold_path = gold_file.as_posix().replace("'", "''")

            connection.execute(
                f"CREATE VIEW orders AS SELECT * FROM read_parquet('{silver_path}')"
            )
            connection.execute(
                f"""
                COPY (
                    SELECT product, SUM(total_amount) AS revenue
                    FROM orders
                    WHERE status = 'Completed'
                    GROUP BY product
                    ORDER BY revenue DESC
                )
                TO '{gold_path}' (FORMAT PARQUET)
                """
            )
        finally:
            connection.close()

        # Update state only after Silver and Gold have been published successfully.
        if incremental_enabled:
            newly_processed_ids = set(new_orders[incremental_key].astype(str))
            save_processed_ids(
                state_file,
                processed_ids | newly_processed_ids,
            )

        quality_summary = build_quality_report(orders, combined_silver)
        quality_summary["quarantined_rows"] = int(len(invalid_orders))
        quality_summary["new_rows"] = int(len(new_orders))
        quality_summary["skipped_rows"] = int(len(skipped_orders))

        report_file = project_root / "reports" / "quality_report.json"
        save_quality_report(quality_summary, report_file)
        logger.info("Data quality summary: %s", quality_summary)

        file_report = check_pipeline_files(project_root)
        file_report["Quarantine"] = quarantine_file.is_file()

        for layer, exists in file_report.items():
            if not exists:
                raise FileNotFoundError(f"{layer} output is missing")
            logger.info("%s layer is available", layer)

        logger.info("DataForge pipeline completed successfully")

    except Exception:
        logger.exception("DataForge pipeline failed")
        raise


if __name__ == "__main__":
    run_pipeline()
