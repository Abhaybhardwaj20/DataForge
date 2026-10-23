
import logging
from pathlib import Path

import duckdb
import pandas as pd
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
from run_tracking import (
    create_run_record,
    finish_run_record,
    save_run_record,
)


def setup_logging(project_root: Path) -> None:
    """Configure console and file logging."""
    log_dir = project_root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(
                log_dir / "dataforge.log",
                encoding="utf-8",
            ),
            logging.StreamHandler(),
        ],
        force=True,
    )


def run_pipeline() -> None:
    """Run DataForge and record the outcome of every execution."""
    project_root = Path(__file__).resolve().parents[2]
    setup_logging(project_root)
    logger = logging.getLogger("dataforge")

    run_record = create_run_record()
    history_file = project_root / "reports" / "run_history.json"

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

        logger.info(
            "Starting %s pipeline | Run ID: %s",
            config["project"]["name"],
            run_record["run_id"],
        )

        # 1. Ingest raw data.
        orders = load_orders(raw_file)
        run_record["raw_rows"] = int(len(orders))

        logger.info("Loaded %s raw rows", len(orders))

        # 2. Preserve the original input in Bronze.
        publish_orders(orders, bronze_file)

        # 3. Separate invalid records into quarantine.
        valid_orders, invalid_orders = split_valid_and_invalid_orders(
            orders
        )

        run_record["valid_rows"] = int(len(valid_orders))
        run_record["quarantined_rows"] = int(len(invalid_orders))

        save_quarantine(invalid_orders, quarantine_file)

        logger.info(
            "Validation split: %s valid, %s quarantined",
            len(valid_orders),
            len(invalid_orders),
        )

        # 4. Identify new and previously processed records.
        if incremental_enabled:
            processed_ids = load_processed_ids(state_file)

            new_orders, skipped_orders = split_new_orders(
                valid_orders,
                processed_ids,
                key=incremental_key,
            )
        else:
            processed_ids = set()
            new_orders = valid_orders.copy()
            skipped_orders = valid_orders.iloc[0:0].copy()

        run_record["new_rows"] = int(len(new_orders))
        run_record["skipped_rows"] = int(len(skipped_orders))

        logger.info(
            "Incremental split: %s new, %s previously processed",
            len(new_orders),
            len(skipped_orders),
        )

        report_file = project_root / "reports" / "quality_report.json"

        # 5. Handle runs with no new records.
        if new_orders.empty:
            logger.info("No new valid orders to process")

            if silver_file.exists():
                run_record["silver_rows"] = int(
                    len(pd.read_parquet(silver_file))
                )

            quality_summary = build_quality_report(
                orders,
                valid_orders,
            )
            quality_summary["quarantined_rows"] = int(len(invalid_orders))
            quality_summary["new_rows"] = 0
            quality_summary["skipped_rows"] = int(len(skipped_orders))

            save_quality_report(quality_summary, report_file)

            finish_run_record(run_record, "success")
            save_run_record(run_record, history_file)

            logger.info(
                "Pipeline completed with no new records | Run ID: %s",
                run_record["run_id"],
            )
            return

        # 6. Validate new records before transformation.
        validate_orders(new_orders)

        # 7. Transform new records into Silver.
        new_silver = transform_orders(new_orders)

        # 8. Merge with the existing Silver dataset.
        if silver_file.exists():
            existing_silver = pd.read_parquet(silver_file)

            combined_silver = pd.concat(
                [existing_silver, new_silver],
                ignore_index=True,
            )

            combined_silver = combined_silver.drop_duplicates(
                subset=[incremental_key],
                keep="first",
            ).reset_index(drop=True)
        else:
            combined_silver = new_silver

        publish_orders(combined_silver, silver_file)
        run_record["silver_rows"] = int(len(combined_silver))

        # 9. Generate Gold analytics using DuckDB.
        gold_file.parent.mkdir(parents=True, exist_ok=True)

        connection = duckdb.connect()

        try:
            silver_path = silver_file.as_posix().replace("'", "''")
            gold_path = gold_file.as_posix().replace("'", "''")

            connection.execute(
                f"""
                CREATE VIEW orders AS
                SELECT * FROM read_parquet('{silver_path}')
                """
            )

            connection.execute(
                f"""
                COPY (
                    SELECT
                        product,
                        SUM(total_amount) AS revenue
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

        # 10. Save incremental state after Silver and Gold publication.
        if incremental_enabled:
            newly_processed_ids = set(
                new_orders[incremental_key].astype(str)
            )

            save_processed_ids(
                state_file,
                processed_ids | newly_processed_ids,
            )

        # 11. Save the data-quality report.
        quality_summary = build_quality_report(
            orders,
            combined_silver,
        )
        quality_summary["quarantined_rows"] = int(len(invalid_orders))
        quality_summary["new_rows"] = int(len(new_orders))
        quality_summary["skipped_rows"] = int(len(skipped_orders))

        save_quality_report(quality_summary, report_file)
        logger.info("Data quality summary: %s", quality_summary)

        # 12. Verify expected pipeline outputs.
        file_report = check_pipeline_files(project_root)
        file_report["Quarantine"] = quarantine_file.is_file()

        for layer, exists in file_report.items():
            if not exists:
                raise FileNotFoundError(
                    f"{layer} output is missing"
                )

            logger.info("%s layer is available", layer)

        # 13. Record successful execution.
        finish_run_record(run_record, "success")
        save_run_record(run_record, history_file)

        logger.info(
            "DataForge pipeline completed successfully | Run ID: %s",
            run_record["run_id"],
        )

    except Exception as exc:
        # Record a failure, including the error message.
        finish_run_record(
            run_record,
            "failed",
            error=f"{type(exc).__name__}: {exc}",
        )

        try:
            save_run_record(run_record, history_file)
        except Exception:
            logger.exception("Could not save the pipeline run history")

        logger.exception(
            "DataForge pipeline failed | Run ID: %s",
            run_record["run_id"],
        )
        raise


if __name__ == "__main__":
    run_pipeline()
