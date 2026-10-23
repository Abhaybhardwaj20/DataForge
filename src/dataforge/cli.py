
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


def setup_logging(project_root: Path) -> None:
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

        logger.info("Starting %s pipeline", config["project"]["name"])

        orders = load_orders(raw_file)
        validate_orders(orders)
        logger.info("Loaded and validated %s raw rows", len(orders))

        publish_orders(orders, bronze_file)

        silver_orders = transform_orders(orders)
        publish_orders(silver_orders, silver_file)

        # Save the reusable data-quality report.
        quality_summary = build_quality_report(orders, silver_orders)
        report_file = project_root / "reports" / "quality_report.json"
        save_quality_report(quality_summary, report_file)
        logger.info("Data quality summary: %s", quality_summary)

        # Generate Gold analytics.
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

            gold_count = connection.execute(
                f"SELECT COUNT(*) FROM read_parquet('{gold_path}')"
            ).fetchone()[0]

            logger.info("Gold analytics generated: %s products", gold_count)

        finally:
            connection.close()

        # Verify output files.
        file_report = check_pipeline_files(project_root)

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
