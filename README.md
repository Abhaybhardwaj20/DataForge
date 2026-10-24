
# DataForge — Cloud Data Engineering Platform

A local-first data engineering project built with Python, pandas, Parquet, DuckDB, and SQL. DataForge ingests order data, validates records, transforms them into analytics-ready datasets, and generates quality reports.

## Features

- CSV ingestion using Python and pandas
- Source-data validation
- Bronze, Silver, and Gold data layers
- Parquet storage using PyArrow
- Data cleaning, duplicate removal, and total-amount calculation
- SQL analytics using DuckDB
- Product-level completed-revenue reporting
- JSON data-quality reports
- Pipeline logging and exception diagnostics
- Automated tests using pytest

## Architecture

```text
data/raw/orders.csv
        |
        v
   Ingestion
        |
        v
 Source Validation
        |
        v
 Bronze Layer
        |
        v
 Cleaning and Transformation
        |
        v
 Silver Layer
        |
        v
 DuckDB Analytics
        |
        v
 Gold Layer
        |
        v
 Quality Report and Logs
```

## Technology Stack

- Python
- pandas
- PyArrow
- DuckDB
- PyYAML
- pytest
- Parquet
- SQL

## Project Structure

```text
dataforge/
├── .github/workflows/tests.yml
├── config/config.yaml
├── data/raw/orders.csv
├── src/dataforge/
│   ├── __init__.py
│   ├── cli.py
│   ├── incremental.py
│   ├── ingest.py
│   ├── publish.py
│   ├── quality.py
│   ├── quarantine.py
│   ├── run_tracking.py
│   ├── storage.py
│   ├── transform.py
│   └── validate.py
├── sql/analytics.sql
├── tests/
│   ├── test_quarantine.py
│   ├── test_storage.py
│   ├── test_transform.py
│   └── tests/
│       ├── test_cli_tracking.py
│       ├── test_incremental.py
│       └── test_run_tracking.py
├── .dockerignore
├── .gitignore
├── compose.yaml
├── Dockerfile
├── README.md
└── requirements.txt
```

The pipeline creates its Bronze, Silver, Gold, and quarantine Parquet outputs, state, reports, and logs when it runs. These generated files are ignored by Git.

## Prerequisites

- Python installed
- Git installed
- VS Code or another Python editor

## Setup on Windows

Clone or download the repository, then open its root directory in PowerShell.

Create a virtual environment:

```powershell
python -m venv .venv
```

Activate it:

```powershell
.\.venv\Scripts\Activate.ps1
```

Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

## Run the Pipeline

The Docker Compose setup runs the pipeline container and local RustFS object storage. RustFS provides S3-compatible storage on your machine; it is not AWS S3. The pipeline uploads outputs to the `dataforge-learning-bucket` bucket configured in `config/config.yaml`, so that bucket must exist in RustFS.

Create or update the root `.env` file with the required variable names below. Keep its actual credential values private:

```text
RUSTFS_ACCESS_KEY=<local RustFS access key>
RUSTFS_SECRET_KEY=<local RustFS secret key>
```

`DATAFORGE_S3_ENDPOINT` and `AWS_DEFAULT_REGION` are optional; Compose defaults them to the RustFS service endpoint and `us-east-1`. The `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` variables are set inside the pipeline container from the RustFS credentials above.

Keep `.env` untracked. It is ignored by `.gitignore`; do not force-add or commit it.

From the project root, start RustFS and run the pipeline:

```powershell
docker compose up -d object-storage
docker compose run --rm dataforge
```

To stop RustFS after the run:

```powershell
docker compose down
```

For a local Python run instead, use the virtual environment and make sure RustFS is running with the same credentials in your shell environment:

```powershell
python src\dataforge\cli.py
```

The pipeline loads the CSV, validates the source data, writes Bronze and Silver Parquet files, generates Gold product-revenue analytics, saves a JSON quality report, and logs pipeline activity.

## Run Automated Tests

```powershell
python -m pytest tests -v
```

## Quality Report

The generated report is stored at:

```text
reports/quality_report.json
```

It records raw and Silver row counts, rows removed, unique orders, unique customers, unique products, missing Silver values, and duplicate Silver order IDs.

## Verified Local Results

The sample dataset currently contains 9 order records.

The latest verified quality report showed:

- Raw rows: 9
- Silver rows: 9
- Rows removed: 0
- Unique orders: 9
- Unique customers: 7
- Unique products: 6
- Missing Silver values: 0
- Duplicate Silver order IDs: 0

The Gold dataset contains 5 product-revenue records for products with completed orders.

Automated tests: 53 passed in the latest verified test run.

These results describe the current local sample dataset and test run; they are not production-scale performance benchmarks.

## Implemented Features

- **Bronze, Silver, and Gold layers:** Store pipeline data as Parquet files.
- **Validation and quarantine:** Validate order records and save invalid rows separately.
- **Incremental processing:** Track processed order IDs to avoid reprocessing existing records.
- **DuckDB analytics:** Aggregate completed-order revenue by product.
- **Quality reports:** Generate JSON summaries of pipeline data checks.
- **Pipeline run history:** Record run IDs, timestamps, metrics, statuses, and errors.
- **Logging:** Capture pipeline events and exceptions for troubleshooting.
- **Docker Compose and RustFS:** Run the pipeline with local S3-compatible object storage. This setup does not use AWS cloud services.
- **boto3 uploads:** Upload Parquet outputs to the configured S3-compatible storage.
- **Automated tests and GitHub Actions:** Run the test suite locally and in CI.

## Future Improvements

- Integrate AWS S3 and AWS Athena for cloud storage and analytics; neither cloud service is currently implemented. The current setup uses local RustFS storage.
- Add Apache Airflow for workflow scheduling and orchestration.
- Add Apache Kafka for streaming data ingestion.
- Explore Apache Spark for larger-scale data processing.
- Expand monitoring with dashboards, alerts, and operational metrics.
- Introduce data lineage and schema versioning.
- Test performance and reliability with larger datasets.
- Document deployment to a cloud environment.

## Project Goal

Build practical data engineering skills by developing a reliable, testable pipeline from raw data ingestion to analytics-ready datasets.
