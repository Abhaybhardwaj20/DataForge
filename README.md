
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
├── config/
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── docs/
├── logs/
├── reports/
├── sql/
├── src/
│   └── dataforge/
│       ├── cli.py
│       ├── ingest.py
│       ├── publish.py
│       ├── quality.py
│       ├── transform.py
│       └── validate.py
├── tests/
├── .gitignore
├── README.md
└── requirements.txt
```

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

From the project root, with the virtual environment activated:

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

The sample dataset currently contains 8 order records.

The latest verified quality report showed:

- Raw rows: 8
- Silver rows: 8
- Rows removed: 0
- Unique orders: 8
- Unique customers: 6
- Unique products: 5
- Missing Silver values: 0
- Duplicate Silver order IDs: 0

The Gold dataset contains 4 product-revenue records for products with completed orders.

Automated tests: 14 passed in the latest verified test run.

These results describe the current local sample dataset and test run; they are not production-scale performance benchmarks.

## Future Improvements

- Incremental ingestion and idempotent reruns
- Stronger schema and business-rule validation
- Quarantine storage for rejected records
- More comprehensive integration tests
- GitHub Actions CI
- AWS S3 storage and Athena analytics
- Pipeline metrics and monitoring
- Documentation and architecture diagrams

## Project Goal

Build practical data engineering skills by developing a reliable, testable pipeline from raw data ingestion to analytics-ready datasets.
