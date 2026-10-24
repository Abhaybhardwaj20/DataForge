import os
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


class StorageUploadError(RuntimeError):
    """Raised when a configured S3 output cannot be uploaded."""


def create_s3_client(endpoint_url: str):
    """Create a RustFS-compatible S3 client using local environment credentials."""
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    if not access_key or not secret_key:
        raise StorageUploadError(
            "S3 uploads require AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
        )

    if not endpoint_url:
        raise StorageUploadError("S3 endpoint_url must not be empty")

    try:
        return boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=os.getenv("AWS_DEFAULT_REGION") or "us-east-1",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(
                s3={"addressing_style": "path"},
                retries={"mode": "standard", "total_max_attempts": 3},
                connect_timeout=5,
                read_timeout=30,
            ),
        )
    except BotoCoreError as exc:
        raise StorageUploadError(
            f"Could not initialize S3 client for endpoint {endpoint_url}"
        ) from exc


def upload_pipeline_outputs(
    bronze_path: str | Path,
    silver_path: str | Path,
    gold_path: str | Path,
    quarantine_path: str | Path,
    *,
    bucket: str,
    endpoint_url: str,
) -> None:
    """Upload completed local Parquet outputs under stable object keys."""
    outputs = [
        (Path(bronze_path), "bronze/orders.parquet"),
        (Path(silver_path), "silver/orders.parquet"),
        (Path(gold_path), "gold/product_revenue.parquet"),
        (Path(quarantine_path), "quarantine/invalid_orders.parquet"),
    ]

    if not bucket:
        raise StorageUploadError("S3 bucket must not be empty")

    missing_paths = [str(path) for path, _ in outputs if not path.is_file()]
    if missing_paths:
        raise FileNotFoundError(
            "Required Parquet output is missing: " + ", ".join(missing_paths)
        )

    client = create_s3_client(endpoint_url)

    for local_path, object_key in outputs:
        try:
            client.upload_file(str(local_path), bucket, object_key)
        except (BotoCoreError, ClientError) as exc:
            raise StorageUploadError(
                f"Failed to upload {local_path} to s3://{bucket}/{object_key}"
            ) from exc