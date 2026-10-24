import sys
from pathlib import Path

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "dataforge"))

import storage


class FakeS3Client:
    def __init__(self, error=None):
        self.uploads = []
        self.error = error

    def upload_file(self, filename, bucket, key):
        self.uploads.append((filename, bucket, key))
        if self.error:
            raise self.error


def output_paths(tmp_path):
    paths = {
        "bronze": tmp_path / "bronze" / "orders.parquet",
        "silver": tmp_path / "silver" / "orders.parquet",
        "gold": tmp_path / "gold" / "product_revenue.parquet",
        "quarantine": tmp_path / "quarantine" / "invalid_orders.parquet",
    }
    for path in paths.values():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"parquet test data")
    return paths


def upload_outputs(paths):
    return storage.upload_pipeline_outputs(
        paths["bronze"],
        paths["silver"],
        paths["gold"],
        paths["quarantine"],
        bucket="dataforge-learning-bucket",
        endpoint_url="http://127.0.0.1:9000",
    )


def test_upload_uses_expected_bucket_keys_and_local_files(
    monkeypatch, tmp_path
):
    paths = output_paths(tmp_path)
    client = FakeS3Client()
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "local-access")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "local-secret")
    monkeypatch.setattr(storage, "create_s3_client", lambda _: client)

    upload_outputs(paths)

    assert client.uploads == [
        (str(paths["bronze"]), "dataforge-learning-bucket", "bronze/orders.parquet"),
        (str(paths["silver"]), "dataforge-learning-bucket", "silver/orders.parquet"),
        (str(paths["gold"]), "dataforge-learning-bucket", "gold/product_revenue.parquet"),
        (str(paths["quarantine"]), "dataforge-learning-bucket", "quarantine/invalid_orders.parquet"),
    ]


def test_repeated_uploads_reuse_the_same_object_keys(monkeypatch, tmp_path):
    paths = output_paths(tmp_path)
    client = FakeS3Client()
    monkeypatch.setattr(storage, "create_s3_client", lambda _: client)

    upload_outputs(paths)
    upload_outputs(paths)

    first_keys = [upload[2] for upload in client.uploads[:4]]
    second_keys = [upload[2] for upload in client.uploads[4:]]
    assert first_keys == second_keys
    assert len(client.uploads) == 8


def test_missing_output_fails_before_client_creation(monkeypatch, tmp_path):
    paths = output_paths(tmp_path)
    paths["gold"].unlink()
    monkeypatch.setattr(
        storage,
        "create_s3_client",
        lambda _: pytest.fail("S3 client should not be created"),
    )

    with pytest.raises(FileNotFoundError, match="gold.*product_revenue"):
        upload_outputs(paths)


def test_empty_bucket_fails_before_client_creation(monkeypatch, tmp_path):
    paths = output_paths(tmp_path)
    monkeypatch.setattr(
        storage,
        "create_s3_client",
        lambda _: pytest.fail("S3 client should not be created"),
    )

    with pytest.raises(storage.StorageUploadError, match="bucket"):
        storage.upload_pipeline_outputs(
            paths["bronze"],
            paths["silver"],
            paths["gold"],
            paths["quarantine"],
            bucket="",
            endpoint_url="http://127.0.0.1:9000",
        )


def test_missing_credentials_fail_without_client_creation(
    monkeypatch, tmp_path
):
    paths = output_paths(tmp_path)
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.setattr(
        storage.boto3,
        "client",
        lambda *args, **kwargs: pytest.fail(
            "boto3 client should not be created"
        ),
    )

    with pytest.raises(storage.StorageUploadError, match="AWS_ACCESS_KEY_ID"):
        upload_outputs(paths)


def test_client_uses_path_style_region_and_bounded_retries(monkeypatch):
    captured = {}

    def fake_client(service, **kwargs):
        captured["service"] = service
        captured.update(kwargs)
        return object()

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "local-access")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "local-secret")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-south-1")
    monkeypatch.setattr(storage.boto3, "client", fake_client)

    storage.create_s3_client("http://object-storage:9000")

    assert captured["service"] == "s3"
    assert captured["endpoint_url"] == "http://object-storage:9000"
    assert captured["region_name"] == "ap-south-1"
    assert captured["config"].s3["addressing_style"] == "path"
    assert captured["config"].retries["total_max_attempts"] == 3


def test_client_construction_failure_is_wrapped(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "local-access")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "local-secret")
    monkeypatch.setattr(
        storage.boto3,
        "client",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            EndpointConnectionError(endpoint_url="http://unavailable")
        ),
    )

    with pytest.raises(
        storage.StorageUploadError,
        match="Could not initialize S3 client",
    ):
        storage.create_s3_client("http://unavailable")


@pytest.mark.parametrize(
    "error",
    [
        EndpointConnectionError(endpoint_url="http://unavailable"),
        ClientError(
            {"Error": {"Code": "NoSuchBucket", "Message": "missing"}},
            "PutObject",
        ),
    ],
)
def test_client_failures_raise_clear_upload_error(
    monkeypatch, tmp_path, error
):
    paths = output_paths(tmp_path)
    monkeypatch.setattr(
        storage, "create_s3_client", lambda _: FakeS3Client(error)
    )

    with pytest.raises(
        storage.StorageUploadError,
        match="s3://dataforge-learning-bucket",
    ):
        upload_outputs(paths)