"""Pytest fixtures shared across the test suite.

The fixtures lean on `moto` to mock AWS services entirely in-memory so
tests run without any AWS credentials or network access.
"""

from __future__ import annotations

import sys
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

# Make the `src` tree importable as a regular package.
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC / "lambdas") not in sys.path:
    sys.path.insert(0, str(SRC / "lambdas"))


@pytest.fixture(autouse=True)
def _aws_env(monkeypatch):
    """Stub AWS env vars so boto3 never reaches a real account."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    yield


@pytest.fixture
def aws():
    """Bring up moto for all supported AWS services."""
    with mock_aws():
        yield


@pytest.fixture
def s3_buckets(aws):
    """Create raw / processed buckets and yield their names."""
    client = boto3.client("s3", region_name="us-east-1")
    raw = "test-raw-bucket"
    processed = "test-processed-bucket"
    client.create_bucket(Bucket=raw)
    client.create_bucket(Bucket=processed)
    yield {"raw": raw, "processed": processed, "client": client}


@pytest.fixture
def dynamodb_table(aws):
    """Create a DynamoDB table with `id` as the primary key."""
    resource = boto3.resource("dynamodb", region_name="us-east-1")
    table = resource.create_table(
        TableName="test-lookup-table",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    table.wait_until_exists()
    yield table
