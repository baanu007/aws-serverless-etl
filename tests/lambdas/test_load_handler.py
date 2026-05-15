"""Tests for the load Lambda handler.

Moto does not implement `glue:StartJobRun` exhaustively across all
versions, so the Glue client is patched with a stub. DynamoDB and S3
interactions use the real moto-backed services.
"""

from __future__ import annotations

import importlib
import json
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def load_module(monkeypatch, s3_buckets, dynamodb_table):
    monkeypatch.setenv("DDB_TABLE", dynamodb_table.name)
    monkeypatch.setenv("DDB_PRIMARY_KEY", "id")
    monkeypatch.setenv("GLUE_JOB_NAME", "test-analytics-load")
    import load_handler.handler as handler  # type: ignore
    importlib.reload(handler)

    fake_glue = MagicMock()
    fake_glue.start_job_run.return_value = {"JobRunId": "jr_123"}
    monkeypatch.setattr(handler, "_glue", fake_glue)
    return handler


def test_handler_writes_dynamodb_and_starts_glue(load_module, s3_buckets, dynamodb_table):
    client = s3_buckets["client"]
    key = "processed/source=orders/dt=2026-05-15/foo.ndjson"
    body = "\n".join(json.dumps({"id": str(i), "amount": i}) for i in range(3))
    client.put_object(Bucket=s3_buckets["processed"], Key=key, Body=body.encode("utf-8"))

    result = load_module.handler(
        {"processed_bucket": s3_buckets["processed"], "processed_key": key}
    )

    assert result["status"] == "OK"
    assert result["dynamodb"]["records_written"] == 3
    assert result["glue"]["job_run_id"] == "jr_123"

    items = dynamodb_table.scan()["Items"]
    assert {item["id"] for item in items} == {"0", "1", "2"}


def test_handler_skips_records_without_pk(load_module, s3_buckets):
    client = s3_buckets["client"]
    key = "processed/source=orders/dt=2026-05-15/bad.ndjson"
    body = "\n".join([json.dumps({"id": "1"}), json.dumps({"no_id": True})])
    client.put_object(Bucket=s3_buckets["processed"], Key=key, Body=body.encode("utf-8"))

    result = load_module.handler(
        {"processed_bucket": s3_buckets["processed"], "processed_key": key}
    )
    assert result["dynamodb"]["records_written"] == 1
    assert result["dynamodb"]["records_total"] == 2
