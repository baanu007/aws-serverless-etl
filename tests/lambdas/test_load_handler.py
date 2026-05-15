"""Tests for the load Lambda handler.

The load handler now only writes to DynamoDB; Glue orchestration lives
in Step Functions and is handled by the separate ``glue_trigger`` Lambda.
"""

from __future__ import annotations

import importlib
import json

import pytest


@pytest.fixture
def load_module(monkeypatch, s3_buckets, dynamodb_table):
    monkeypatch.setenv("DDB_TABLE", dynamodb_table.name)
    monkeypatch.setenv("DDB_PRIMARY_KEY", "id")
    import load_handler.handler as handler  # type: ignore
    importlib.reload(handler)
    return handler


def test_handler_writes_dynamodb(load_module, s3_buckets, dynamodb_table):
    client = s3_buckets["client"]
    key = "processed/source=orders/dt=2026-05-15/foo.ndjson"
    body = "\n".join(json.dumps({"id": str(i), "amount": i}) for i in range(3))
    client.put_object(Bucket=s3_buckets["processed"], Key=key, Body=body.encode("utf-8"))

    result = load_module.handler(
        {"processed_bucket": s3_buckets["processed"], "processed_key": key}
    )

    assert result["status"] == "OK"
    assert result["dynamodb"]["records_written"] == 3
    assert "glue" not in result

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
