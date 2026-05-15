"""Tests for the transform Lambda handler."""

from __future__ import annotations

import importlib
import json

import pytest


@pytest.fixture
def transform_module(monkeypatch, s3_buckets):
    monkeypatch.setenv("PROCESSED_BUCKET", s3_buckets["processed"])
    monkeypatch.setenv("PROCESSED_PREFIX", "processed")
    import transform_handler.handler as handler  # type: ignore
    importlib.reload(handler)
    return handler


def test_normalize_record_lowercases_and_trims(transform_module):
    out = transform_module._normalize_record({"ID": " 123 ", "Name": " Alice "})
    assert out["id"] == "123"
    assert out["name"] == "Alice"
    assert "_processed_at" in out


def test_dedupe_keeps_last_occurrence(transform_module):
    records = [
        {"id": "1", "v": 1},
        {"id": "2", "v": 2},
        {"id": "1", "v": 99},
    ]
    deduped = transform_module._dedupe(records, "id")
    by_id = {r["id"]: r["v"] for r in deduped}
    assert by_id == {"1": 99, "2": 2}


def test_handler_end_to_end(transform_module, s3_buckets):
    client = s3_buckets["client"]
    staging_key = "staging/source=orders/dt=2026-05-15/sample.ndjson"
    body = "\n".join(
        json.dumps({"ID": str(i), "AMOUNT": i * 10}) for i in [1, 2, 1]
    )
    client.put_object(
        Bucket=s3_buckets["processed"], Key=staging_key, Body=body.encode("utf-8")
    )

    result = transform_module.handler(
        {
            "staging_bucket": s3_buckets["processed"],
            "staging_key": staging_key,
            "dedupe_key": "id",
        }
    )

    assert result["status"] == "OK"
    assert result["input_records"] == 3
    assert result["output_records"] == 2

    written = client.get_object(
        Bucket=s3_buckets["processed"], Key=result["processed_key"]
    )["Body"].read().decode("utf-8")
    rows = [json.loads(line) for line in written.splitlines()]
    assert {r["id"] for r in rows} == {"1", "2"}
