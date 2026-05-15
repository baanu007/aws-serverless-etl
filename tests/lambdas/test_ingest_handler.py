"""Tests for the ingest Lambda handler."""

from __future__ import annotations

import json
import importlib

import pytest


@pytest.fixture
def ingest_module(monkeypatch, s3_buckets):
    """Import the handler module after env vars are set."""
    monkeypatch.setenv("STAGING_BUCKET", s3_buckets["processed"])
    monkeypatch.setenv("STAGING_PREFIX", "staging")
    monkeypatch.setenv("REQUIRED_FIELDS", "id")
    # Reload so module-level env reads pick up the new values.
    import ingest_handler.handler as handler  # type: ignore
    importlib.reload(handler)
    return handler


def _put_raw_object(client, bucket, key, body):
    client.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))


def test_handler_processes_json_array(ingest_module, s3_buckets):
    client = s3_buckets["client"]
    records = [{"id": "1", "value": 10}, {"id": "2", "value": 20}]
    _put_raw_object(
        client, s3_buckets["raw"], "raw/orders/sample.json", json.dumps(records)
    )

    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": s3_buckets["raw"]},
                    "object": {"key": "raw/orders/sample.json"},
                }
            }
        ]
    }

    result = ingest_module.handler(event)

    assert result["status"] == "OK"
    assert result["processed_objects"] == 1
    assert result["total_valid_records"] == 2

    out_key = result["results"][0]["staging_key"]
    written = client.get_object(Bucket=s3_buckets["processed"], Key=out_key)
    lines = written["Body"].read().decode("utf-8").splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["id"] == "1"


def test_handler_validates_records(ingest_module, s3_buckets):
    client = s3_buckets["client"]
    records = [{"id": "1"}, {"no_id": True}, {"id": "2"}]
    _put_raw_object(
        client, s3_buckets["raw"], "raw/orders/mixed.json", json.dumps(records)
    )

    result = ingest_module.handler(
        {"bucket": s3_buckets["raw"], "key": "raw/orders/mixed.json"}
    )

    summary = result["results"][0]
    assert summary["valid_records"] == 2
    assert summary["invalid_records"] == 1
    assert summary["invalid_samples"][0]["reason"].startswith("missing required field")


def test_handler_accepts_ndjson(ingest_module, s3_buckets):
    client = s3_buckets["client"]
    ndjson = "\n".join(
        json.dumps({"id": str(i), "value": i}) for i in range(3)
    )
    _put_raw_object(client, s3_buckets["raw"], "raw/orders/stream.ndjson", ndjson)

    result = ingest_module.handler(
        {"bucket": s3_buckets["raw"], "key": "raw/orders/stream.ndjson"}
    )

    assert result["total_valid_records"] == 3


def test_handler_rejects_invalid_event(ingest_module):
    with pytest.raises(ValueError):
        ingest_module.handler({"not_a_record": True})
