"""Tests for the data-quality Lambda handler."""

from __future__ import annotations

import importlib
import json
from datetime import datetime, timedelta, timezone

import pytest


@pytest.fixture
def dq_module():
    import dq_handler.handler as handler  # type: ignore
    importlib.reload(handler)
    return handler


def test_min_rows_pass(dq_module):
    records = [{"id": "1"}, {"id": "2"}]
    result = dq_module.run_checks(records, {"min_rows": 1, "critical_fields": ["id"]})
    assert result["status"] == "PASS"


def test_min_rows_fail(dq_module):
    result = dq_module.run_checks([], {"min_rows": 1, "critical_fields": ["id"]})
    assert result["status"] == "FAIL"
    names = [c["name"] for c in result["failures"]]
    assert "min_row_count" in names


def test_null_rate_fail(dq_module):
    records = [{"id": "1", "amount": None}, {"id": "2", "amount": None}, {"id": "3", "amount": 10}]
    result = dq_module.run_checks(
        records,
        {"min_rows": 1, "critical_fields": ["amount"], "max_null_rate": 0.1},
    )
    assert result["status"] == "FAIL"
    failure = next(c for c in result["failures"] if c["name"] == "null_rate.amount")
    assert failure["observed"] > 0.1


def test_freshness_check_skipped_when_no_timestamps(dq_module):
    records = [{"id": "1"}, {"id": "2"}]
    result = dq_module.run_checks(records, {"min_rows": 1, "critical_fields": ["id"]})
    # No freshness check should have been added.
    assert all(c["name"] != "freshness" for c in result["checks"])


def test_freshness_pass(dq_module):
    recent = datetime.now(tz=timezone.utc).isoformat()
    records = [{"id": "1", "timestamp": recent}]
    result = dq_module.run_checks(
        records,
        {"min_rows": 1, "critical_fields": ["id"], "freshness_minutes": 60},
    )
    assert result["status"] == "PASS"


def test_freshness_fail(dq_module):
    stale = (datetime.now(tz=timezone.utc) - timedelta(days=7)).isoformat()
    records = [{"id": "1", "timestamp": stale}]
    result = dq_module.run_checks(
        records,
        {"min_rows": 1, "critical_fields": ["id"], "freshness_minutes": 60},
    )
    assert result["status"] == "FAIL"


def test_handler_reads_from_s3(dq_module, s3_buckets):
    client = s3_buckets["client"]
    key = "processed/source=orders/dt=2026-05-15/foo.ndjson"
    body = "\n".join(json.dumps({"id": str(i)}) for i in range(3))
    client.put_object(Bucket=s3_buckets["processed"], Key=key, Body=body.encode("utf-8"))

    result = dq_module.handler(
        {
            "processed_bucket": s3_buckets["processed"],
            "processed_key": key,
            "config": {"min_rows": 1, "critical_fields": ["id"]},
        }
    )
    assert result["status"] == "PASS"
    assert result["summary"]["row_count"] == 3
