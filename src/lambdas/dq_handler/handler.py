"""Data Quality Lambda.

Runs lightweight data quality checks on the NDJSON file produced by
``transform_handler``. The handler reports a pass/fail status back to
Step Functions so the state machine can branch into either the load
step or an SNS notification path.

Checks implemented
------------------
* ``min_row_count`` — at least this many records must be present.
* ``max_null_rate`` — for each configured critical field, the fraction
  of null/missing values must not exceed this rate.
* ``freshness_minutes`` — the newest ``timestamp`` field value (if
  present) must be within this many minutes of "now". If no timestamp
  exists the check is skipped.

Return shape (consumed by Step Functions Choice state)::

    {"status": "PASS", "checks": [...], "summary": {...}}
    {"status": "FAIL", "checks": [...], "summary": {...}, "failures": [...]}
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import boto3

try:  # pragma: no cover - import shim
    from common.logging_utils import get_logger, log_event
except ImportError:  # pragma: no cover - import shim
    from src.lambdas.common.logging_utils import get_logger, log_event


LOGGER = get_logger(__name__)
_s3 = boto3.client("s3")

# Defaults can be overridden per-invocation via the event payload.
DEFAULT_MIN_ROWS = int(os.environ.get("DQ_MIN_ROWS", "1"))
DEFAULT_MAX_NULL_RATE = float(os.environ.get("DQ_MAX_NULL_RATE", "0.05"))
DEFAULT_FRESHNESS_MIN = int(os.environ.get("DQ_FRESHNESS_MINUTES", "1440"))


def _read_ndjson(bucket: str, key: str) -> List[Dict[str, Any]]:
    body = _s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    return [json.loads(line) for line in body.splitlines() if line.strip()]


def _null_rate(records: List[Dict[str, Any]], field: str) -> float:
    if not records:
        return 1.0
    nulls = sum(1 for r in records if r.get(field) in (None, "", []))
    return nulls / len(records)


def _check_min_rows(records: List[Dict[str, Any]], min_rows: int) -> Dict[str, Any]:
    passed = len(records) >= min_rows
    return {
        "name": "min_row_count",
        "passed": passed,
        "observed": len(records),
        "threshold": min_rows,
    }


def _check_null_rates(
    records: List[Dict[str, Any]], fields: List[str], max_rate: float
) -> List[Dict[str, Any]]:
    results = []
    for field in fields:
        rate = _null_rate(records, field)
        results.append(
            {
                "name": f"null_rate.{field}",
                "passed": rate <= max_rate,
                "observed": round(rate, 4),
                "threshold": max_rate,
            }
        )
    return results


def _check_freshness(
    records: List[Dict[str, Any]],
    field: str,
    max_minutes: int,
) -> Dict[str, Any] | None:
    candidates = [r.get(field) for r in records if r.get(field)]
    if not candidates:
        return None
    parsed = []
    for value in candidates:
        try:
            parsed.append(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
        except ValueError:
            continue
    if not parsed:
        return None
    newest = max(parsed)
    if newest.tzinfo is None:
        newest = newest.replace(tzinfo=timezone.utc)
    age = datetime.now(tz=timezone.utc) - newest
    passed = age <= timedelta(minutes=max_minutes)
    return {
        "name": "freshness",
        "passed": passed,
        "observed_minutes": round(age.total_seconds() / 60, 2),
        "threshold_minutes": max_minutes,
        "newest_timestamp": newest.isoformat(),
    }


def run_checks(records: List[Dict[str, Any]], config: Dict[str, Any]) -> Dict[str, Any]:
    """Run all configured checks against the in-memory record list.

    Exposed as a top-level function so unit tests can drive it without
    touching S3.
    """
    checks: List[Dict[str, Any]] = []
    checks.append(_check_min_rows(records, config.get("min_rows", DEFAULT_MIN_ROWS)))
    checks.extend(
        _check_null_rates(
            records,
            config.get("critical_fields", ["id"]),
            config.get("max_null_rate", DEFAULT_MAX_NULL_RATE),
        )
    )
    freshness = _check_freshness(
        records,
        config.get("freshness_field", "timestamp"),
        config.get("freshness_minutes", DEFAULT_FRESHNESS_MIN),
    )
    if freshness is not None:
        checks.append(freshness)

    failures = [c for c in checks if not c["passed"]]
    return {
        "status": "PASS" if not failures else "FAIL",
        "checks": checks,
        "summary": {
            "total_checks": len(checks),
            "failed_checks": len(failures),
            "row_count": len(records),
        },
        "failures": failures,
    }


def handler(event: Dict[str, Any], context: Any = None) -> Dict[str, Any]:
    """Lambda entry point.

    Expected event::

        {
          "processed_bucket": "...",
          "processed_key": "processed/source=foo/dt=2026-05-15/foo.ndjson",
          "config": {"min_rows": 10, "critical_fields": ["id", "amount"]}
        }
    """
    log_event(LOGGER, event)
    bucket = event["processed_bucket"]
    key = event["processed_key"]
    config = event.get("config", {})

    records = _read_ndjson(bucket, key)
    result = run_checks(records, config)
    # Pass through the input pointers so the next state can reuse them.
    result["processed_bucket"] = bucket
    result["processed_key"] = key
    return result
