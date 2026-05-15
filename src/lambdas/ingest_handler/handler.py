"""Ingest Lambda.

Triggered by an S3 PUT under the configured `RAW_PREFIX` (e.g. ``raw/``).
For each record the handler:

1. Reads the JSON or NDJSON file from the source bucket.
2. Validates each record against the minimal expected schema.
3. Writes a normalized NDJSON file to ``STAGING_PREFIX`` partitioned by ``dt=YYYY-MM-DD``.

The handler returns a structured payload describing the work so it can be
chained into Step Functions if desired.

Design notes
------------
* Pure-Python: no pandas / pyarrow dependency, which keeps cold start fast
  and zip size small.
* The schema validator is intentionally permissive — strict business rules
  belong in the DQ handler, not the ingest step.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Tuple

import boto3

# Local imports work both when packaged in zip (handler at root) and when
# imported via the `src.lambdas.ingest_handler` path during unit tests.
try:  # pragma: no cover - import shim
    from common.logging_utils import get_logger, log_event
    from common.s3_utils import build_partitioned_key, parse_s3_event_record
except ImportError:  # pragma: no cover - import shim
    from src.lambdas.common.logging_utils import get_logger, log_event
    from src.lambdas.common.s3_utils import build_partitioned_key, parse_s3_event_record


LOGGER = get_logger(__name__)

STAGING_BUCKET = os.environ.get("STAGING_BUCKET", "")
STAGING_PREFIX = os.environ.get("STAGING_PREFIX", "staging")
REQUIRED_FIELDS = [
    f.strip() for f in os.environ.get("REQUIRED_FIELDS", "id").split(",") if f.strip()
]

_s3 = boto3.client("s3")


def _read_object(bucket: str, key: str) -> bytes:
    response = _s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def _iter_records(raw_bytes: bytes) -> Iterable[Dict[str, Any]]:
    """Yield records from either a JSON array, single object, or NDJSON file."""
    text = raw_bytes.decode("utf-8").strip()
    if not text:
        return []
    # Try a single JSON document first.
    try:
        doc = json.loads(text)
    except json.JSONDecodeError:
        # Treat as NDJSON.
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    if isinstance(doc, list):
        return doc
    if isinstance(doc, dict):
        # API responses often wrap records under a "data" or "results" key.
        for wrapper in ("data", "results", "items"):
            inner = doc.get(wrapper)
            if isinstance(inner, list):
                return inner
        return [doc]
    raise ValueError(f"Unsupported JSON root type: {type(doc).__name__}")


def _validate(record: Dict[str, Any]) -> Tuple[bool, str]:
    if not isinstance(record, dict):
        return False, "record is not a JSON object"
    for field in REQUIRED_FIELDS:
        if field not in record:
            return False, f"missing required field '{field}'"
    return True, ""


def _process_object(bucket: str, key: str) -> Dict[str, Any]:
    LOGGER.info("processing s3://%s/%s", bucket, key)
    raw = _read_object(bucket, key)
    records = list(_iter_records(raw))

    valid: List[Dict[str, Any]] = []
    invalid: List[Dict[str, Any]] = []
    for rec in records:
        ok, reason = _validate(rec)
        if ok:
            valid.append(rec)
        else:
            invalid.append({"reason": reason, "record": rec})

    if not STAGING_BUCKET:
        raise RuntimeError("STAGING_BUCKET env var is not configured")

    source_name = key.split("/", 2)[1] if "/" in key else "unknown"
    filename = os.path.basename(key).rsplit(".", 1)[0] + ".ndjson"
    out_key = build_partitioned_key(
        prefix=STAGING_PREFIX,
        source_name=source_name,
        filename=filename,
        ts=datetime.now(tz=timezone.utc),
    )

    body = "\n".join(json.dumps(r, default=str) for r in valid).encode("utf-8")
    _s3.put_object(
        Bucket=STAGING_BUCKET,
        Key=out_key,
        Body=body,
        ContentType="application/x-ndjson",
    )

    return {
        "source_bucket": bucket,
        "source_key": key,
        "staging_bucket": STAGING_BUCKET,
        "staging_key": out_key,
        "total_records": len(records),
        "valid_records": len(valid),
        "invalid_records": len(invalid),
        "invalid_samples": invalid[:5],
    }


def handler(event: Dict[str, Any], context: Any = None) -> Dict[str, Any]:
    """Lambda entry point.

    Accepts either a native S3 event (``Records`` array) or a simple
    ``{"bucket": "...", "key": "..."}`` payload that Step Functions can pass.
    """
    log_event(LOGGER, event)
    results: List[Dict[str, Any]] = []

    if "Records" in event:
        for record in event["Records"]:
            bucket, key = parse_s3_event_record(record)
            results.append(_process_object(bucket, key))
    elif "bucket" in event and "key" in event:
        results.append(_process_object(event["bucket"], event["key"]))
    else:
        raise ValueError("event must contain S3 Records or bucket/key fields")

    try:
        total_valid = sum(r["valid_records"] for r in results)
    except (KeyError, TypeError):
        total_valid = 0

    return {
        "status": "OK",
        "processed_objects": len(results),
        "total_valid_records": total_valid,
        "results": results,
    }


if __name__ == "__main__":  # pragma: no cover - manual smoke test
    import sys

    if len(sys.argv) >= 3:
        print(json.dumps(handler({"bucket": sys.argv[1], "key": sys.argv[2]}), indent=2))
    else:
        print("Usage: python handler.py <bucket> <key>")
