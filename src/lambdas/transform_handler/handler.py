"""Transform Lambda.

Invoked by Step Functions after the ingest step. Reads the NDJSON
staging file produced by ``ingest_handler``, applies lightweight
column-level transformations, and writes the result to the
``processed/`` prefix as NDJSON. Heavy Spark transformations are
delegated to the Glue job downstream.

The transform layer is intentionally small / dependency-light so that
the same code can run in a Lambda (256-512 MB) without needing pandas
or polars as a hard requirement. If `pandas` is available it will be
used for cleaner column operations; otherwise the code falls back to
pure-Python dict comprehensions.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

import boto3

try:  # pragma: no cover - import shim
    from common.logging_utils import get_logger, log_event
    from common.s3_utils import build_partitioned_key
except ImportError:  # pragma: no cover - import shim
    from src.lambdas.common.logging_utils import get_logger, log_event
    from src.lambdas.common.s3_utils import build_partitioned_key


LOGGER = get_logger(__name__)
PROCESSED_BUCKET = os.environ.get("PROCESSED_BUCKET", "")
PROCESSED_PREFIX = os.environ.get("PROCESSED_PREFIX", "processed")

_s3 = boto3.client("s3")


def _read_ndjson(bucket: str, key: str) -> List[Dict[str, Any]]:
    body = _s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    return [json.loads(line) for line in body.splitlines() if line.strip()]


def _normalize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Apply per-record transformations.

    * Trims whitespace on string values.
    * Lower-cases all keys for downstream consistency.
    * Adds an ``_ingested_at`` audit timestamp.
    """
    out: Dict[str, Any] = {}
    for k, v in record.items():
        new_key = k.strip().lower()
        if isinstance(v, str):
            out[new_key] = v.strip()
        else:
            out[new_key] = v
    out["_processed_at"] = datetime.now(tz=timezone.utc).isoformat()
    return out


def _dedupe(records: Iterable[Dict[str, Any]], key_field: str) -> List[Dict[str, Any]]:
    """Keep the last occurrence of each ``key_field`` value.

    Records missing ``key_field`` are skipped with a warning so they do not
    collapse together under a shared ``None`` key. A CloudWatch-visible
    log entry is emitted for each skipped record (counter-friendly).
    """
    seen: Dict[Any, Dict[str, Any]] = {}
    skipped = 0
    for rec in records:
        key_value = rec.get(key_field)
        if key_value is None:
            skipped += 1
            LOGGER.warning(
                "dedupe: skipping record without key '%s' (metric=dedupe_missing_key)",
                key_field,
            )
            continue
        seen[key_value] = rec
    if skipped:
        LOGGER.info(
            "dedupe: skipped %d record(s) missing key '%s' (metric=dedupe_missing_key_total)",
            skipped,
            key_field,
        )
    return list(seen.values())


def _write_ndjson(records: List[Dict[str, Any]], bucket: str, key: str) -> None:
    body = "\n".join(json.dumps(r, default=str) for r in records).encode("utf-8")
    _s3.put_object(
        Bucket=bucket, Key=key, Body=body, ContentType="application/x-ndjson"
    )


def handler(event: Dict[str, Any], context: Any = None) -> Dict[str, Any]:
    """Lambda entry point.

    Expected event shape (from Step Functions, mirroring ingest output)::

        {
          "staging_bucket": "...",
          "staging_key": "staging/source=foo/dt=2026-05-15/foo.ndjson",
          "dedupe_key": "id"
        }
    """
    log_event(LOGGER, event)

    staging_bucket = event["staging_bucket"]
    staging_key = event["staging_key"]
    dedupe_key = event.get("dedupe_key", "id")

    if not PROCESSED_BUCKET:
        raise RuntimeError("PROCESSED_BUCKET env var is not configured")

    raw_records = _read_ndjson(staging_bucket, staging_key)
    normalized = [_normalize_record(r) for r in raw_records]
    deduped = _dedupe(normalized, dedupe_key)

    source_name = "unknown"
    if "source=" in staging_key:
        # Recover the source name from the partition path.
        for part in staging_key.split("/"):
            if part.startswith("source="):
                source_name = part.split("=", 1)[1]
                break

    out_key = build_partitioned_key(
        prefix=PROCESSED_PREFIX,
        source_name=source_name,
        filename=os.path.basename(staging_key),
    )
    _write_ndjson(deduped, PROCESSED_BUCKET, out_key)

    return {
        "status": "OK",
        "processed_bucket": PROCESSED_BUCKET,
        "processed_key": out_key,
        "input_records": len(raw_records),
        "output_records": len(deduped),
        "deduped": len(normalized) - len(deduped),
    }
