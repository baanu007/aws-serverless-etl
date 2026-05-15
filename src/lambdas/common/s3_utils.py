"""Lightweight helpers for S3 paths and partitioning.

These functions intentionally avoid third-party deps so they can be
reused from ingest/transform/dq handlers without inflating zip size.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Tuple
from urllib.parse import unquote_plus


def parse_s3_event_record(record: dict) -> Tuple[str, str]:
    """Extract (bucket, key) from a single S3 event record.

    S3 keys can be URL-encoded by the event source, so they are decoded
    with `unquote_plus` to match the actual object key.
    """
    bucket = record["s3"]["bucket"]["name"]
    key = unquote_plus(record["s3"]["object"]["key"])
    return bucket, key


def build_partitioned_key(
    prefix: str,
    source_name: str,
    filename: str,
    ts: datetime | None = None,
) -> str:
    """Build a Hive-style partitioned S3 key: prefix/source=.../dt=YYYY-MM-DD/file."""
    if ts is None:
        ts = datetime.now(tz=timezone.utc)
    dt = ts.strftime("%Y-%m-%d")
    return f"{prefix.rstrip('/')}/source={source_name}/dt={dt}/{filename}"


def utc_now_iso() -> str:
    """Return an ISO-8601 UTC timestamp (seconds resolution)."""
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()
