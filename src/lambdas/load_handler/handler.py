"""Load Lambda.

Loads the post-DQ processed NDJSON file into a DynamoDB table for hot
lookups, then triggers a Glue job for the heavier analytics load.

DynamoDB writes use the ``batch_writer`` context manager so that
unprocessed items are automatically retried with exponential backoff.

The handler returns both the DynamoDB summary and the Glue ``JobRunId``
so the Step Functions state machine can chain a "Wait + DescribeJobRun"
loop if desired.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List

import boto3
from botocore.exceptions import ClientError

try:  # pragma: no cover - import shim
    from common.logging_utils import get_logger, log_event
except ImportError:  # pragma: no cover - import shim
    from src.lambdas.common.logging_utils import get_logger, log_event


LOGGER = get_logger(__name__)

DDB_TABLE = os.environ.get("DDB_TABLE", "")
DDB_PRIMARY_KEY = os.environ.get("DDB_PRIMARY_KEY", "id")
GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME", "")

_s3 = boto3.client("s3")
_ddb = boto3.resource("dynamodb")
_glue = boto3.client("glue")


def _read_ndjson(bucket: str, key: str) -> List[Dict[str, Any]]:
    body = _s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    return [json.loads(line) for line in body.splitlines() if line.strip()]


def _write_to_dynamodb(records: List[Dict[str, Any]], table_name: str, pk: str) -> int:
    """Write records using BatchWriter; returns number of records attempted."""
    if not records:
        return 0
    table = _ddb.Table(table_name)
    written = 0
    with table.batch_writer(overwrite_by_pkeys=[pk]) as batch:
        for rec in records:
            if pk not in rec:
                LOGGER.warning("skipping record without pk '%s': %s", pk, rec)
                continue
            batch.put_item(Item=rec)
            written += 1
    return written


def _start_glue_job(job_name: str, arguments: Dict[str, str]) -> str:
    response = _glue.start_job_run(JobName=job_name, Arguments=arguments)
    return response["JobRunId"]


def handler(event: Dict[str, Any], context: Any = None) -> Dict[str, Any]:
    """Lambda entry point.

    Expected event::

        {
          "processed_bucket": "...",
          "processed_key": "processed/source=foo/dt=2026-05-15/foo.ndjson"
        }
    """
    log_event(LOGGER, event)
    if not DDB_TABLE:
        raise RuntimeError("DDB_TABLE env var is not configured")
    if not GLUE_JOB_NAME:
        raise RuntimeError("GLUE_JOB_NAME env var is not configured")

    bucket = event["processed_bucket"]
    key = event["processed_key"]

    records = _read_ndjson(bucket, key)
    written = _write_to_dynamodb(records, DDB_TABLE, DDB_PRIMARY_KEY)

    glue_args = {
        "--source_bucket": bucket,
        "--source_key": key,
    }
    try:
        job_run_id = _start_glue_job(GLUE_JOB_NAME, glue_args)
    except ClientError as exc:
        LOGGER.error("failed to start glue job: %s", exc)
        raise

    return {
        "status": "OK",
        "dynamodb": {
            "table": DDB_TABLE,
            "records_written": written,
            "records_total": len(records),
        },
        "glue": {
            "job_name": GLUE_JOB_NAME,
            "job_run_id": job_run_id,
        },
    }
