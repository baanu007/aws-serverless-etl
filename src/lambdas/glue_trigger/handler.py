"""Glue Trigger Lambda.

A thin wrapper around ``glue:StartJobRun`` so that the Step Functions
state machine can invoke a Glue job and receive back the ``JobRunId``
for downstream polling / waiting.

This is split out from ``load_handler`` so that "fire-and-forget"
analytics flows can run independently of the DynamoDB hot load.
"""

from __future__ import annotations

import os
from typing import Any, Dict

import boto3

try:  # pragma: no cover - import shim
    from common.logging_utils import get_logger, log_event
except ImportError:  # pragma: no cover - import shim
    from src.lambdas.common.logging_utils import get_logger, log_event


LOGGER = get_logger(__name__)
DEFAULT_JOB_NAME = os.environ.get("GLUE_JOB_NAME", "")
_glue = boto3.client("glue")


def handler(event: Dict[str, Any], context: Any = None) -> Dict[str, Any]:
    """Lambda entry point.

    Expected event::

        {
          "job_name": "optional-override",
          "arguments": {"--source_bucket": "...", "--source_key": "..."}
        }
    """
    log_event(LOGGER, event)
    job_name = event.get("job_name") or DEFAULT_JOB_NAME
    if not job_name:
        raise RuntimeError(
            "Glue job name must be provided via event.job_name or GLUE_JOB_NAME env var"
        )

    arguments = event.get("arguments", {}) or {}
    response = _glue.start_job_run(JobName=job_name, Arguments=arguments)
    job_run_id = response["JobRunId"]
    LOGGER.info("started glue job %s run %s", job_name, job_run_id)

    return {
        "status": "STARTED",
        "job_name": job_name,
        "job_run_id": job_run_id,
    }
