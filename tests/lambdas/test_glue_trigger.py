"""Tests for the glue_trigger Lambda handler."""

from __future__ import annotations

import importlib
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def glue_trigger_module(monkeypatch):
    monkeypatch.setenv("GLUE_JOB_NAME", "default-job")
    import glue_trigger.handler as handler  # type: ignore
    importlib.reload(handler)
    fake_glue = MagicMock()
    fake_glue.start_job_run.return_value = {"JobRunId": "run-42"}
    monkeypatch.setattr(handler, "_glue", fake_glue)
    return handler


def test_uses_default_job_name(glue_trigger_module):
    result = glue_trigger_module.handler({})
    assert result["job_name"] == "default-job"
    assert result["job_run_id"] == "run-42"
    glue_trigger_module._glue.start_job_run.assert_called_once_with(
        JobName="default-job", Arguments={}
    )


def test_event_can_override_job_name(glue_trigger_module):
    result = glue_trigger_module.handler(
        {"job_name": "override-job", "arguments": {"--foo": "bar"}}
    )
    assert result["job_name"] == "override-job"
    glue_trigger_module._glue.start_job_run.assert_called_once_with(
        JobName="override-job", Arguments={"--foo": "bar"}
    )


def test_missing_job_name_raises(glue_trigger_module, monkeypatch):
    monkeypatch.setattr(glue_trigger_module, "DEFAULT_JOB_NAME", "")
    with pytest.raises(RuntimeError):
        glue_trigger_module.handler({})
