# Architecture

This document describes the runtime flow of the serverless ETL pipeline
defined in this repository. The intent is to keep the code, IaC and
diagrams aligned so an interviewer can read the repo top-to-bottom
and understand exactly what runs where.

## High-level flow

```
S3 raw/  ──(S3 ObjectCreated)──▶  ingest_handler (Lambda)
                                       │
                                       ▼
                              S3 staging/dt=YYYY-MM-DD/...
                                       │
                                       ▼
                            Step Functions: etl_pipeline
        ┌───────────────────────┬──────────────────┬──────────────────────┐
        ▼                       ▼                  ▼                      ▼
 transform_handler        dq_handler         load_handler (DDB)   glue_trigger ─▶ analytics_load (Glue)
        │                       │                  │                      │
        ▼                       ▼                  ▼                      ▼
 S3 processed/...       PASS / FAIL choice   DynamoDB hot table     S3 curated/ Parquet
                                |
                                ▼ (on FAIL)
                          SNS failure topic
```

## Components

| Layer            | Service                     | Purpose                                                                 |
|------------------|-----------------------------|-------------------------------------------------------------------------|
| Storage          | S3 (raw / processed / curated) | Three-zone data lake. `raw/` transitions to Glacier after 90 days.   |
| Ingestion        | Lambda `ingest_handler`     | Validates incoming JSON / NDJSON and lands NDJSON to `staging/`.        |
| Orchestration    | Step Functions              | Single state machine wires the steps together with retries + catches.   |
| Transformation   | Lambda `transform_handler`  | Lightweight per-record normalization + dedupe.                          |
| Data quality     | Lambda `dq_handler`         | Row-count, null-rate, and freshness checks. Drives the SF Choice state. |
| Hot serving      | Lambda `load_handler` + DynamoDB | BatchWriter pattern for low-latency lookups.                        |
| Analytics load   | Lambda `glue_trigger` + Glue `analytics_load` | PySpark job writes partitioned Parquet to `curated/`.   |
| Notifications    | SNS                         | Single failure topic published from the state machine.                  |

## Why split transform from Glue?

The transform Lambda exists for the common case where the per-batch
volume is small (hundreds of MB or fewer): Lambda is cheaper and has
no cold-start spin-up for Spark drivers. The Glue job handles the
heavier curated load (joins, partitioned Parquet, Redshift ingest).

## State machine error handling

* Every Lambda task has a `Retry` block for transient AWS errors.
* A `Catch` redirects fatal errors to the `NotifyFailure` step, which
  publishes a JSON-stringified state to the configured SNS topic.
* The DQ step uses a `Choice` so soft-failures (data issues, not code
  errors) are routed to the same notification path.

## Build & deploy

1. `./package_lambdas.sh` builds zip artifacts under `build/`.
2. The analytics Glue script must be uploaded to
   `s3://<processed-bucket>/scripts/analytics_load.py` before
   `terraform apply` (the CI deploy workflow currently runs `plan`
   only — see `.github/workflows/deploy.yml`).
3. `cd infrastructure/terraform && terraform init -backend-config=...`
4. `terraform plan -var-file=environments/dev/terraform.tfvars`

> Baanu must validate against a real AWS account before merging. The
> CI workflow runs `plan` against placeholders only and does **not**
> apply.
