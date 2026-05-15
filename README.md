# ☁️ AWS Serverless ETL Pipeline

A serverless ETL pipeline built on **AWS Lambda**, **Step Functions**, **Glue**, **DynamoDB**, and **S3**, deployed with modular **Terraform** IaC.

![AWS Lambda](https://img.shields.io/badge/AWS%20Lambda-FF9900?style=for-the-badge&logo=aws-lambda&logoColor=white)
![Step Functions](https://img.shields.io/badge/Step%20Functions-FF4F8B?style=for-the-badge&logo=amazon-aws&logoColor=white)
![AWS Glue](https://img.shields.io/badge/AWS%20Glue-FF9900?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Amazon S3](https://img.shields.io/badge/Amazon%20S3-569A31?style=for-the-badge&logo=amazon-s3&logoColor=white)
![DynamoDB](https://img.shields.io/badge/DynamoDB-4053D6?style=for-the-badge&logo=amazon-dynamodb&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-7B42BC?style=for-the-badge&logo=terraform&logoColor=white)

## 📋 Overview

This repository implements an end-to-end serverless ETL pipeline:

- **Ingest** — Lambda triggered by S3 PUT on the `raw/` prefix; validates and lands NDJSON to `staging/dt=YYYY-MM-DD/`.
- **Transform** — Lambda invoked from Step Functions; normalizes records and dedupes by configurable key.
- **Data quality** — Lambda runs row-count / null-rate / freshness checks and returns `PASS` / `FAIL` to the state machine.
- **Load** — Lambda batch-writes to DynamoDB for hot lookups and kicks off a Glue job for the analytics load.
- **Analytics** — PySpark Glue job writes partitioned Parquet to a curated zone (optional Redshift load via Glue connection).
- **Notify** — A `FAIL` path publishes to SNS so on-call gets paged.

## 🏗️ Architecture

```
S3 raw/  ──(ObjectCreated)──▶  ingest_handler ──▶ S3 staging/
                                       │
                                       ▼
                          Step Functions: etl_pipeline
        ┌────────────┬────────────┬───────────────────┬──────────────────────┐
        ▼            ▼            ▼                   ▼                      ▼
   transform     dq_handler  load_handler        glue_trigger ─▶ analytics_load (Glue)
        │            │            │                                          │
        ▼            ▼            ▼                                          ▼
 S3 processed/   PASS/FAIL   DynamoDB hot table                       S3 curated/ Parquet
                     │
                     ▼ (FAIL)
                SNS failure topic
```

See [`docs/architecture.md`](docs/architecture.md) for the full flow.

## 📁 Project Structure

```
aws-serverless-etl/
├── src/
│   ├── lambdas/
│   │   ├── common/               # shared logging / S3 helpers
│   │   ├── ingest_handler/
│   │   ├── transform_handler/
│   │   ├── dq_handler/
│   │   ├── load_handler/
│   │   ├── glue_trigger/
│   │   └── api_ingestion/        # optional: scheduled REST API puller
│   └── glue_jobs/
│       ├── analytics_load.py     # PySpark curated/Redshift load
│       └── raw_to_processed.py   # legacy raw → processed transformation
├── infrastructure/
│   ├── state_machines/
│   │   └── etl_pipeline.asl.json
│   └── terraform/
│       ├── main.tf / variables.tf / outputs.tf / providers.tf / backend.tf
│       ├── modules/
│       │   ├── lambda/
│       │   ├── step_function/
│       │   ├── s3_bucket/
│       │   ├── dynamodb/
│       │   └── glue_job/
│       └── environments/
│           └── dev/terraform.tfvars.example
├── tests/
│   ├── conftest.py
│   └── lambdas/test_*.py         # moto-mocked unit tests
├── .github/workflows/
│   ├── ci.yml                    # pytest + flake8 + terraform fmt/validate
│   └── deploy.yml                # manual workflow_dispatch; runs `terraform plan` only
├── package_lambdas.sh            # builds zip artifacts for each Lambda
├── requirements.txt
├── requirements-dev.txt
└── LICENSE
```

## 🚀 Local development

```bash
# Install dev deps
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt

# Run the test suite (moto-mocked, no AWS credentials needed)
pytest tests/ -q

# Lint
flake8 src tests

# Build Lambda artifacts
./package_lambdas.sh
```

## ☁️ Deploy

> **Heads up:** this project has not yet been validated against a real
> AWS account end-to-end. The CI workflow runs `terraform plan` only
> against placeholders. Operators should review the plan and run
> `terraform apply` manually after wiring up real bucket names and an
> OIDC role.

```bash
# 1. Build Lambda zips
./package_lambdas.sh

# 2. Configure backend + tfvars (placeholders)
cd infrastructure/terraform
cp environments/dev/terraform.tfvars.example terraform.tfvars
cp environments/dev/backend.hcl.example backend.hcl
# ...edit both files to point at real buckets / state backend...

# 3. Init + plan
terraform init -backend-config=backend.hcl
terraform plan -var-file=terraform.tfvars
```

## 🧩 Components

### Lambda functions

| Function            | Trigger                          | Purpose                                                          |
|---------------------|----------------------------------|------------------------------------------------------------------|
| `ingest_handler`    | S3 PUT on `raw/`                 | Validates JSON / NDJSON; lands NDJSON in `staging/`.             |
| `transform_handler` | Step Functions task              | Normalizes records + dedupes by key.                             |
| `dq_handler`        | Step Functions task              | Row-count / null-rate / freshness checks. Drives Choice state.   |
| `load_handler`      | Step Functions task              | DynamoDB BatchWriter + triggers Glue analytics job.              |
| `glue_trigger`      | Step Functions task              | Thin wrapper around `glue:StartJobRun`.                          |
| `api_ingestion`     | EventBridge (optional)           | Pulls REST APIs on a schedule, lands raw JSON in S3.             |

### Glue jobs

| Job                  | Purpose                                                              |
|----------------------|----------------------------------------------------------------------|
| `analytics_load.py`  | PySpark — reads `processed/` NDJSON, writes curated Parquet (+ optional Redshift).|
| `raw_to_processed.py`| PySpark — legacy raw → processed transformation (kept for reference). |

### Terraform modules

| Module           | What it provisions                                                        |
|------------------|---------------------------------------------------------------------------|
| `s3_bucket`      | Versioned, encrypted bucket with optional Glacier lifecycle.              |
| `lambda`         | Function + IAM role + CloudWatch log group + scoped policy statements.    |
| `step_function`  | State machine + IAM role (lambda:Invoke + sns:Publish scoped).            |
| `dynamodb`       | On-demand table with PITR + SSE enabled.                                  |
| `glue_job`       | Glue job + IAM role with S3 data access scoped to declared buckets.       |

## ✅ Testing

- 19 unit tests under `tests/lambdas/` run end-to-end against moto-mocked S3, DynamoDB, and stubbed Glue / SNS clients.
- `terraform validate` runs in CI against the root module.
- `terraform fmt -check -recursive` enforces formatting.

## 💰 Cost notes

- **Lambda** — billed per invocation; the handlers in this repo are intentionally small (256–512 MB) to keep $/run low.
- **DynamoDB** — on-demand billing (`PAY_PER_REQUEST`); fine for spiky ETL writes.
- **S3** — `raw/` transitions to Glacier after 90 days via lifecycle policy.
- **Glue** — `G.1X` worker, 2 DPU default; size up via `default_arguments` when load grows.
- **Step Functions** — Standard workflows billed per state transition.

## 🔐 Security posture

- No hardcoded account IDs, ARNs, or bucket names anywhere in the repo.
- Backend state config is left empty in `backend.tf` — operators pass `-backend-config=backend.hcl` at `init` time.
- `.gitignore` blocks `*.tfvars`, `*.tfstate*`, `*.hcl`, and Lambda zips by default.
- IAM policies in each module are scoped to declared resources (no broad `Resource = "*"` except where AWS APIs require it, e.g. `glue:StartJobRun`).

## 📄 License

[MIT](LICENSE) © 2026 Baanu Sai Sankar Bojja

---

*Defensible, modular, serverless data engineering on AWS.*
