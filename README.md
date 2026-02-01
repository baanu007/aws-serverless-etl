# ☁️ AWS Serverless ETL Pipeline

A production-ready serverless ETL pipeline using **AWS Lambda**, **Glue**, **S3**, and **QuickSight** for automated data ingestion and visualization.

![AWS Lambda](https://img.shields.io/badge/AWS%20Lambda-FF9900?style=for-the-badge&logo=aws-lambda&logoColor=white)
![AWS Glue](https://img.shields.io/badge/AWS%20Glue-FF9900?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Amazon S3](https://img.shields.io/badge/Amazon%20S3-569A31?style=for-the-badge&logo=amazon-s3&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-7B42BC?style=for-the-badge&logo=terraform&logoColor=white)

## 📋 Overview

This project implements a fully serverless ETL pipeline that:

- **Ingests** data from REST APIs on a schedule
- **Transforms** raw data using AWS Glue jobs
- **Stores** processed data in S3 data lake
- **Catalogs** metadata in AWS Glue Data Catalog
- **Visualizes** with QuickSight dashboards

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA SOURCES                                    │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐            │
│  │  REST API  │  │  Webhook   │  │   SFTP     │  │  Database  │            │
│  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘            │
└────────┼───────────────┼───────────────┼───────────────┼────────────────────┘
         │               │               │               │
         ▼               ▼               ▼               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         INGESTION LAYER                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                    AWS Lambda Functions                               │   │
│  │  • api_ingestion_lambda      - Pull from REST APIs                   │   │
│  │  • webhook_handler_lambda    - Process incoming webhooks             │   │
│  │  • file_processor_lambda     - Process uploaded files                │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                              │                                               │
│                    ┌─────────┴─────────┐                                    │
│                    ▼                   ▼                                    │
│         ┌──────────────────┐  ┌──────────────────┐                         │
│         │  EventBridge     │  │  S3 Event        │                         │
│         │  (Scheduler)     │  │  Triggers        │                         │
│         └──────────────────┘  └──────────────────┘                         │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           S3 DATA LAKE                                       │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐          │
│  │   RAW ZONE       │  │  PROCESSED ZONE  │  │  CURATED ZONE    │          │
│  │   s3://raw/      │  │  s3://processed/ │  │  s3://curated/   │          │
│  │                  │  │                  │  │                  │          │
│  │  • JSON files    │  │  • Parquet       │  │  • Aggregated    │          │
│  │  • CSV dumps     │─▶│  • Partitioned   │─▶│  • Optimized     │          │
│  │  • API responses │  │  • Deduplicated  │  │  • Analytics     │          │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘          │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        TRANSFORMATION LAYER                                  │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                      AWS Glue Jobs                                    │   │
│  │  • raw_to_processed_job   - Clean, dedupe, partition                 │   │
│  │  • processed_to_curated   - Aggregate, enrich, join                  │   │
│  │  • data_quality_job       - Validate and report                      │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                      │                                       │
│                    ┌─────────────────┴─────────────────┐                    │
│                    ▼                                   ▼                    │
│         ┌──────────────────┐                ┌──────────────────┐           │
│         │  Glue Data       │                │  Glue Workflow   │           │
│         │  Catalog         │                │  Orchestration   │           │
│         └──────────────────┘                └──────────────────┘           │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CONSUMPTION LAYER                                    │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐                │
│  │   QuickSight   │  │    Athena      │  │   Redshift     │                │
│  │   Dashboards   │  │   Queries      │  │   Spectrum     │                │
│  └────────────────┘  └────────────────┘  └────────────────┘                │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 📁 Project Structure

```
aws-serverless-etl/
├── lambda/
│   ├── api_ingestion/
│   │   ├── handler.py
│   │   └── requirements.txt
│   ├── file_processor/
│   │   ├── handler.py
│   │   └── requirements.txt
│   └── data_quality/
│       └── handler.py
├── glue/
│   ├── jobs/
│   │   ├── raw_to_processed.py
│   │   ├── processed_to_curated.py
│   │   └── data_quality_check.py
│   └── scripts/
│       └── common_transforms.py
├── terraform/
│   ├── main.tf
│   ├── lambda.tf
│   ├── glue.tf
│   ├── s3.tf
│   ├── iam.tf
│   └── variables.tf
├── tests/
│   ├── unit/
│   └── integration/
├── config/
│   └── pipeline_config.yaml
└── README.md
```

## 🚀 Deployment

### Prerequisites

- AWS CLI configured
- Terraform 1.0+
- Python 3.9+

### Deploy Infrastructure

```bash
# Clone repository
git clone https://github.com/baanu007/aws-serverless-etl.git
cd aws-serverless-etl

# Initialize Terraform
cd terraform
terraform init

# Plan deployment
terraform plan -var-file="prod.tfvars"

# Apply
terraform apply -var-file="prod.tfvars"
```

## 📊 Pipeline Features

### Lambda Functions

| Function | Trigger | Purpose |
|----------|---------|---------|
| `api_ingestion` | EventBridge (hourly) | Pull data from REST APIs |
| `file_processor` | S3 Event | Process uploaded files |
| `data_quality` | Glue Workflow | Validate processed data |

### Glue Jobs

| Job | Schedule | Description |
|-----|----------|-------------|
| `raw_to_processed` | Hourly | Clean and partition raw data |
| `processed_to_curated` | Daily | Aggregate for analytics |
| `data_quality_check` | After ETL | Run quality validations |

## 🔧 Configuration

```yaml
# config/pipeline_config.yaml
sources:
  - name: sales_api
    type: rest_api
    url: https://api.example.com/sales
    schedule: rate(1 hour)
    auth_type: api_key
    
  - name: inventory_feed
    type: sftp
    path: /data/inventory/
    schedule: rate(6 hours)

transforms:
  partitioning:
    keys: [year, month, day]
  deduplication:
    keys: [id, timestamp]
  
output:
  format: parquet
  compression: snappy
```

## 🛠️ Technologies

| Component | AWS Service |
|-----------|-------------|
| Ingestion | Lambda, EventBridge |
| Storage | S3 |
| Processing | Glue, Glue Catalog |
| Orchestration | Step Functions, Glue Workflows |
| Analytics | Athena, QuickSight |
| Infrastructure | Terraform |
| Monitoring | CloudWatch, SNS |

## 💰 Cost Optimization

- Lambda: Pay per invocation (~$0.20/million)
- S3: Intelligent-Tiering enabled
- Glue: Spot instances for jobs
- Athena: Partitioning reduces scan costs

## 📄 License

MIT License

---

*Scalable, cost-effective serverless data engineering on AWS*
