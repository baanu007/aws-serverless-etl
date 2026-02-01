# AWS Serverless ETL Infrastructure
# Terraform configuration for Lambda, Glue, and S3

terraform {
  required_version = ">= 1.0.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  
  backend "s3" {
    bucket = "terraform-state-bucket"
    key    = "serverless-etl/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = {
      Project     = "serverless-etl"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

# Variables
variable "aws_region" {
  default = "us-east-1"
}

variable "environment" {
  default = "prod"
}

variable "project_name" {
  default = "serverless-etl"
}

# S3 Buckets for Data Lake
resource "aws_s3_bucket" "raw" {
  bucket = "${var.project_name}-raw-${var.environment}"
}

resource "aws_s3_bucket" "processed" {
  bucket = "${var.project_name}-processed-${var.environment}"
}

resource "aws_s3_bucket" "curated" {
  bucket = "${var.project_name}-curated-${var.environment}"
}

# S3 Bucket Versioning
resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration {
    status = "Enabled"
  }
}

# S3 Lifecycle Rules
resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  
  rule {
    id     = "transition-to-ia"
    status = "Enabled"
    
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

# IAM Role for Lambda
resource "aws_iam_role" "lambda_role" {
  name = "${var.project_name}-lambda-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "lambda_policy" {
  name = "${var.project_name}-lambda-policy"
  role = aws_iam_role.lambda_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.raw.arn,
          "${aws_s3_bucket.raw.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = "*"
      }
    ]
  })
}

# Lambda Function
resource "aws_lambda_function" "api_ingestion" {
  filename         = "../lambda/api_ingestion/deployment.zip"
  function_name    = "${var.project_name}-api-ingestion"
  role            = aws_iam_role.lambda_role.arn
  handler         = "handler.handler"
  runtime         = "python3.9"
  timeout         = 300
  memory_size     = 256
  
  environment {
    variables = {
      RAW_BUCKET        = aws_s3_bucket.raw.id
      API_CONFIG_SECRET = aws_secretsmanager_secret.api_config.name
    }
  }
}

# EventBridge Rule (Scheduler)
resource "aws_cloudwatch_event_rule" "hourly_ingestion" {
  name                = "${var.project_name}-hourly-ingestion"
  description         = "Trigger API ingestion every hour"
  schedule_expression = "rate(1 hour)"
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.hourly_ingestion.name
  target_id = "api-ingestion-lambda"
  arn       = aws_lambda_function.api_ingestion.arn
}

resource "aws_lambda_permission" "eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api_ingestion.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.hourly_ingestion.arn
}

# Secrets Manager for API credentials
resource "aws_secretsmanager_secret" "api_config" {
  name = "${var.project_name}-api-config"
}

# Glue Database
resource "aws_glue_catalog_database" "data_lake" {
  name = "${var.project_name}_${var.environment}"
}

# Glue IAM Role
resource "aws_iam_role" "glue_role" {
  name = "${var.project_name}-glue-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Glue Job
resource "aws_glue_job" "raw_to_processed" {
  name     = "${var.project_name}-raw-to-processed"
  role_arn = aws_iam_role.glue_role.arn
  
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.raw.id}/scripts/raw_to_processed.py"
    python_version  = "3"
  }
  
  default_arguments = {
    "--source_database"           = aws_glue_catalog_database.data_lake.name
    "--source_table"              = "raw_data"
    "--target_bucket"             = aws_s3_bucket.processed.id
    "--target_prefix"             = "processed/"
    "--enable-metrics"            = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"              = "python"
  }
  
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
}

# Outputs
output "raw_bucket" {
  value = aws_s3_bucket.raw.id
}

output "processed_bucket" {
  value = aws_s3_bucket.processed.id
}

output "lambda_function" {
  value = aws_lambda_function.api_ingestion.arn
}

output "glue_database" {
  value = aws_glue_catalog_database.data_lake.name
}
