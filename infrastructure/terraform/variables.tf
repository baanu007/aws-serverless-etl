variable "aws_region" {
  description = "AWS region for all resources."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (dev, stage, prod)."
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Short project name used as a resource-name prefix."
  type        = string
  default     = "serverless-etl"
}

variable "raw_bucket_name" {
  description = "Globally unique name for the raw zone bucket."
  type        = string
}

variable "processed_bucket_name" {
  description = "Globally unique name for the processed zone bucket."
  type        = string
}

variable "curated_bucket_name" {
  description = "Globally unique name for the curated zone bucket."
  type        = string
}

variable "lambda_artifacts_dir" {
  description = "Local directory containing built Lambda zip artifacts."
  type        = string
  default     = "../../build"
}

variable "glue_script_s3_key" {
  description = "S3 key (within the processed bucket) for the analytics_load.py Glue script."
  type        = string
  default     = "scripts/analytics_load.py"
}

variable "failure_topic_name" {
  description = "SNS topic name used by the state machine to publish failures."
  type        = string
  default     = "serverless-etl-failures"
}
