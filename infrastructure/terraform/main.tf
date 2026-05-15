############################################
# Root composition for the serverless ETL stack.
#
# Wires together the bucket / dynamodb / lambda / step_function /
# glue_job modules. All globally unique names are passed in as
# variables so this file can be reused per environment.
############################################

locals {
  name_prefix = "${var.project_name}-${var.environment}"

  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

############################################
# S3 buckets
############################################

module "raw_bucket" {
  source      = "./modules/s3_bucket"
  bucket_name = var.raw_bucket_name
  tags        = local.common_tags

  enable_lifecycle = true
  lifecycle_prefix = "raw/"
}

module "processed_bucket" {
  source      = "./modules/s3_bucket"
  bucket_name = var.processed_bucket_name
  tags        = local.common_tags

  enable_lifecycle = false
}

module "curated_bucket" {
  source      = "./modules/s3_bucket"
  bucket_name = var.curated_bucket_name
  tags        = local.common_tags

  enable_lifecycle = false
}

############################################
# DynamoDB hot-lookup table
############################################

module "lookup_table" {
  source     = "./modules/dynamodb"
  table_name = "${local.name_prefix}-lookups"
  hash_key   = "id"
  tags       = local.common_tags
}

############################################
# Lambda functions
#
# NOTE: zip artifacts are built locally by `package_lambdas.sh` and
# referenced by path. The CI deploy workflow runs the script before
# `terraform plan`.
############################################

module "ingest_lambda" {
  source        = "./modules/lambda"
  function_name = "${local.name_prefix}-ingest"
  filename      = "${var.lambda_artifacts_dir}/ingest_handler.zip"
  tags          = local.common_tags

  environment = {
    STAGING_BUCKET = module.processed_bucket.id
    STAGING_PREFIX = "staging"
    LOG_LEVEL      = "INFO"
  }

  extra_policy_statements = [
    {
      Effect   = "Allow"
      Action   = ["s3:GetObject"]
      Resource = ["${module.raw_bucket.arn}/*"]
    },
    {
      Effect   = "Allow"
      Action   = ["s3:PutObject"]
      Resource = ["${module.processed_bucket.arn}/*"]
    },
  ]
}

module "transform_lambda" {
  source        = "./modules/lambda"
  function_name = "${local.name_prefix}-transform"
  filename      = "${var.lambda_artifacts_dir}/transform_handler.zip"
  memory_size   = 512
  tags          = local.common_tags

  environment = {
    PROCESSED_BUCKET = module.processed_bucket.id
    PROCESSED_PREFIX = "processed"
    LOG_LEVEL        = "INFO"
  }

  extra_policy_statements = [
    {
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject"]
      Resource = ["${module.processed_bucket.arn}/*"]
    },
  ]
}

module "dq_lambda" {
  source        = "./modules/lambda"
  function_name = "${local.name_prefix}-dq"
  filename      = "${var.lambda_artifacts_dir}/dq_handler.zip"
  tags          = local.common_tags

  environment = {
    LOG_LEVEL = "INFO"
  }

  extra_policy_statements = [
    {
      Effect   = "Allow"
      Action   = ["s3:GetObject"]
      Resource = ["${module.processed_bucket.arn}/*"]
    },
  ]
}

module "load_lambda" {
  source        = "./modules/lambda"
  function_name = "${local.name_prefix}-load"
  filename      = "${var.lambda_artifacts_dir}/load_handler.zip"
  timeout       = 120
  memory_size   = 512
  tags          = local.common_tags

  environment = {
    DDB_TABLE       = module.lookup_table.name
    DDB_PRIMARY_KEY = "id"
    GLUE_JOB_NAME   = module.analytics_glue_job.name
    LOG_LEVEL       = "INFO"
  }

  extra_policy_statements = [
    {
      Effect   = "Allow"
      Action   = ["s3:GetObject"]
      Resource = ["${module.processed_bucket.arn}/*"]
    },
    {
      Effect   = "Allow"
      Action   = ["dynamodb:PutItem", "dynamodb:BatchWriteItem"]
      Resource = [module.lookup_table.arn]
    },
    {
      Effect   = "Allow"
      Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns"]
      Resource = ["*"]
    },
  ]
}

module "glue_trigger_lambda" {
  source        = "./modules/lambda"
  function_name = "${local.name_prefix}-glue-trigger"
  filename      = "${var.lambda_artifacts_dir}/glue_trigger.zip"
  tags          = local.common_tags

  environment = {
    GLUE_JOB_NAME = module.analytics_glue_job.name
    LOG_LEVEL     = "INFO"
  }

  extra_policy_statements = [
    {
      Effect   = "Allow"
      Action   = ["glue:StartJobRun", "glue:GetJobRun"]
      Resource = ["*"]
    },
  ]
}

############################################
# Glue analytics job
############################################

module "analytics_glue_job" {
  source          = "./modules/glue_job"
  job_name        = "${local.name_prefix}-analytics-load"
  description     = "Reads processed NDJSON and writes curated Parquet."
  script_s3_uri   = "s3://${module.processed_bucket.id}/${var.glue_script_s3_key}"
  temp_dir_s3_uri = "s3://${module.processed_bucket.id}/_glue_tmp/"

  s3_resource_arns = [
    module.processed_bucket.arn,
    "${module.processed_bucket.arn}/*",
    module.curated_bucket.arn,
    "${module.curated_bucket.arn}/*",
  ]

  default_arguments = {
    "--target_bucket" = module.curated_bucket.id
    "--target_prefix" = "curated/"
    "--source_prefix" = "processed/"
  }

  tags = local.common_tags
}

############################################
# SNS topic for failure notifications
############################################

resource "aws_sns_topic" "failures" {
  name = var.failure_topic_name
  tags = local.common_tags
}

############################################
# Step Functions state machine
############################################

locals {
  state_machine_definition = templatefile(
    "${path.module}/../state_machines/etl_pipeline.asl.json",
    {
      IngestFunctionArn      = module.ingest_lambda.arn
      TransformFunctionArn   = module.transform_lambda.arn
      DqFunctionArn          = module.dq_lambda.arn
      LoadFunctionArn        = module.load_lambda.arn
      GlueTriggerFunctionArn = module.glue_trigger_lambda.arn
      FailureTopicArn        = aws_sns_topic.failures.arn
    }
  )
}

module "etl_state_machine" {
  source     = "./modules/step_function"
  name       = "${local.name_prefix}-etl"
  definition = local.state_machine_definition

  lambda_invoke_arns = [
    module.ingest_lambda.arn,
    module.transform_lambda.arn,
    module.dq_lambda.arn,
    module.load_lambda.arn,
    module.glue_trigger_lambda.arn,
  ]

  sns_publish_arns = [aws_sns_topic.failures.arn]

  tags = local.common_tags
}

############################################
# S3 -> ingest Lambda notification
############################################

resource "aws_lambda_permission" "allow_s3_invoke_ingest" {
  statement_id  = "AllowS3InvokeIngest"
  action        = "lambda:InvokeFunction"
  function_name = module.ingest_lambda.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = module.raw_bucket.arn
}

resource "aws_s3_bucket_notification" "raw_notify" {
  bucket = module.raw_bucket.id

  lambda_function {
    lambda_function_arn = module.ingest_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"
  }

  depends_on = [aws_lambda_permission.allow_s3_invoke_ingest]
}
