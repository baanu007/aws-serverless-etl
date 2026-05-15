############################################
# Lambda module
#
# Reusable wrapper that creates:
#   * a CloudWatch log group (with retention)
#   * a minimal execution role + inline policy
#   * the Lambda function itself
#
# Per-function permissions (S3, DynamoDB, Glue, SNS, etc.) are passed
# in via `extra_policy_statements` so the calling stack stays explicit
# about least-privilege access.
############################################

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  log_group_name = "/aws/lambda/${var.function_name}"
}

resource "aws_cloudwatch_log_group" "this" {
  name              = local.log_group_name
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

resource "aws_iam_role" "this" {
  name = "${var.function_name}-role"
  tags = var.tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "this" {
  name = "${var.function_name}-policy"
  role = aws_iam_role.this.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      [
        {
          Effect = "Allow"
          Action = [
            "logs:CreateLogStream",
            "logs:PutLogEvents",
          ]
          Resource = "${aws_cloudwatch_log_group.this.arn}:*"
        }
      ],
      var.extra_policy_statements
    )
  })
}

resource "aws_lambda_function" "this" {
  function_name = var.function_name
  role          = aws_iam_role.this.arn
  handler       = var.handler
  runtime       = var.runtime
  timeout       = var.timeout
  memory_size   = var.memory_size

  filename         = var.filename
  source_code_hash = var.source_code_hash

  environment {
    variables = var.environment
  }

  tags = var.tags

  depends_on = [aws_cloudwatch_log_group.this]
}
