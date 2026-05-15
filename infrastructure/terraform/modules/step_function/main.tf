############################################
# Step Functions module
#
# Renders the ASL definition from a template file and provisions the
# state machine + IAM role. The role is granted lambda:InvokeFunction
# and sns:Publish based on caller-supplied lists.
############################################

resource "aws_iam_role" "this" {
  name = "${var.name}-sf-role"
  tags = var.tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "states.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "this" {
  name = "${var.name}-sf-policy"
  role = aws_iam_role.this.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["lambda:InvokeFunction"]
        Resource = var.lambda_invoke_arns
      },
      {
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = var.sns_publish_arns
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups",
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_cloudwatch_log_group" "this" {
  count             = var.enable_logging ? 1 : 0
  name              = "/aws/vendedlogs/states/${var.name}"
  retention_in_days = 14
  tags              = var.tags
}

resource "aws_sfn_state_machine" "this" {
  name       = var.name
  role_arn   = aws_iam_role.this.arn
  definition = var.definition
  tags       = var.tags

  dynamic "logging_configuration" {
    for_each = var.enable_logging ? [1] : []
    content {
      log_destination        = "${aws_cloudwatch_log_group.this[0].arn}:*"
      include_execution_data = true
      level                  = "ERROR"
    }
  }
}
