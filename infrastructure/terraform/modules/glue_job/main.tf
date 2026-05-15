############################################
# Glue job module
#
# Creates a Glue ETL job + scoped IAM role. The script must already be
# uploaded to S3 (done by package_lambdas.sh / deploy pipeline).
############################################

resource "aws_iam_role" "this" {
  name = "${var.job_name}-glue-role"
  tags = var.tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "glue.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "service" {
  role       = aws_iam_role.this.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "data_access" {
  name = "${var.job_name}-data-access"
  role = aws_iam_role.this.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ]
        Resource = var.s3_resource_arns
      }
    ]
  })
}

resource "aws_glue_job" "this" {
  name         = var.job_name
  role_arn     = aws_iam_role.this.arn
  glue_version = var.glue_version
  description  = var.description
  tags         = var.tags

  command {
    name            = "glueetl"
    script_location = var.script_s3_uri
    python_version  = "3"
  }

  default_arguments = merge(
    {
      "--enable-metrics"                   = "true"
      "--enable-continuous-cloudwatch-log" = "true"
      "--job-language"                     = "python"
      "--TempDir"                          = var.temp_dir_s3_uri
    },
    var.default_arguments,
  )

  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers
  timeout           = var.timeout_minutes
}
