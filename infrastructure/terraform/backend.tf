############################################
# Remote state backend
#
# Placeholders only — do NOT hardcode real bucket / table / account
# values here. Operators should override these via a backend config
# file when running `terraform init`, e.g.:
#
#   terraform init -backend-config=environments/dev/backend.hcl
#
# Example backend.hcl:
#   bucket         = "my-tf-state-bucket"
#   key            = "aws-serverless-etl/dev/terraform.tfstate"
#   region         = "us-east-1"
#   dynamodb_table = "my-tf-lock-table"
#   encrypt        = true
############################################

terraform {
  backend "s3" {
    # Intentionally empty — populated via -backend-config at init time.
  }
}
