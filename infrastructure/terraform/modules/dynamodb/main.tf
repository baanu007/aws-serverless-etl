############################################
# DynamoDB module
#
# Creates an on-demand DynamoDB table with point-in-time recovery
# enabled by default. The table schema is intentionally minimal — a
# single string partition key — to keep the hot-lookup contract simple.
############################################

resource "aws_dynamodb_table" "this" {
  name         = var.table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = var.hash_key

  attribute {
    name = var.hash_key
    type = var.hash_key_type
  }

  point_in_time_recovery {
    enabled = var.enable_pitr
  }

  server_side_encryption {
    enabled = true
  }

  tags = var.tags
}
