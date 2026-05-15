output "raw_bucket" {
  description = "Raw zone S3 bucket."
  value       = module.raw_bucket.id
}

output "processed_bucket" {
  description = "Processed zone S3 bucket."
  value       = module.processed_bucket.id
}

output "curated_bucket" {
  description = "Curated zone S3 bucket."
  value       = module.curated_bucket.id
}

output "lookup_table" {
  description = "DynamoDB hot-lookup table name."
  value       = module.lookup_table.name
}

output "state_machine_arn" {
  description = "ETL Step Functions state machine ARN."
  value       = module.etl_state_machine.arn
}

output "glue_job_name" {
  description = "Glue analytics job name."
  value       = module.analytics_glue_job.name
}

output "failure_topic_arn" {
  description = "SNS topic for pipeline failure notifications."
  value       = aws_sns_topic.failures.arn
}
