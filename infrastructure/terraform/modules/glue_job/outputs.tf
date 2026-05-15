output "name" {
  description = "Glue job name."
  value       = aws_glue_job.this.name
}

output "arn" {
  description = "Glue job ARN."
  value       = aws_glue_job.this.arn
}

output "role_arn" {
  description = "Glue IAM role ARN."
  value       = aws_iam_role.this.arn
}
