output "id" {
  description = "Bucket ID."
  value       = aws_s3_bucket.this.id
}

output "arn" {
  description = "Bucket ARN."
  value       = aws_s3_bucket.this.arn
}

output "bucket_domain_name" {
  description = "Bucket regional domain name."
  value       = aws_s3_bucket.this.bucket_regional_domain_name
}
