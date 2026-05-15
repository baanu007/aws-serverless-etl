output "arn" {
  description = "State machine ARN."
  value       = aws_sfn_state_machine.this.arn
}

output "role_arn" {
  description = "Execution role ARN."
  value       = aws_iam_role.this.arn
}
