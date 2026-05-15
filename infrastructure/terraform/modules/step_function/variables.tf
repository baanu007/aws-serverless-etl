variable "name" {
  description = "State machine name."
  type        = string
}

variable "definition" {
  description = "Rendered ASL JSON definition for the state machine."
  type        = string
}

variable "lambda_invoke_arns" {
  description = "List of Lambda ARNs the state machine is allowed to invoke."
  type        = list(string)
  default     = []
}

variable "sns_publish_arns" {
  description = "List of SNS topic ARNs the state machine can publish to."
  type        = list(string)
  default     = []
}

variable "enable_logging" {
  description = "Send execution logs to CloudWatch."
  type        = bool
  default     = true
}

variable "tags" {
  description = "Resource tags."
  type        = map(string)
  default     = {}
}
