variable "function_name" {
  description = "Lambda function name."
  type        = string
}

variable "handler" {
  description = "Lambda handler entrypoint (module.function)."
  type        = string
  default     = "handler.handler"
}

variable "runtime" {
  description = "Lambda runtime."
  type        = string
  default     = "python3.11"
}

variable "timeout" {
  description = "Function timeout in seconds."
  type        = number
  default     = 60
}

variable "memory_size" {
  description = "Function memory size in MB."
  type        = number
  default     = 256
}

variable "filename" {
  description = "Path to the zipped deployment artifact."
  type        = string
}

variable "source_code_hash" {
  description = "Base64-encoded SHA256 of the zip file (drives redeploys)."
  type        = string
  default     = null
}

variable "environment" {
  description = "Environment variables passed to the function."
  type        = map(string)
  default     = {}
}

variable "extra_policy_statements" {
  description = "Additional IAM policy statements (least-privilege)."
  type        = list(any)
  default     = []
}

variable "log_retention_days" {
  description = "CloudWatch log retention."
  type        = number
  default     = 14
}

variable "tags" {
  description = "Resource tags."
  type        = map(string)
  default     = {}
}
