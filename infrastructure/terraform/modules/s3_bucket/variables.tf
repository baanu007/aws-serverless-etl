variable "bucket_name" {
  description = "Globally unique S3 bucket name."
  type        = string
}

variable "tags" {
  description = "Tags applied to the bucket."
  type        = map(string)
  default     = {}
}

variable "enable_versioning" {
  description = "Whether to enable bucket versioning."
  type        = bool
  default     = true
}

variable "enable_lifecycle" {
  description = "Whether to configure a lifecycle rule."
  type        = bool
  default     = true
}

variable "lifecycle_prefix" {
  description = "Prefix the lifecycle rule applies to."
  type        = string
  default     = "raw/"
}

variable "glacier_transition_days" {
  description = "Days after which objects under lifecycle_prefix transition to Glacier."
  type        = number
  default     = 90
}

variable "noncurrent_expiration_days" {
  description = "Days after which noncurrent versions are expired."
  type        = number
  default     = 365
}
