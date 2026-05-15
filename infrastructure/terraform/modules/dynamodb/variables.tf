variable "table_name" {
  description = "DynamoDB table name."
  type        = string
}

variable "hash_key" {
  description = "Partition key attribute name."
  type        = string
  default     = "id"
}

variable "hash_key_type" {
  description = "Partition key attribute type (S, N, or B)."
  type        = string
  default     = "S"
}

variable "enable_pitr" {
  description = "Enable point-in-time recovery."
  type        = bool
  default     = true
}

variable "tags" {
  description = "Tags applied to the table."
  type        = map(string)
  default     = {}
}
