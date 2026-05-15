variable "job_name" {
  description = "Glue job name."
  type        = string
}

variable "description" {
  description = "Glue job description."
  type        = string
  default     = "Managed by Terraform"
}

variable "script_s3_uri" {
  description = "S3 URI of the PySpark script."
  type        = string
}

variable "temp_dir_s3_uri" {
  description = "S3 URI used as the Glue --TempDir."
  type        = string
}

variable "s3_resource_arns" {
  description = "List of S3 ARNs the Glue role can access."
  type        = list(string)
}

variable "default_arguments" {
  description = "Extra job arguments merged into default_arguments."
  type        = map(string)
  default     = {}
}

variable "glue_version" {
  description = "Glue version."
  type        = string
  default     = "4.0"
}

variable "worker_type" {
  description = "Worker type (G.1X, G.2X, etc.)."
  type        = string
  default     = "G.1X"
}

variable "number_of_workers" {
  description = "Number of DPUs / workers."
  type        = number
  default     = 2
}

variable "timeout_minutes" {
  description = "Job timeout in minutes."
  type        = number
  default     = 60
}

variable "tags" {
  description = "Resource tags."
  type        = map(string)
  default     = {}
}
