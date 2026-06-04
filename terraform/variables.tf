variable "environment" {
  description = "Deployment environment (dev | staging | prod)"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod."
  }
}

variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Project name used as a prefix for all resources"
  type        = string
  default     = "financial-platform"
}

variable "redshift_node_type" {
  description = "Redshift node type"
  type        = string
  default     = "ra3.xlplus"
}

variable "redshift_number_of_nodes" {
  description = "Number of Redshift nodes"
  type        = number
  default     = 2
}

variable "redshift_db_name" {
  description = "Redshift database name"
  type        = string
  default     = "financial_dw"
}

variable "kinesis_shard_count" {
  description = "Number of Kinesis shards (1 shard = ~1 MB/s, 1000 records/s)"
  type        = number
  default     = 4
}

variable "lambda_memory_mb" {
  description = "Lambda memory for fraud detection function"
  type        = number
  default     = 512
}

variable "lambda_timeout_seconds" {
  description = "Lambda timeout for fraud detection function"
  type        = number
  default     = 60
}

variable "glue_worker_type" {
  description = "Glue worker type (G.1X | G.2X)"
  type        = string
  default     = "G.2X"
}

variable "glue_number_of_workers" {
  description = "Number of Glue workers for ETL jobs"
  type        = number
  default     = 10
}

variable "alert_email" {
  description = "Email address for fraud alert SNS notifications"
  type        = string
}

variable "tags" {
  description = "Common tags applied to all resources"
  type        = map(string)
  default     = {}
}
