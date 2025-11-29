variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-west-1"
}

variable "project_name" {
  description = "Base name for created resources"
  type        = string
  default     = "sqs-demo"
}

variable "vpc_cidr" {
  description = "CIDR block for the demo VPC"
  type        = string
  default     = "10.0.0.0/20"
}

variable "desired_count" {
  description = "Number of consumer tasks to run"
  type        = number
  default     = 1
}

variable "container_cpu" {
  description = "CPU units for the consumer task"
  type        = number
  default     = 256
}

variable "container_memory" {
  description = "Memory (MB) for the consumer task"
  type        = number
  default     = 512
}

variable "container_image_tag" {
  description = "Tag to deploy for the consumer image"
  type        = string
  default     = "latest"
}

variable "queue_visibility_timeout" {
  description = "Visibility timeout for the SQS queue (seconds)"
  type        = number
  default     = 10
}

variable "queue_receive_wait" {
  description = "Long-poll wait time for the SQS queue (seconds)"
  type        = number
  default     = 10
}

variable "log_retention_days" {
  description = "Retention in days for CloudWatch Logs groups"
  type        = number
  default     = 14
}
